#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

NODE_ID="${1:-}"
if [ -z "${NODE_ID}" ]; then
    echo "usage: $(basename "$0") NODE_ID" >&2
    exit 2
fi

validate_node_id "${NODE_ID}"

# 在非目标节点机器上停止远程节点时，SSH 转发到对应主机执行
if ! epoch_is_local_node "${NODE_ID}"; then
    REMOTE="$(epoch_ssh_target "${NODE_ID}")"
    RB="$(epoch_bench_root_for_node "${NODE_ID}")"
    ssh -o BatchMode=yes -o ConnectTimeout=10 "${REMOTE}" \
        "export EPOCH_LOCAL_NODE_ID=${NODE_ID}; export EPOCH_BENCH_ROOT=${RB}; \
        export EPOCH_SKIP_NODE=${EPOCH_SKIP_NODE}; \
        export EPOCH_SKIP_NODE2=$( [ \"${EPOCH_SKIP_NODE}\" = 2 ] && echo 1 || echo 0 ); \
        bash ${RB}/openGauss-server/scripts/epoch_benchmark/stop_node.sh ${NODE_ID}"
    exit $?
fi

DATA_DIR="$(data_dir_for_node "${NODE_ID}")"
PID_FILE="${DATA_DIR}/postmaster.pid"
PORT="${EPOCH_DB_PORT}"
SOCKET_FILE="/tmp/.s.PGSQL.${PORT}"

is_node_running() {
    pgrep -f "gaussdb.*${DATA_DIR}" >/dev/null 2>&1
}

collect_pids() {
    pgrep -f "gaussdb.*${DATA_DIR}" 2>/dev/null || true
    if [ -f "${PID_FILE}" ]; then
        head -1 "${PID_FILE}" 2>/dev/null || true
    fi
}

force_kill_node() {
    echo "force killing gaussdb for node ${NODE_ID} ..."
    local pid
    local pids
    pids="$(collect_pids | tr ' ' '\n' | sort -u | tr '\n' ' ')"
    for pid in ${pids}; do
        [ -n "${pid}" ] || continue
        kill -TERM "${pid}" 2>/dev/null || true
    done
    sleep 2
    if is_node_running; then
        for pid in $(collect_pids); do
            [ -n "${pid}" ] || continue
            kill -9 "${pid}" 2>/dev/null || true
        done
        pkill -9 -f "gaussdb.*${DATA_DIR}" 2>/dev/null || true
        sleep 1
    fi
    rm -f "${PID_FILE}" "${SOCKET_FILE}"
}

cleanup_stale_shmem() {
    local key=$((PORT * 1000 + 1))
    local shmid
    shmid="$(ipcs -m 2>/dev/null | awk -v key="$(printf '0x%x' "${key}")" '$1 == key {print $2; exit}')"
    if [ -z "${shmid}" ]; then
        return
    fi
    local nattch
    nattch="$(ipcs -m -i "${shmid}" 2>/dev/null | awk '/nattch/{print $4}')"
    if [ "${nattch:-1}" = "0" ] || ! is_node_running; then
        echo "removing shared memory segment shmid=${shmid} (port ${PORT})"
        ipcrm -m "${shmid}" 2>/dev/null || true
    fi
}

if ! is_node_running && [ ! -f "${PID_FILE}" ]; then
    echo "node ${NODE_ID} already stopped"
    cleanup_stale_shmem
    exit 0
fi

# gs_ctl -m fast 在 epoch worker 活跃时可能卡 60s+，加超时后改 immediate
if is_node_running; then
    echo "stopping node ${NODE_ID} via gs_ctl (timeout 10s) ..."
    timeout 10 gs_ctl stop -D "${DATA_DIR}" -m fast 2>/dev/null || true
fi
if is_node_running; then
    echo "gs_ctl fast stop timed out or failed, trying immediate ..."
    timeout 5 gs_ctl stop -D "${DATA_DIR}" -m immediate 2>/dev/null || true
fi
if is_node_running; then
    force_kill_node
fi

cleanup_stale_shmem

if is_node_running; then
    echo "ERROR: gaussdb still running for node ${NODE_ID}" >&2
    pgrep -af "gaussdb.*${DATA_DIR}" >&2 || true
    exit 1
fi

echo "node ${NODE_ID} stopped"
