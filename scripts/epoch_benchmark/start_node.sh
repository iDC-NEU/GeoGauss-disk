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

# 在非目标节点机器上启动远程节点时，SSH 转发到对应主机执行
if ! epoch_is_local_node "${NODE_ID}"; then
    REMOTE="$(epoch_ssh_target "${NODE_ID}")"
    RB="$(epoch_bench_root_for_node "${NODE_ID}")"
    ssh -o BatchMode=yes -o ConnectTimeout=10 "${REMOTE}" \
        "export EPOCH_LOCAL_NODE_ID=${NODE_ID}; export EPOCH_BENCH_ROOT=${RB}; \
        export EPOCH_SKIP_NODE=${EPOCH_SKIP_NODE}; \
        export EPOCH_SKIP_NODE2=$( [ \"${EPOCH_SKIP_NODE}\" = 2 ] && echo 1 || echo 0 ); \
        bash ${RB}/openGauss-server/scripts/epoch_benchmark/start_node.sh ${NODE_ID}"
    exit $?
fi

DATA_DIR="$(data_dir_for_node "${NODE_ID}")"
LOG_FILE="${DATA_DIR}/pg_log/epoch-node${NODE_ID}.log"
PID_FILE="${DATA_DIR}/postmaster.pid"
GAUSS_PATTERN="gaussdb.*${DATA_DIR}"

mkdir -p "${DATA_DIR}/pg_log"
export GAUSSLOG="${DATA_DIR}/pg_log"

cleanup_stale_shmem() {
    local port="${EPOCH_DB_PORT}"
    local key=$((port * 1000 + 1))
    local shmid
    shmid="$(ipcs -m 2>/dev/null | awk -v key="$(printf '0x%x' "${key}")" '$1 == key {print $2; exit}')"
    if [ -n "${shmid}" ]; then
        local nattch
        nattch="$(ipcs -m -i "${shmid}" 2>/dev/null | awk '/nattch/{print $4}')"
        if [ "${nattch:-1}" = "0" ]; then
            echo "removing orphan shared memory segment shmid=${shmid} (port ${port})"
            ipcrm -m "${shmid}" 2>/dev/null || true
        fi
    fi
}
cleanup_stale_shmem

is_running() {
    pgrep -f "${GAUSS_PATTERN}" >/dev/null 2>&1
}

if [ -f "${PID_FILE}" ]; then
    pid="$(head -1 "${PID_FILE}" 2>/dev/null || true)"
    if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
        echo "node ${NODE_ID} already running (pid ${pid})"
        exit 0
    fi
fi

# pid 文件缺失但进程仍在（常见于 gs_ctl stop 失败），先强制清理
if is_running; then
    echo "stale gaussdb detected for node ${NODE_ID}, stopping before start ..."
    bash "${SCRIPT_DIR}/stop_node.sh" "${NODE_ID}" || {
        pkill -9 -f "${GAUSS_PATTERN}" 2>/dev/null || true
        rm -f "${PID_FILE}"
        sleep 2
    }
fi

if is_running; then
    echo "ERROR: cannot start node ${NODE_ID}, gaussdb still running" >&2
    exit 1
fi

epoch_check_gaussdb

# gs_ctl 默认 -t 60；300w 等大库 redo 可能超时，这里提到 2×（120s）
echo "starting node ${NODE_ID} at ${DATA_DIR} ..."
if ! gs_ctl start -D "${DATA_DIR}" -l "${LOG_FILE}" -Z single_node -t 120; then
    echo "gs_ctl start failed, check ${LOG_FILE}" >&2
    tail -20 "${LOG_FILE}" 2>/dev/null || true
    exit 1
fi
sleep 2
if ! gs_ctl status -D "${DATA_DIR}"; then
    echo "ERROR: node ${NODE_ID} failed to become ready" >&2
    tail -30 "${LOG_FILE}" 2>/dev/null || true
    exit 1
fi
echo "node ${NODE_ID} started"
