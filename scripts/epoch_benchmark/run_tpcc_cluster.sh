#!/usr/bin/env bash
# 三节点 epoch 集群：deploy / configure / restart
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

export EPOCH_BENCH_ROOT="${EPOCH_BENCH_ROOT:-/home/hcj2003/2025-9}"
export EPOCH_PROFILE="${EPOCH_PROFILE:-async-stats}"

# prep-only | deploy-only
RUN_MODE=prep-only

usage() {
    cat <<EOF
usage: $(basename "$0") [OPTION]

  (default)     deploy node1/2 → configure → restart → 健康检查
  --prep-only   同上
  --deploy-only 仅 rsync 二进制到 node1/2

env: EPOCH_BENCH_ROOT, EPOCH_PROFILE (default async-stats)
  profile 内置 gap / physical，一般只需 export EPOCH_PROFILE：
    benchmark   sync  gap=5  physical=100ms
    stats       sync  gap=5  physical=100ms +diag
    async       async gap=16 physical=5ms
    async-wan   async gap=48 physical=3ms
    async-stats async gap=20 physical=10ms +diag（默认）
  可选覆盖: EPOCH_PHYS_LOGICAL_GAP_LIMIT, EPOCH_PHYSICAL_INTERVAL_MS
EOF
}

epoch_remote_shell() {
    local n="$1"
    shift
    local rb env_prefix
    rb="$(epoch_bench_root_for_node "${n}")"
    env_prefix="$(epoch_remote_gauss_env "${n}")"
    if epoch_is_local_node "${n}"; then
        eval "${env_prefix}"
        bash -c "$*"
    else
        ssh -o BatchMode=yes -o ConnectTimeout=10 "$(epoch_ssh_target "${n}")" \
            "${env_prefix}; $*"
    fi
}

epoch_check_nodes_running() {
    local n rb detail
    echo "=== postflight: gaussdb + gsql ==="
    for n in 0 1 2; do
        rb="$(epoch_bench_root_for_node "${n}")"
        if ! epoch_remote_shell "${n}" \
            "gs_ctl status -D ${rb}/data/epoch-node${n} >/dev/null 2>&1"; then
            detail="$(epoch_remote_shell "${n}" \
                "gs_ctl status -D ${rb}/data/epoch-node${n} 2>&1 | tail -2" 2>/dev/null || true)"
            echo "ERROR: epoch-node${n} failed gs_ctl status (datadir ${rb}/data/epoch-node${n})" >&2
            [ -n "${detail}" ] && echo "  ${detail}" >&2
            exit 1
        fi
        if ! epoch_remote_shell "${n}" \
            "timeout 8 gsql -d test -p 32000 -tAc 'SELECT 1' >/dev/null 2>&1"; then
            echo "ERROR: epoch-node${n} not accepting SQL within 8s (process may be hung)" >&2
            exit 1
        fi
        echo "node${n}: running, gsql ok"
    done
}

while [ $# -gt 0 ]; do
    case "$1" in
        --prep-only)
            RUN_MODE=prep-only
            shift
            ;;
        --deploy-only)
            RUN_MODE=deploy-only
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown option: $1 (try --help)" >&2
            exit 2
            ;;
    esac
done

epoch_invoke_deploy() {
    env -u LD_LIBRARY_PATH \
        EPOCH_BENCH_ROOT="${EPOCH_BENCH_ROOT}" \
        EPOCH_PROFILE="${EPOCH_PROFILE:-}" \
        EPOCH_PHYS_LOGICAL_GAP_LIMIT="${EPOCH_PHYS_LOGICAL_GAP_LIMIT:-}" \
        EPOCH_PHYSICAL_INTERVAL_MS="${EPOCH_PHYSICAL_INTERVAL_MS:-}" \
        PATH="/usr/bin:/bin:/usr/local/bin" \
        bash "${SCRIPT_DIR}/deploy_to_remote.sh" "$@"
}

epoch_cluster_deploy() {
    echo "=== [1/3] deploy binary to node1/node2 ==="
    for n in 1 2; do
        epoch_invoke_deploy "${n}"
    done
}

epoch_cluster_configure() {
    echo "=== [2/3] configure all nodes (profile=${EPOCH_PROFILE}) ==="
    for n in 0 1 2; do
        bash "${SCRIPT_DIR}/configure_node.sh" "${n}"
    done
}

epoch_cluster_restart() {
    echo "=== [3/3] stop all nodes ==="
    for n in 0 1 2; do
        bash "${SCRIPT_DIR}/stop_node.sh" "${n}" || true
    done
    sleep 3
    echo "=== start all nodes ==="
    for n in 0 1 2; do
        bash "${SCRIPT_DIR}/start_node.sh" "${n}"
    done
    sleep 5
}

epoch_snapshot_epoch_guc() {
    local result_dir="$1"
    for n in 0 1 2; do
        local dest="${result_dir}/postgresql.epoch.node${n}.conf"
        if epoch_is_local_node "${n}"; then
            grep -E '^epoch_|^enable_epoch|^occ_enable_stats' \
                "$(data_dir_for_node "${n}")/postgresql.conf" > "${dest}" 2>/dev/null || true
        else
            local rb
            rb="$(epoch_bench_root_for_node "${n}")"
            ssh -o BatchMode=yes "$(epoch_ssh_target "${n}")" \
                "grep -E '^epoch_|^enable_epoch|^occ_enable_stats' ${rb}/data/epoch-node${n}/postgresql.conf" \
                > "${dest}" 2>/dev/null || true
        fi
    done
}

case "${RUN_MODE}" in
    deploy-only)
        epoch_cluster_deploy
        echo "DONE: deploy-only (node1/node2 synced)"
        ;;
    prep-only|*)
        PREP_RESULT="${EPOCH_BENCH_ROOT}/epoch_benchmark_results/prep_${EPOCH_PROFILE}_$(date +%Y%m%d_%H%M%S)"
        mkdir -p "${PREP_RESULT}"
        {
            echo "run_mode=prep-only"
            echo "profile=${EPOCH_PROFILE}"
            echo "EPOCH_BENCH_ROOT=${EPOCH_BENCH_ROOT}"
            echo "EPOCH_PHYS_LOGICAL_GAP_LIMIT=${EPOCH_PHYS_LOGICAL_GAP_LIMIT:-}"
            echo "EPOCH_PHYSICAL_INTERVAL_MS=${EPOCH_PHYSICAL_INTERVAL_MS:-}"
            echo "started_at=$(date -Iseconds)"
        } | tee "${PREP_RESULT}/summary.txt"
        epoch_cluster_deploy
        epoch_cluster_configure
        epoch_cluster_restart
        epoch_check_nodes_running | tee -a "${PREP_RESULT}/summary.txt"
        echo "finished_at=$(date -Iseconds)" | tee -a "${PREP_RESULT}/summary.txt"
        epoch_snapshot_epoch_guc "${PREP_RESULT}"
        echo "result_dir=${PREP_RESULT}" | tee -a "${PREP_RESULT}/summary.txt"
        echo "DONE: ${PREP_RESULT}"
        ;;
esac
