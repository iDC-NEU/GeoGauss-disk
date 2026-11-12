#!/usr/bin/env bash
# 双节点 epoch 集群预处理（不含压测）：
#   restore → deploy → configure → restart → 健康检查
# TPCC 由你在两侧自行 ./runBenchmark.sh props...
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

export EPOCH_BENCH_ROOT="${EPOCH_BENCH_ROOT:-${HOME}/pacer-bench}"
# EPOCH_PROFILE、慢节点、RTT、带宽在 env.sh 里改
# thread 数由 configure_node.sh 设定；仅在显式 export 时覆盖
export EPOCH_RECV_DIAG_VERBOSE="${EPOCH_RECV_DIAG_VERBOSE:-off}"
export EPOCH_LIFECYCLE_LOG="${EPOCH_LIFECYCLE_LOG:-on}"

# 选库（改这里即可；prep 用整目录 bak→live，与 EPOCH_TPCC_DB 标签无关）：
#   test    — 当前主测（100w，props *-33/*-50）
#   tpcc300 — 旧同目录方案已弃用；300w 请另 gs_initdb 新 data dir 再 load
export EPOCH_TPCC_DB=test

# prep = 完整预处理；deploy-only = 仅同步二进制
RUN_MODE=prep

usage() {
    cat <<EOF
usage: $(basename "$0") [OPTION]

  (default)     restore → deploy → configure → restart → 健康检查
                （不跑 TPCC；压测请自行 runBenchmark.sh）
  --deploy-only 仅 rsync 二进制到远程节点

  env.sh 里改（wait/dbr/pa/pacer 共用）。configure_node.sh 写库参数时会按这些值执行 tc：
    EPOCH_PROFILE=${EPOCH_PROFILE}          # wait | dbr | pa | pacer
    EPOCH_DYN_NET_SLOW_NODES=${EPOCH_DYN_NET_SLOW_NODES:-}  # 慢节点，0=node1 1=node2
    EPOCH_NETWORK_RTT_MS=${EPOCH_NETWORK_RTT_MS:-}         # ping 应看到的往返延迟
    EPOCH_DYN_NET_BW_MODE=${EPOCH_DYN_NET_BW_MODE:-}       # uplimit|downlimit|bilimit|unlimit
    EPOCH_DYN_NET_BW_MBPS=${EPOCH_DYN_NET_BW_MBPS:-}
  本脚本启动测试（prep）会调用 configure，因此会自动加延迟和限速。
  单独核对：bash scripts/epoch_benchmark/apply_netem.sh status
  本脚本顶部可改：
    EPOCH_TPCC_DB=${EPOCH_TPCC_DB}   # tpcc300 | test

  prep 后建议的手动压测 props：
    tpcc300 → node1: props.epoch.node0-300w ；node2: props.epoch.node2-300w
    test    → node1: props.epoch.node0-33 ；node2: props.epoch.node2-33

  可选覆盖: EPOCH_COMMIT_THREADS, EPOCH_MERGE_THREADS
EOF
}

epoch_invoke_deploy() {
    env -u LD_LIBRARY_PATH \
        EPOCH_BENCH_ROOT="${EPOCH_BENCH_ROOT}" \
        EPOCH_PROFILE="${EPOCH_PROFILE:-}" \
        EPOCH_SKIP_NODE="${EPOCH_SKIP_NODE:-}" \
        EPOCH_SKIP_NODE2="${EPOCH_SKIP_NODE2:-}" \
        PATH="/usr/bin:/bin:/usr/local/bin" \
        bash "${SCRIPT_DIR}/deploy_to_remote.sh" "$@"
}

epoch_cluster_deploy() {
    echo "=== [1/3] deploy binary to remote nodes ==="
    for n in $(epoch_remote_deploy_node_ids); do
        epoch_invoke_deploy "${n}"
    done
}

epoch_cluster_configure() {
    echo "=== [2/3] configure profile nodes (profile=${EPOCH_PROFILE}, db=${EPOCH_TPCC_DB}) ==="
    for n in $(epoch_profile_node_ids); do
        bash "${SCRIPT_DIR}/configure_node.sh" "${n}"
    done
}

epoch_cluster_restart() {
    echo "=== [3/3] stop profile nodes ==="
    for n in $(epoch_profile_node_ids); do
        bash "${SCRIPT_DIR}/stop_node.sh" "${n}" || true
    done
    sleep 3
    echo "=== start profile nodes ==="
    for n in $(epoch_profile_node_ids); do
        bash "${SCRIPT_DIR}/start_node.sh" "${n}"
    done
    sleep 5
}

epoch_sync_benchmark_client() {
    local bench_root="${EPOCH_BENCH_ROOT}/benchmarksql-5.0-epoch"
    local dist_jar="${bench_root}/dist/BenchmarkSQL-5.0.jar"
    local lib_jar="${bench_root}/lib/BenchmarkSQL-5.0.jar"
    local dist_md5

    if [ ! -f "${dist_jar}" ]; then
        echo "ERROR: missing ${dist_jar}" >&2
        exit 1
    fi
    dist_md5="$(md5sum "${dist_jar}" | awk '{print $1}')"

    echo "=== sync BenchmarkSQL client: dist -> lib (local) ==="
    cp -a "${dist_jar}" "${lib_jar}"
    echo "node1 (id=1): lib/BenchmarkSQL-5.0.jar <= dist (${dist_md5})"

    for n in $(epoch_remote_deploy_node_ids); do
        local rb remote
        rb="$(epoch_bench_root_for_node "${n}")"
        remote="$(epoch_ssh_target "${n}")"
        echo "=== sync BenchmarkSQL client: dist -> lib + build/ + props (remote) ==="
        rsync -avz "${dist_jar}" "${remote}:${rb}/benchmarksql-5.0-epoch/lib/"
        if [ -d "${bench_root}/build" ]; then
            rsync -avz "${bench_root}/build/" "${remote}:${rb}/benchmarksql-5.0-epoch/build/"
        fi
        rsync -avz "${bench_root}/run/props.epoch."* \
            "${remote}:${rb}/benchmarksql-5.0-epoch/run/" 2>/dev/null || true
        echo "node${n} (id=${n}): lib/BenchmarkSQL-5.0.jar <= dist (${dist_md5})"
    done
}

epoch_check_nodes_running() {
    local n rb detail sql_timeout
    if [ "${EPOCH_TPCC_DB}" = "tpcc300" ]; then
        sql_timeout=30
    else
        sql_timeout=8
    fi
    echo "=== postflight: gaussdb status (db=${EPOCH_TPCC_DB}, sql_timeout=${sql_timeout}s) ==="
    for n in $(epoch_profile_node_ids); do
        rb="$(epoch_bench_root_for_node "${n}")"
        dd="$(epoch_data_dir_for_node "${n}")"
        if epoch_is_local_node "${n}"; then
            if ! gs_ctl status -D "${dd}" >/dev/null 2>&1; then
                detail="$(gs_ctl status -D "${dd}" 2>&1 | tail -2 || true)"
                echo "ERROR: $(epoch_node_display_name "${n}") (id=${n}) failed gs_ctl status (${dd})" >&2
                [ -n "${detail}" ] && echo "  ${detail}" >&2
                exit 1
            fi
            if ! timeout "${sql_timeout}" gsql -d "${EPOCH_TPCC_DB}" -p "${EPOCH_DB_PORT}" -tAc 'SELECT 1' >/dev/null 2>&1; then
                echo "ERROR: $(epoch_node_display_name "${n}") (id=${n}) not accepting SQL on db=${EPOCH_TPCC_DB} within ${sql_timeout}s" >&2
                exit 1
            fi
        else
            if ! ssh -o BatchMode=yes -o ConnectTimeout=10 "$(epoch_ssh_target "${n}")" \
                "$(epoch_remote_gauss_env "${n}"); gs_ctl status -D ${dd} >/dev/null 2>&1"; then
                echo "ERROR: $(epoch_node_display_name "${n}") (id=${n}) failed gs_ctl status (${dd})" >&2
                exit 1
            fi
            if ! ssh -o BatchMode=yes -o ConnectTimeout=10 "$(epoch_ssh_target "${n}")" \
                "$(epoch_remote_gauss_env "${n}"); timeout ${sql_timeout} gsql -d ${EPOCH_TPCC_DB} -p ${EPOCH_DB_PORT} -tAc 'SELECT 1' >/dev/null 2>&1"; then
                echo "ERROR: $(epoch_node_display_name "${n}") (id=${n}) not accepting SQL on db=${EPOCH_TPCC_DB} within ${sql_timeout}s" >&2
                exit 1
            fi
        fi
        echo "$(epoch_node_display_name "${n}") (id=${n}): running (db=${EPOCH_TPCC_DB})"
    done
}

epoch_snapshot_epoch_guc() {
    local result_dir="$1"
    echo "=== postflight: epoch GUC snapshot ==="
    for n in $(epoch_profile_node_ids); do
        local dest="${result_dir}/postgresql.epoch.node${n}.conf"
        if epoch_is_local_node "${n}"; then
            grep -E '^epoch_|^enable_epoch|^occ_enable_stats' \
                "$(data_dir_for_node "${n}")/postgresql.conf" > "${dest}" 2>/dev/null || true
        else
            local rb dd
            rb="$(epoch_bench_root_for_node "${n}")"
            dd="$(epoch_data_dir_for_node "${n}")"
            ssh -o BatchMode=yes "$(epoch_ssh_target "${n}")" \
                "grep -E '^epoch_|^enable_epoch|^occ_enable_stats' ${dd}/postgresql.conf" \
                > "${dest}" 2>/dev/null || true
        fi
    done
    {
        echo "lifecycle_log_hint=各节点 DataDir/epoch_lifecycle_*.log（logical 阶段耗时）"
        echo "apply_pool_hint=epoch_commit_threads 见 ${result_dir}/postgresql.epoch.node*.conf"
        echo "merge_pool_hint=epoch_merge_threads 见同目录 conf"
        echo "recv_diag_hint=epoch_recv_diag_*.log（verbose=${EPOCH_RECV_DIAG_VERBOSE})"
        echo "tpcc_hint=本脚本不压测；请自行：cd \$EPOCH_BENCHMARKSQL && ./runBenchmark.sh \$(epoch_tpcc_props_for_node N)"
    } | tee -a "${result_dir}/summary.txt"
}

epoch_do_prep() {
    local prep_result="$1"
    {
        echo "run_mode=${RUN_MODE}"
        echo "profile=${EPOCH_PROFILE}"
        echo "EPOCH_TPCC_DB=${EPOCH_TPCC_DB}"
        echo "EPOCH_LOAD_WAREHOUSES=${EPOCH_LOAD_WAREHOUSES}"
        echo "EPOCH_RUN_WAREHOUSES_PER_NODE=${EPOCH_RUN_WAREHOUSES_PER_NODE}"
        echo "props_node1=$(epoch_tpcc_props_for_node 1)"
        echo "props_node2=$(epoch_tpcc_props_for_node 2)"
        echo "EPOCH_BENCH_ROOT=${EPOCH_BENCH_ROOT}"
        echo "EPOCH_COMMIT_THREADS=${EPOCH_COMMIT_THREADS:-<profile default>}"
        echo "EPOCH_MERGE_THREADS=${EPOCH_MERGE_THREADS:-<profile default>}"
        echo "EPOCH_RECV_DIAG_VERBOSE=${EPOCH_RECV_DIAG_VERBOSE}"
        echo "EPOCH_LIFECYCLE_LOG=${EPOCH_LIFECYCLE_LOG}"
        echo "EPOCH_SKIP_NODE=${EPOCH_SKIP_NODE:-0}"
        echo "effective_total_nodes=$(epoch_effective_total_nodes)"
        echo "effective_peer_hosts=$(epoch_effective_peer_hosts)"
        echo "profile_nodes=$(epoch_profile_node_ids)"
        echo "started_at=$(date -Iseconds)"
    } | tee "${prep_result}/summary.txt"

    echo "=== stop profile nodes before datadir/db restore (db=${EPOCH_TPCC_DB}) ==="
    for n in $(epoch_profile_node_ids); do
        bash "${SCRIPT_DIR}/stop_node.sh" "${n}" || true
    done
    sleep 2
    bash "${SCRIPT_DIR}/restore_tpcc_datadir.sh" --no-stop-start
    epoch_cluster_deploy
    epoch_cluster_configure
    epoch_cluster_restart
    epoch_sync_benchmark_client | tee -a "${prep_result}/summary.txt"
    epoch_check_nodes_running | tee -a "${prep_result}/summary.txt"
    echo "finished_prep_at=$(date -Iseconds)" | tee -a "${prep_result}/summary.txt"
    epoch_snapshot_epoch_guc "${prep_result}"
    echo ""
    echo "NEXT: 自行压测（本脚本不跑 TPCC），例如："
    local n rb
    for n in $(epoch_profile_node_ids); do
        if [ "${n}" = "1" ]; then
            echo "  node1: cd ${EPOCH_BENCHMARKSQL} && ./runBenchmark.sh $(epoch_tpcc_props_for_node 1)"
        else
            rb="$(epoch_bench_root_for_node "${n}")"
            echo "  node${n}: cd ${rb}/benchmarksql-5.0-epoch/run && ./runBenchmark.sh $(epoch_tpcc_props_for_node "${n}")"
        fi
    done
}

while [ $# -gt 0 ]; do
    case "$1" in
        --prep-only)
            # 兼容旧参数名：与默认相同
            RUN_MODE=prep
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

case "${RUN_MODE}" in
    deploy-only)
        epoch_cluster_deploy
        echo "DONE: deploy-only (remote nodes synced)"
        ;;
    prep|*)
        PREP_RESULT="${EPOCH_BENCH_ROOT}/epoch_benchmark_results/prep_${EPOCH_PROFILE}_${EPOCH_TPCC_DB}_$(date +%Y%m%d_%H%M%S)"
        mkdir -p "${PREP_RESULT}"
        epoch_do_prep "${PREP_RESULT}"
        echo "result_dir=${PREP_RESULT}" | tee -a "${PREP_RESULT}/summary.txt"
        echo "DONE: ${PREP_RESULT}"
        ;;
esac
