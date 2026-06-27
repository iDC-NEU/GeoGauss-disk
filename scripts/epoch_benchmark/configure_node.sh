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

# 在非目标节点机器上配置远程节点时，SSH 转发到对应主机执行
if ! epoch_is_local_node "${NODE_ID}"; then
    REMOTE="$(epoch_ssh_target "${NODE_ID}")"
    RB="$(epoch_bench_root_for_node "${NODE_ID}")"
    ssh -o BatchMode=yes -o ConnectTimeout=10 "${REMOTE}" \
        "export EPOCH_LOCAL_NODE_ID=${NODE_ID}; export EPOCH_BENCH_ROOT=${RB}; export EPOCH_PROFILE=${EPOCH_PROFILE:-benchmark}; \
        export EPOCH_ENABLE_GEOGAUSS_OPT=${EPOCH_ENABLE_GEOGAUSS_OPT:-off}; \
        export EPOCH_PHYS_LOGICAL_GAP_LIMIT=${EPOCH_PHYS_LOGICAL_GAP_LIMIT:-}; \
        export EPOCH_PHYSICAL_INTERVAL_MS=${EPOCH_PHYSICAL_INTERVAL_MS:-}; \
        bash ${RB}/openGauss-server/scripts/epoch_benchmark/configure_node.sh ${NODE_ID}"
    exit $?
fi

DATA_DIR="$(data_dir_for_node "${NODE_ID}")"
NODE_IP="$(epoch_node_ip "${NODE_ID}")"
CONF="${DATA_DIR}/postgresql.conf"
HBA="${DATA_DIR}/pg_hba.conf"

if [ ! -f "${CONF}" ]; then
    echo "missing ${CONF}, run gs_initdb first, e.g.:" >&2
    echo "  mkdir -p ${DATA_DIR}" >&2
    echo "  gs_initdb -D ${DATA_DIR} --nodename=epoch-node${NODE_ID} -w \"$(epoch_super_password_for_node "${NODE_ID}")\"" >&2
    exit 1
fi

set_conf() {
    local key="$1"
    local value="$2"
    if grep -qE "^[[:space:]]*#?${key}[[:space:]]*=" "${CONF}"; then
        sed -i "s/^[#[:space:]]*${key}[[:space:]]*=.*/${key} = ${value}/" "${CONF}"
    else
        echo "${key} = ${value}" >> "${CONF}"
    fi
}

# 旧版脚本写入、当前代码未注册的 GUC，避免 postgresql.conf 残留未知参数
remove_stale_conf() {
    local key="$1"
    sed -i "/^[[:space:]]*#\\?[[:space:]]*${key}[[:space:]]*=/d" "${CONF}"
}

# 勿强制 listen_addresses='*' 或具体网卡 IP（本环境 HA socket 会失败）；保持 gs_initdb 默认即可
sed -i '/^listen_addresses[[:space:]]*=/d' "${CONF}"

set_conf port "${EPOCH_DB_PORT}"

# 关闭主备复制（epoch 多主为独立实例）
sed -i "s/^[[:space:]]*replconninfo1.*/# replconninfo1 disabled for epoch multimaster/" "${CONF}" 2>/dev/null || true

# 与可启动的 opengauss-occ 对齐
set_conf shared_buffers "1024MB"
set_conf max_process_memory "16GB"
set_conf max_connections "2000"
set_conf work_mem "16MB"
set_conf maintenance_work_mem "256MB"
set_conf wal_buffers "16MB"
set_conf checkpoint_completion_target "0.9"
set_conf synchronous_commit "on"
set_conf default_transaction_isolation "'repeatable read'"
set_conf full_page_writes "off"

# 清理已回退代码中不存在的 epoch GUC，以及 gs_initdb 模板里集群版残留项
for stale in \
    epoch_logical_advance_batch \
    epoch_physical_advance_batch \
    epoch_worker_poll_rounds \
    epoch_seal_settle_us \
    epoch_peer_lag_slack \
    epoch_async_balanced \
    epoch_async_peer_pace \
    epoch_async_commit_wait_ms \
    epoch_async_admit_ahead \
    epoch_async_advance_batch \
    epoch_async_peer_pace_slack \
    epoch_async_admit_gap \
    epoch_async_admit_pack_slack \
    epoch_async_start_gate_max_wait_ms \
    epoch_apply_shard_only \
    epoch_shard_warehouse_lo \
    epoch_shard_warehouse_hi \
    default_storage_nodegroup \
    expected_computing_nodegroup; do
    remove_stale_conf "${stale}"
done

# HCJ ADDBY epoch multimaster（与 src/epoch/e_config.cpp 对齐，改后须 restart）
# EPOCH_PROFILE 内置参数（仅 export EPOCH_PROFILE=xxx 即可）：
#   benchmark   — sync,  gap=5,  physical=100ms, perf
#   stats       — sync,  gap=5,  physical=100ms, perf+diag+occ_stats
#   async       — async, gap=16, physical=5ms,   perf
#   async-wan   — async, gap=48, physical=3ms,   perf
#   async-stats — async, gap=20, physical=10ms,  perf+diag（推荐实验档）
# 可选覆盖：EPOCH_PHYS_LOGICAL_GAP_LIMIT, EPOCH_PHYSICAL_INTERVAL_MS
# 可选覆盖：EPOCH_PHYS_LOGICAL_GAP_LIMIT, EPOCH_PHYSICAL_INTERVAL_MS
# GeoGauss 模块开关（运行时，默认 off = 当前行为）：
#   EPOCH_ENABLE_GEOGAUSS_OPT=off — enable_occ_staging/enable_epoch_dyn_net 均为 off
#   EPOCH_ENABLE_GEOGAUSS_OPT=on  — 开启 occ-opt 版本暂存 + dynamic-net 异构网络
# 可选（仅 geogauss=on 时生效）：
#   EPOCH_DYN_NET_CATCHUP, EPOCH_DYN_NET_PROXY, EPOCH_DYN_NET_SLOW_NODES
EPOCH_PROFILE="${EPOCH_PROFILE:-benchmark}"
EPOCH_ENABLE_GEOGAUSS_OPT="${EPOCH_ENABLE_GEOGAUSS_OPT:-off}"
EPOCH_PHYSICAL_INTERVAL_MS="${EPOCH_PHYSICAL_INTERVAL_MS:-}"
EPOCH_PHYS_LOGICAL_GAP_LIMIT="${EPOCH_PHYS_LOGICAL_GAP_LIMIT:-}"

geogauss_opt_val="off"
occ_staging_val="off"
epoch_dyn_net_val="off"
epoch_dyn_net_catchup_val="off"
epoch_dyn_net_proxy_val="off"
epoch_dyn_net_slow_nodes_val="''"

case "${EPOCH_ENABLE_GEOGAUSS_OPT}" in
    on|yes|1|true|TRUE)
        geogauss_opt_val="on"
        occ_staging_val="on"
        epoch_dyn_net_val="on"
        epoch_dyn_net_catchup_val="${EPOCH_DYN_NET_CATCHUP:-off}"
        epoch_dyn_net_proxy_val="${EPOCH_DYN_NET_PROXY:-off}"
        if [ -n "${EPOCH_DYN_NET_SLOW_NODES:-}" ]; then
            epoch_dyn_net_slow_nodes_val="'${EPOCH_DYN_NET_SLOW_NODES}'"
        else
            epoch_dyn_net_slow_nodes_val="''"
        fi
        ;;
    off|no|0|false|FALSE|"")
        ;;
    *)
        echo "WARN: unknown EPOCH_ENABLE_GEOGAUSS_OPT=${EPOCH_ENABLE_GEOGAUSS_OPT}, using off" >&2
        ;;
esac

set_conf enable_occ_staging "${occ_staging_val}"
set_conf enable_epoch_dyn_net "${epoch_dyn_net_val}"
set_conf epoch_dyn_net_catchup "${epoch_dyn_net_catchup_val}"
set_conf epoch_dyn_net_proxy "${epoch_dyn_net_proxy_val}"
if [ "${epoch_dyn_net_slow_nodes_val}" = "''" ]; then
    set_conf epoch_dyn_net_slow_nodes "''"
else
    set_conf epoch_dyn_net_slow_nodes "${epoch_dyn_net_slow_nodes_val}"
fi

set_conf enable_occ_disk "on"
set_conf enable_epoch_multimaster "on"
set_conf epoch_recv_diag "off"
set_conf epoch_diag "off"
set_conf epoch_perf_log "off"
set_conf occ_enable_stats_log "off"

commit_sync_val="on"
gap_limit_val="5"
physical_interval_ms="100"

case "${EPOCH_PROFILE}" in
    benchmark)
        set_conf epoch_commit_sync "on"
        set_conf epoch_perf_log "on"
        commit_sync_val="on"
        gap_limit_val="5"
        physical_interval_ms="100"
        ;;
    stats)
        set_conf occ_enable_stats_log "on"
        set_conf epoch_commit_sync "on"
        set_conf epoch_recv_diag "on"
        set_conf epoch_diag "on"
        set_conf epoch_perf_log "on"
        commit_sync_val="on"
        gap_limit_val="5"
        physical_interval_ms="100"
        ;;
    async)
        set_conf epoch_commit_sync "off"
        set_conf epoch_perf_log "on"
        commit_sync_val="off"
        gap_limit_val="16"
        physical_interval_ms="5"
        ;;
    async-wan)
        set_conf epoch_commit_sync "off"
        set_conf epoch_perf_log "on"
        commit_sync_val="off"
        gap_limit_val="48"
        physical_interval_ms="3"
        ;;
    async-stats)
        set_conf occ_enable_stats_log "on"
        set_conf epoch_commit_sync "off"
        set_conf epoch_recv_diag "on"
        set_conf epoch_diag "on"
        set_conf epoch_perf_log "on"
        commit_sync_val="off"
        gap_limit_val="20"
        physical_interval_ms="10"
        ;;
    *)
        echo "WARN: unknown EPOCH_PROFILE=${EPOCH_PROFILE}, using benchmark defaults" >&2
        set_conf epoch_commit_sync "on"
        set_conf epoch_perf_log "on"
        commit_sync_val="on"
        gap_limit_val="5"
        physical_interval_ms="100"
        ;;
esac

if [ -n "${EPOCH_PHYS_LOGICAL_GAP_LIMIT:-}" ]; then
    gap_limit_val="${EPOCH_PHYS_LOGICAL_GAP_LIMIT}"
fi
if [ -n "${EPOCH_PHYSICAL_INTERVAL_MS:-}" ]; then
    physical_interval_ms="${EPOCH_PHYSICAL_INTERVAL_MS}"
fi

set_conf epoch_node_id "${NODE_ID}"
set_conf epoch_total_nodes "${EPOCH_TOTAL_NODES}"
set_conf epoch_peer_hosts "'${EPOCH_PEER_HOSTS}'"
set_conf epoch_data_port_base "${EPOCH_ZMQ_PORT_BASE}"
set_conf epoch_physical_interval_ms "${physical_interval_ms}"
set_conf epoch_delay_slots "0"
set_conf epoch_cache_slots "8192"
set_conf epoch_phys_logical_gap_limit "${gap_limit_val}"
set_conf epoch_merge_threads "2"
set_conf epoch_commit_threads "2"
set_conf epoch_pack_threads "1"
set_conf epoch_async_stale_pack_limit "${EPOCH_ASYNC_STALE_PACK_LIMIT:-1200}"
set_conf epoch_async_stagnation_ms "${EPOCH_ASYNC_STAGNATION_MS:-500}"
set_conf epoch_async_assign_max_sleep_ms "${EPOCH_ASYNC_ASSIGN_MAX_SLEEP_MS:-20}"
set_conf epoch_async_max_tpmc "${EPOCH_ASYNC_MAX_TPMC:-3500}"
set_conf epoch_async_tpmc_write_ratio_percent "${EPOCH_ASYNC_TPMC_WRITE_RATIO_PERCENT:-180}"
set_conf epoch_apply_database "'${EPOCH_TPCC_DB}'"
set_conf epoch_apply_user "'$(epoch_super_user_for_node "${NODE_ID}")'"

for ip in "${EPOCH_NODE0_IP}" "${EPOCH_NODE1_IP}" "${EPOCH_NODE2_IP}" "127.0.0.1"; do
    if ! grep -q "${ip}/32" "${HBA}"; then
        echo "host    all             all             ${ip}/32            trust" >> "${HBA}"
    fi
done

verify_conf() {
    local key="$1"
    local expect="$2"
    local actual
    actual="$(grep -E "^[[:space:]]*${key}[[:space:]]*=" "${CONF}" | tail -1 | sed 's/.*=[[:space:]]*//' | tr -d "'")"
    if [ "${actual}" != "${expect}" ]; then
        echo "ERROR: ${CONF} ${key}=${actual:-<missing>}, expected ${expect}" >&2
        echo "hint: check EPOCH_DATA_ROOT=${EPOCH_DATA_ROOT}, profile=${EPOCH_PROFILE}" >&2
        exit 1
    fi
}

echo "configured ${DATA_DIR} (node ${NODE_ID}, ip ${NODE_IP}, port ${EPOCH_DB_PORT}, profile=${EPOCH_PROFILE}, geogauss=${geogauss_opt_val})"
echo "edited: ${CONF}"
verify_conf epoch_commit_sync "${commit_sync_val}"
verify_conf epoch_phys_logical_gap_limit "${gap_limit_val}"
verify_conf epoch_physical_interval_ms "${physical_interval_ms}"
verify_conf enable_occ_staging "${occ_staging_val}"
verify_conf enable_epoch_dyn_net "${epoch_dyn_net_val}"
verify_conf synchronous_commit "on"
verify_conf default_transaction_isolation "repeatable read"
echo "verified: epoch_commit_sync=${commit_sync_val}, gap=${gap_limit_val}, interval_ms=${physical_interval_ms}"
echo "verified: synchronous_commit=on, default_transaction_isolation=repeatable read"
echo "verified: enable_occ_staging=${occ_staging_val}, enable_epoch_dyn_net=${epoch_dyn_net_val}, geogauss=${geogauss_opt_val}"
echo "epoch: profile=${EPOCH_PROFILE}, commit_sync=${commit_sync_val}, gap=${gap_limit_val}, physical=${physical_interval_ms}ms"
case "${EPOCH_PROFILE}" in
    benchmark)
        echo "epoch: benchmark — sync TPCC 稳定语义, perf on"
        ;;
    stats)
        echo "epoch: stats — sync + recv/epoch/occ diag + perf"
        ;;
    async|async-wan|async-stats)
        echo "epoch: async — COMMIT 不等待关窗；gap 顶满 stale 时对称发号 throttle"
        echo "epoch: async throttle stale_pack=${EPOCH_ASYNC_STALE_PACK_LIMIT:-1200} stagnation_ms=${EPOCH_ASYNC_STAGNATION_MS:-500} max_sleep_ms=${EPOCH_ASYNC_ASSIGN_MAX_SLEEP_MS:-20}"
        echo "epoch: async commit rate cap max_tpmc=${EPOCH_ASYNC_MAX_TPMC:-3500} write_ratio_pct=${EPOCH_ASYNC_TPMC_WRITE_RATIO_PERCENT:-180}"
        ;;
esac
echo "IMPORTANT: epoch GUCs are PGC_POSTMASTER — gs_ctl restart required (reload is NOT enough)."
