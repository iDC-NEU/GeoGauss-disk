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
        "export EPOCH_LOCAL_NODE_ID=${NODE_ID}; export EPOCH_BENCH_ROOT=${RB}; \
        export EPOCH_PROFILE=${EPOCH_PROFILE:-benchmark}; \
        export EPOCH_TPCC_DB=${EPOCH_TPCC_DB:-tpcc300}; \
        export EPOCH_ENABLE_GEOGAUSS_OPT=${EPOCH_ENABLE_GEOGAUSS_OPT:-off}; \
        export EPOCH_PHYS_LOGICAL_GAP_LIMIT=${EPOCH_PHYS_LOGICAL_GAP_LIMIT:-}; \
        export EPOCH_PHYSICAL_TXNS_PER_EPOCH=${EPOCH_PHYSICAL_TXNS_PER_EPOCH:-}; \
        export EPOCH_SYNC_START_GATE=${EPOCH_SYNC_START_GATE:-}; \
        export EPOCH_COMMIT_THREADS=${EPOCH_COMMIT_THREADS:-}; \
        export EPOCH_MERGE_THREADS=${EPOCH_MERGE_THREADS:-}; \
        export EPOCH_TIP_FLUSH_THREADS=${EPOCH_TIP_FLUSH_THREADS:-}; \
        export EPOCH_SKIP_NODE=${EPOCH_SKIP_NODE}; \
        export EPOCH_SKIP_NODE2=$( [ \"${EPOCH_SKIP_NODE}\" = 2 ] && echo 1 || echo 0 ); \
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
    echo "  gs_initdb -D ${DATA_DIR} --nodename=$(epoch_node_display_name "${NODE_ID}") -w \"$(epoch_super_password_for_node "${NODE_ID}")\"" >&2
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

# 大缓冲池：缓解 apply find ReadBuffer miss（数据~25G 时过小 SB 易抖动）
# 机器约 125GB：SB 16GB → 单实例 shm 约 ~30GB（原 32GB SB 时约 61GB，减半以留余量给 OS/双节点/其它进程）
# max_process_memory 须明显大于 shared_buffers+cstore；约按 SB≈40% 取 40GB
set_conf shared_buffers "16GB"
set_conf max_process_memory "40GB"
set_conf effective_cache_size "40GB"
set_conf max_connections "2000"
set_conf work_mem "16MB"
set_conf maintenance_work_mem "1GB"
set_conf wal_buffers "64MB"
set_conf checkpoint_completion_target "0.9"
set_conf default_transaction_isolation "'repeatable read'"
set_conf full_page_writes "off"

# 清理已回退代码中不存在的 epoch GUC，以及 gs_initdb 模板里集群版残留项
for stale in \
    epoch_logical_advance_batch \
    epoch_physical_interval_ms \
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
    epoch_commit_sync \
    epoch_async_stale_pack_limit \
    epoch_async_stagnation_ms \
    epoch_async_assign_max_sleep_ms \
    epoch_async_max_tpmc \
    epoch_async_tpmc_write_ratio_percent \
    epoch_apply_shard_only \
    epoch_shard_warehouse_lo \
    epoch_shard_warehouse_hi \
    default_storage_nodegroup \
    expected_computing_nodegroup; do
    remove_stale_conf "${stale}"
done

# HCJ ADDBY epoch multimaster（与 src/epoch/e_config.cpp 对齐，改后须 restart）
# EPOCH_PROFILE 内置参数（仅 export EPOCH_PROFILE=xxx 即可）：
#   benchmark            — gap=5, perf, start_gate=on, 2-node
#   stats                — gap=5, perf+diag+occ_stats, start_gate=on, synchronous_commit=on
#   stats-async-commit   — 同 stats，synchronous_commit=off（apply commit 不做 XLogWaitFlush，验证 WAL 瓶颈）
#   stats-nogate         — start_gate=off, gap=5, physical_txns_per_epoch=25
#   single-node-baseline — epoch_total_nodes=1、无 peer_hosts，start_gate=on；仅 node1 压测对比用
# 可选覆盖：EPOCH_PHYS_LOGICAL_GAP_LIMIT, EPOCH_PHYSICAL_TXNS_PER_EPOCH, EPOCH_SYNC_START_GATE
# GeoGauss 模块开关（运行时，默认 off = 当前行为）：
#   EPOCH_ENABLE_GEOGAUSS_OPT=off — enable_occ_staging/enable_epoch_dyn_net 均为 off
#   EPOCH_ENABLE_GEOGAUSS_OPT=on  — 开启 occ-opt 版本暂存 + dynamic-net 异构网络
# 可选（仅 geogauss=on 时生效）：
#   EPOCH_DYN_NET_CATCHUP, EPOCH_DYN_NET_PROXY, EPOCH_DYN_NET_SLOW_NODES
EPOCH_PROFILE="${EPOCH_PROFILE:-benchmark}"
if [ "${EPOCH_PROFILE}" = "single-node-baseline" ] && [ "${NODE_ID}" != "1" ]; then
    echo "ERROR: EPOCH_PROFILE=single-node-baseline 仅适用于 node1（NODE_ID=1）" >&2
    exit 1
fi
EPOCH_ENABLE_GEOGAUSS_OPT="${EPOCH_ENABLE_GEOGAUSS_OPT:-off}"
EPOCH_PHYSICAL_TXNS_PER_EPOCH="${EPOCH_PHYSICAL_TXNS_PER_EPOCH:-}"
EPOCH_PHYS_LOGICAL_GAP_LIMIT="${EPOCH_PHYS_LOGICAL_GAP_LIMIT:-}"
EPOCH_SYNC_START_GATE="${EPOCH_SYNC_START_GATE:-}"

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

sync_start_gate_val="on"
gap_limit_val="5"
physical_txns_per_epoch_val="0"
commit_threads_val="1"
merge_threads_val="1"
tip_flush_threads_val="1"
synchronous_commit_val="on"
epoch_total_nodes_val=""
epoch_peer_hosts_val=""

case "${EPOCH_PROFILE}" in
    benchmark)
        set_conf epoch_perf_log "on"
        set_conf epoch_apply_profile "on"
        sync_start_gate_val="on"
        gap_limit_val="5"
        commit_threads_val="5"
        merge_threads_val="5"
        ;;
    stats)
        set_conf occ_enable_stats_log "on"
        set_conf epoch_recv_diag "on"
        set_conf epoch_recv_diag_verbose "off"
        set_conf epoch_lifecycle_log "on"
        set_conf epoch_diag "on"
        set_conf epoch_perf_log "on"
        set_conf epoch_apply_profile "on"
        set_conf epoch_merge_profile "on"
        set_conf epoch_apply_lock_trace_ms "10"
        set_conf log_lock_waits "on"
        set_conf deadlock_timeout "1s"
        sync_start_gate_val="on"
        gap_limit_val="5"
        commit_threads_val="5"
        merge_threads_val="5"
        ;;
    stats-async-commit)
        set_conf occ_enable_stats_log "on"
        set_conf epoch_recv_diag "on"
        set_conf epoch_recv_diag_verbose "off"
        set_conf epoch_lifecycle_log "on"
        set_conf epoch_diag "on"
        set_conf epoch_perf_log "on"
        set_conf epoch_apply_profile "on"
        set_conf epoch_merge_profile "on"
        set_conf epoch_apply_lock_trace_ms "10"
        set_conf log_lock_waits "on"
        set_conf deadlock_timeout "1s"
        sync_start_gate_val="on"
        gap_limit_val="5"
        commit_threads_val="5"
        merge_threads_val="5"
        synchronous_commit_val="off"
        ;;
    stats-nogate)
        set_conf occ_enable_stats_log "on"
        set_conf epoch_recv_diag "on"
        set_conf epoch_recv_diag_verbose "off"
        set_conf epoch_lifecycle_log "on"
        set_conf epoch_diag "on"
        set_conf epoch_perf_log "on"
        set_conf epoch_apply_profile "on"
        set_conf epoch_merge_profile "on"
        set_conf epoch_apply_lock_trace_ms "10"
        set_conf log_lock_waits "on"
        set_conf deadlock_timeout "1s"
        sync_start_gate_val="off"
        gap_limit_val="5"
        physical_txns_per_epoch_val="50"
        commit_threads_val="10"
        merge_threads_val="5"
        synchronous_commit_val="off"
        ;;
    single-node-baseline)
        set_conf epoch_perf_log "on"
        set_conf epoch_recv_diag "off"
        set_conf epoch_recv_diag_verbose "off"
        set_conf epoch_lifecycle_log "off"
        set_conf epoch_diag "off"
        set_conf occ_enable_stats_log "off"
        set_conf epoch_apply_lock_trace_ms "0"
        set_conf log_lock_waits "off"
        sync_start_gate_val="on"
        gap_limit_val="5"
        commit_threads_val="5"
        merge_threads_val="5"
        epoch_total_nodes_val="1"
        epoch_peer_hosts_val="''"
        ;;
    *)
        echo "WARN: unknown EPOCH_PROFILE=${EPOCH_PROFILE}, using benchmark defaults" >&2
        set_conf epoch_perf_log "on"
        sync_start_gate_val="on"
        gap_limit_val="5"
        commit_threads_val="1"
        merge_threads_val="1"
        ;;
esac

set_conf synchronous_commit "${synchronous_commit_val}"

if [ -n "${EPOCH_SYNC_START_GATE:-}" ]; then
    case "${EPOCH_SYNC_START_GATE}" in
        on|yes|1|true|TRUE) sync_start_gate_val="on" ;;
        off|no|0|false|FALSE) sync_start_gate_val="off" ;;
        *)
            echo "WARN: unknown EPOCH_SYNC_START_GATE=${EPOCH_SYNC_START_GATE}, using ${sync_start_gate_val}" >&2
            ;;
    esac
fi

if [ -n "${EPOCH_PHYS_LOGICAL_GAP_LIMIT:-}" ]; then
    gap_limit_val="${EPOCH_PHYS_LOGICAL_GAP_LIMIT}"
fi
if [ -n "${EPOCH_PHYSICAL_TXNS_PER_EPOCH:-}" ]; then
    physical_txns_per_epoch_val="${EPOCH_PHYSICAL_TXNS_PER_EPOCH}"
fi
if [ -n "${EPOCH_COMMIT_THREADS:-}" ]; then
    if ! [[ "${EPOCH_COMMIT_THREADS}" =~ ^[0-9]+$ ]] || [ "${EPOCH_COMMIT_THREADS}" -lt 1 ]; then
        echo "ERROR: EPOCH_COMMIT_THREADS must be integer >= 1, got: ${EPOCH_COMMIT_THREADS}" >&2
        exit 1
    fi
    commit_threads_val="${EPOCH_COMMIT_THREADS}"
    echo "epoch: EPOCH_COMMIT_THREADS=${EPOCH_COMMIT_THREADS} overrides profile default (${EPOCH_PROFILE})"
fi
if [ -n "${EPOCH_MERGE_THREADS:-}" ]; then
    if ! [[ "${EPOCH_MERGE_THREADS}" =~ ^[0-9]+$ ]] || [ "${EPOCH_MERGE_THREADS}" -lt 1 ]; then
        echo "ERROR: EPOCH_MERGE_THREADS must be integer >= 1, got: ${EPOCH_MERGE_THREADS}" >&2
        exit 1
    fi
    merge_threads_val="${EPOCH_MERGE_THREADS}"
fi
if [ -n "${EPOCH_TIP_FLUSH_THREADS:-}" ]; then
    if ! [[ "${EPOCH_TIP_FLUSH_THREADS}" =~ ^[0-9]+$ ]] || [ "${EPOCH_TIP_FLUSH_THREADS}" -lt 1 ]; then
        echo "ERROR: EPOCH_TIP_FLUSH_THREADS must be integer >= 1, got: ${EPOCH_TIP_FLUSH_THREADS}" >&2
        exit 1
    fi
    tip_flush_threads_val="${EPOCH_TIP_FLUSH_THREADS}"
    echo "epoch: EPOCH_TIP_FLUSH_THREADS=${EPOCH_TIP_FLUSH_THREADS} overrides profile default (${EPOCH_PROFILE})"
fi

recv_diag_verbose_val="off"
lifecycle_log_val="off"
case "${EPOCH_PROFILE}" in
    stats|stats-async-commit|stats-nogate)
        recv_diag_verbose_val="off"
        lifecycle_log_val="on"
        ;;
esac
if [ -n "${EPOCH_RECV_DIAG_VERBOSE:-}" ]; then
    recv_diag_verbose_val="${EPOCH_RECV_DIAG_VERBOSE}"
fi
if [ -n "${EPOCH_LIFECYCLE_LOG:-}" ]; then
    lifecycle_log_val="${EPOCH_LIFECYCLE_LOG}"
fi
case "${EPOCH_PROFILE}" in
    stats|stats-async-commit|stats-nogate)
        set_conf epoch_recv_diag_verbose "${recv_diag_verbose_val}"
        set_conf epoch_lifecycle_log "${lifecycle_log_val}"
        ;;
esac

set_conf epoch_node_id "$(epoch_guc_node_id "${NODE_ID}")"
if [ "${EPOCH_PROFILE}" = "single-node-baseline" ]; then
    set_conf epoch_node_id "0"
fi
if [ -n "${epoch_total_nodes_val}" ]; then
    set_conf epoch_total_nodes "${epoch_total_nodes_val}"
else
    set_conf epoch_total_nodes "$(epoch_effective_total_nodes)"
fi
if [ -n "${epoch_peer_hosts_val}" ]; then
    set_conf epoch_peer_hosts "${epoch_peer_hosts_val}"
else
    set_conf epoch_peer_hosts "'$(epoch_effective_peer_hosts)'"
fi
set_conf epoch_data_port_base "${EPOCH_ZMQ_PORT_BASE}"
set_conf epoch_physical_txns_per_epoch "${physical_txns_per_epoch_val}"
set_conf epoch_delay_slots "0"
set_conf epoch_cache_slots "8192"
set_conf epoch_phys_logical_gap_limit "${gap_limit_val}"
set_conf epoch_sync_start_gate "${sync_start_gate_val}"
set_conf epoch_merge_threads "${merge_threads_val}"
set_conf epoch_commit_threads "${commit_threads_val}"
set_conf epoch_tip_flush_threads "${tip_flush_threads_val}"
set_conf epoch_pack_threads "5"
set_conf epoch_txn_queue_capacity "8192"
set_conf epoch_abort_csn_per_epoch "65536"
set_conf epoch_crdt_slot_count "32"
set_conf epoch_crdt_rows_per_slot "8192"
set_conf epoch_crdt_inserts_per_slot "2048"
set_conf epoch_merge_queue_capacity "8192"
set_conf epoch_merge_done_capacity "8192"
set_conf epoch_apply_queue_capacity "8192"
set_conf epoch_apply_done_capacity "8192"
echo "epoch: pipeline coordinator + apply pool（epoch_commit_threads=${commit_threads_val}，多节点时启动 postmaster apply worker 进程）"
echo "epoch: merge pool（epoch_merge_threads=${merge_threads_val}，多节点时启动 postmaster merge worker 进程）"
echo "epoch: tip flush（epoch_tip_flush_threads=${tip_flush_threads_val}，tip staging 后台写回 heap）"
echo "epoch: verify after TPCC — bash scripts/epoch_benchmark/verify_epoch_end_consistency.sh"
set_conf epoch_apply_database "'${EPOCH_TPCC_DB}'"
set_conf epoch_apply_user "'$(epoch_super_user_for_node "${NODE_ID}")'"

EPOCH_HBA_IPS=( "127.0.0.1" )
for _hba_n in $(epoch_active_node_ids); do
    EPOCH_HBA_IPS+=( "$(epoch_node_ip "${_hba_n}")" )
done
for ip in "${EPOCH_HBA_IPS[@]}"; do
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

echo "configured ${DATA_DIR} ($(epoch_node_display_name "${NODE_ID}") id=${NODE_ID}, ip ${NODE_IP}, port ${EPOCH_DB_PORT}, profile=${EPOCH_PROFILE}, geogauss=${geogauss_opt_val})"
echo "edited: ${CONF}"
verify_conf epoch_sync_start_gate "${sync_start_gate_val}"
verify_conf epoch_phys_logical_gap_limit "${gap_limit_val}"
verify_conf epoch_physical_txns_per_epoch "${physical_txns_per_epoch_val}"
verify_conf epoch_commit_threads "${commit_threads_val}"
verify_conf epoch_merge_threads "${merge_threads_val}"
verify_conf epoch_tip_flush_threads "${tip_flush_threads_val}"
if [ -n "${epoch_total_nodes_val}" ]; then
    verify_conf epoch_total_nodes "${epoch_total_nodes_val}"
fi
verify_conf enable_occ_staging "${occ_staging_val}"
verify_conf enable_epoch_dyn_net "${epoch_dyn_net_val}"
verify_conf synchronous_commit "${synchronous_commit_val}"
verify_conf default_transaction_isolation "repeatable read"
echo "verified: sync_start_gate=${sync_start_gate_val}, gap=${gap_limit_val}, physical_txns_per_epoch=${physical_txns_per_epoch_val}, commit_threads=${commit_threads_val}, merge_threads=${merge_threads_val}, tip_flush_threads=${tip_flush_threads_val}"
echo "verified: synchronous_commit=${synchronous_commit_val}, default_transaction_isolation=repeatable read"
echo "verified: enable_occ_staging=${occ_staging_val}, enable_epoch_dyn_net=${epoch_dyn_net_val}, geogauss=${geogauss_opt_val}"
echo "epoch: profile=${EPOCH_PROFILE}, sync_start_gate=${sync_start_gate_val}, gap=${gap_limit_val}"
case "${EPOCH_PROFILE}" in
    benchmark)
        echo "epoch: benchmark — PRE_COMMIT pack + worker defer, perf on, START gate on"
        ;;
    stats)
        echo "epoch: stats — recv_diag(on,verbose=${recv_diag_verbose_val}) lifecycle_log=${lifecycle_log_val} + epoch/occ/perf diag, START gate on, synchronous_commit=on"
        verify_conf epoch_recv_diag "on"
        verify_conf epoch_recv_diag_verbose "${recv_diag_verbose_val}"
        verify_conf epoch_lifecycle_log "${lifecycle_log_val}"
        ;;
    stats-async-commit)
        echo "epoch: stats-async-commit — 同 stats diag/lifecycle/apply_profile，synchronous_commit=off（async WAL commit）"
        verify_conf epoch_recv_diag "on"
        verify_conf epoch_recv_diag_verbose "${recv_diag_verbose_val}"
        verify_conf epoch_lifecycle_log "${lifecycle_log_val}"
        verify_conf epoch_apply_profile "on"
        ;;
    stats-nogate)
        echo "epoch: stats-nogate — nogate + sync_commit=off + gap=${gap_limit_val} + physical_txns_per_epoch=${physical_txns_per_epoch_val}"
        verify_conf epoch_recv_diag "on"
        verify_conf epoch_recv_diag_verbose "${recv_diag_verbose_val}"
        verify_conf epoch_lifecycle_log "${lifecycle_log_val}"
        verify_conf epoch_apply_profile "on"
        verify_conf epoch_physical_txns_per_epoch "${physical_txns_per_epoch_val}"
        ;;
    single-node-baseline)
        echo "epoch: single-node-baseline — epoch_total_nodes=1, peer_hosts empty, START gate GUC on"
        echo "epoch: hint — 代码在 total_nodes<=1 时 E_IsSyncEpochMode 为 false，PRE_COMMIT defer/gate 自旋不生效；与 2-node 验收路径不同，仅作单节点开销对比"
        verify_conf epoch_total_nodes "1"
        verify_conf epoch_sync_start_gate "on"
        verify_conf epoch_recv_diag "off"
        verify_conf epoch_lifecycle_log "off"
        ;;
esac
echo "IMPORTANT: epoch GUCs are PGC_POSTMASTER — gs_ctl restart required (reload is NOT enough)."
