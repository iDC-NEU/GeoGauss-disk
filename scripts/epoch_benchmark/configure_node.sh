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

# wait/dbr/pa/pacer：按 env.sh 的 RTT、带宽、慢节点用 tc 写网卡。其它 profile 清掉规则。
# 只在发起配置的机器上做一次；SSH 到远端改 postgresql.conf 时不再套一层。
epoch_apply_netem_from_profile() {
    if [ -n "${EPOCH_CONFIGURE_REMOTE:-}" ]; then
        return 0
    fi
    local profile="${EPOCH_PROFILE:-wait}"
    case "${profile}" in
        wait|dbr|pa|pacer)
            echo "=== tc netem from profile=${profile} rtt=${EPOCH_NETWORK_RTT_MS:-100}ms bw=${EPOCH_DYN_NET_BW_MODE:-uplimit} ${EPOCH_DYN_NET_BW_MBPS:-100}Mbps slow=${EPOCH_DYN_NET_SLOW_NODES:-} ==="
            bash "${SCRIPT_DIR}/apply_netem.sh" add
            ;;
        *)
            echo "=== tc netem clear (profile=${profile}) ==="
            bash "${SCRIPT_DIR}/apply_netem.sh" del
            ;;
    esac
}

# 在非目标节点机器上配置远程节点时，SSH 转发到对应主机执行
if ! epoch_is_local_node "${NODE_ID}"; then
    REMOTE="$(epoch_ssh_target "${NODE_ID}")"
    RB="$(epoch_bench_root_for_node "${NODE_ID}")"
    ssh -o BatchMode=yes -o ConnectTimeout=10 "${REMOTE}" \
        "export EPOCH_LOCAL_NODE_ID=${NODE_ID}; export EPOCH_BENCH_ROOT=${RB}; \
        export EPOCH_PROFILE=${EPOCH_PROFILE:-wait}; \
        export EPOCH_DYN_NET_SLOW_NODES=${EPOCH_DYN_NET_SLOW_NODES:-}; \
        export EPOCH_NETWORK_RTT_MS=${EPOCH_NETWORK_RTT_MS:-}; \
        export EPOCH_RTT_HEALTHY_MS=${EPOCH_RTT_HEALTHY_MS:-}; \
        export EPOCH_DYN_NET_BW_MODE=${EPOCH_DYN_NET_BW_MODE:-}; \
        export EPOCH_DYN_NET_BW_MBPS=${EPOCH_DYN_NET_BW_MBPS:-}; \
        export EPOCH_DYN_NET_UPLOAD_BPS=${EPOCH_DYN_NET_UPLOAD_BPS:-}; \
        export EPOCH_DYN_NET_DOWNLOAD_BPS=${EPOCH_DYN_NET_DOWNLOAD_BPS:-}; \
        export EPOCH_DYN_NET_CATCHUP_K=${EPOCH_DYN_NET_CATCHUP_K:-}; \
        export EPOCH_TPCC_DB=${EPOCH_TPCC_DB:-tpcc300}; \

        export EPOCH_COMMIT_THREADS=${EPOCH_COMMIT_THREADS:-}; \
        export EPOCH_MERGE_THREADS=${EPOCH_MERGE_THREADS:-}; \
        export EPOCH_TIP_FLUSH_THREADS=${EPOCH_TIP_FLUSH_THREADS:-}; \
        export EPOCH_SKIP_NODE=${EPOCH_SKIP_NODE}; \
        export EPOCH_SKIP_NODE2=$( [ \"${EPOCH_SKIP_NODE}\" = 2 ] && echo 1 || echo 0 ); \
        export EPOCH_CONFIGURE_REMOTE=1; \
        bash ${RB}/openGauss-server/scripts/epoch_benchmark/configure_node.sh ${NODE_ID}"
    rc=$?
    if [ "${rc}" -eq 0 ]; then
        epoch_apply_netem_from_profile
    fi
    exit "${rc}"
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
    epoch_dbr_mode \
    epoch_sync_start_gate \
    enable_epoch_dyn_net \
    epoch_dyn_net_bw_mode \
    default_storage_nodegroup \
    expected_computing_nodegroup; do
    remove_stale_conf "${stale}"
done

# epoch multimaster（与 src/epoch/e_config.cpp 对齐，改后须 restart）
# EPOCH_PROFILE：wait | dbr | pa | pacer
#   慢节点、RTT、带宽由 apply_netem.sh 用 tc 施加（ping 可见）
EPOCH_PROFILE="${EPOCH_PROFILE:-wait}"

occ_staging_val="${EPOCH_ENABLE_OCC_STAGING:-off}"
epoch_dyn_net_catchup_val="off"
epoch_dyn_net_proxy_val="off"
epoch_dyn_net_slow_nodes_val="''"
if [ -n "${EPOCH_DYN_NET_SLOW_NODES:-}" ]; then
    epoch_dyn_net_slow_nodes_val="'${EPOCH_DYN_NET_SLOW_NODES}'"
fi

set_conf enable_occ_staging "${occ_staging_val}"
set_conf enable_occ_disk "on"
set_conf enable_epoch_multimaster "on"

commit_threads_val="10"
merge_threads_val="5"
tip_flush_threads_val="1"
synchronous_commit_val="off"

# 四个 profile 共用同一套节点参数，只切换 Pacer 的两个开关
case "${EPOCH_PROFILE}" in
    wait) ;;
    dbr)   epoch_dyn_net_catchup_val="on" ;;
    pa)    epoch_dyn_net_proxy_val="on" ;;
    pacer) epoch_dyn_net_catchup_val="on"; epoch_dyn_net_proxy_val="on" ;;
    *)
        echo "WARN: unknown EPOCH_PROFILE=${EPOCH_PROFILE}, using wait" >&2
        EPOCH_PROFILE=wait
        ;;
esac

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

# RTT / 带宽由 apply_netem.sh 用 tc 施加；这里只把 RTT 与 K 写给 DBR 的目标 epoch 公式
if ! [[ "${EPOCH_NETWORK_RTT_MS:-100}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: EPOCH_NETWORK_RTT_MS must be an integer, got: ${EPOCH_NETWORK_RTT_MS}" >&2
    exit 1
fi
set_conf epoch_dyn_net_catchup "${epoch_dyn_net_catchup_val}"
set_conf epoch_dyn_net_proxy "${epoch_dyn_net_proxy_val}"
set_conf epoch_dyn_net_slow_nodes "${epoch_dyn_net_slow_nodes_val}"
set_conf epoch_dyn_net_rtt_ms "${EPOCH_NETWORK_RTT_MS:-100}"
set_conf epoch_dyn_net_catchup_k "${EPOCH_DYN_NET_CATCHUP_K:-3}"

# 慢节点把 tc 的带宽上限同时告诉 PM，DBR/PA 才能按利用率判定拥塞
if epoch_node_is_slow "${NODE_ID}"; then
    case "${EPOCH_DYN_NET_BW_MODE:-uplimit}" in
        uplimit)
            set_conf epoch_dyn_net_upload_bps "${EPOCH_DYN_NET_UPLOAD_BPS}"
            set_conf epoch_dyn_net_download_bps "0"
            ;;
        downlimit)
            set_conf epoch_dyn_net_upload_bps "0"
            set_conf epoch_dyn_net_download_bps "${EPOCH_DYN_NET_DOWNLOAD_BPS}"
            ;;
        bilimit)
            set_conf epoch_dyn_net_upload_bps "${EPOCH_DYN_NET_UPLOAD_BPS}"
            set_conf epoch_dyn_net_download_bps "${EPOCH_DYN_NET_DOWNLOAD_BPS}"
            ;;
        *)
            set_conf epoch_dyn_net_upload_bps "0"
            set_conf epoch_dyn_net_download_bps "0"
            ;;
    esac
else
    set_conf epoch_dyn_net_upload_bps "0"
    set_conf epoch_dyn_net_download_bps "0"
fi

echo "epoch net: profile=${EPOCH_PROFILE} catchup=${epoch_dyn_net_catchup_val} proxy=${epoch_dyn_net_proxy_val} slow=${epoch_dyn_net_slow_nodes_val} rtt_ms=${EPOCH_NETWORK_RTT_MS:-100}/${EPOCH_RTT_HEALTHY_MS:-35} (tc: apply_netem.sh, bw=${EPOCH_DYN_NET_BW_MODE:-uplimit} ${EPOCH_DYN_NET_BW_MBPS:-10}Mbps)"

set_conf synchronous_commit "${synchronous_commit_val}"

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
lifecycle_log_val="on"
if [ -n "${EPOCH_RECV_DIAG_VERBOSE:-}" ]; then
    recv_diag_verbose_val="${EPOCH_RECV_DIAG_VERBOSE}"
fi
if [ -n "${EPOCH_LIFECYCLE_LOG:-}" ]; then
    lifecycle_log_val="${EPOCH_LIFECYCLE_LOG}"
fi
set_conf epoch_recv_diag_verbose "${recv_diag_verbose_val}"
set_conf epoch_lifecycle_log "${lifecycle_log_val}"

set_conf epoch_node_id "$(epoch_guc_node_id "${NODE_ID}")"
set_conf epoch_total_nodes "$(epoch_effective_total_nodes)"
set_conf epoch_peer_hosts "'$(epoch_effective_peer_hosts)'"
set_conf epoch_data_port_base "${EPOCH_ZMQ_PORT_BASE}"
set_conf epoch_delay_slots "0"
set_conf epoch_cache_slots "8192"
set_conf epoch_merge_threads "${merge_threads_val}"
set_conf epoch_commit_threads "${commit_threads_val}"
set_conf epoch_tip_flush_threads "${tip_flush_threads_val}"
# 发送必须串行：单打包线程 = 单消费者，收尾包一定排在队尾，
# “取到收尾包时已发多少条”即声明条数，不产生任何并发分叉。
set_conf epoch_pack_threads "1"
set_conf epoch_txn_queue_capacity "8192"
set_conf epoch_abort_csn_per_epoch "1024"
set_conf epoch_crdt_slot_count "32"
set_conf epoch_crdt_rows_per_slot "8192"
set_conf epoch_crdt_inserts_per_slot "2048"
set_conf epoch_merge_queue_capacity "8192"
set_conf epoch_merge_done_capacity "8192"
set_conf epoch_apply_queue_capacity "81920"
set_conf epoch_apply_done_capacity "8192"
echo "epoch: pipeline coordinator + apply pool（epoch_commit_threads=${commit_threads_val}，多节点时启动 postmaster apply worker 进程）"
echo "epoch: merge pool（epoch_merge_threads=${merge_threads_val}，多节点时启动 postmaster merge worker 进程）"
echo "epoch: tip flush（epoch_tip_flush_threads=${tip_flush_threads_val}，tip staging 后台写回 heap）"
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

echo "configured ${DATA_DIR} ($(epoch_node_display_name "${NODE_ID}") id=${NODE_ID}, ip ${NODE_IP}, port ${EPOCH_DB_PORT}, profile=${EPOCH_PROFILE})"
echo "edited: ${CONF}"
verify_conf epoch_commit_threads "${commit_threads_val}"
verify_conf epoch_merge_threads "${merge_threads_val}"
verify_conf epoch_tip_flush_threads "${tip_flush_threads_val}"
verify_conf enable_occ_staging "${occ_staging_val}"
verify_conf synchronous_commit "${synchronous_commit_val}"
verify_conf default_transaction_isolation "repeatable read"
echo "verified: commit_threads=${commit_threads_val}, merge_threads=${merge_threads_val}, tip_flush_threads=${tip_flush_threads_val}"
echo "verified: synchronous_commit=${synchronous_commit_val}, default_transaction_isolation=repeatable read"
echo "verified: enable_occ_staging=${occ_staging_val}"
echo "epoch: profile=${EPOCH_PROFILE} sync_commit=${synchronous_commit_val} catchup=${epoch_dyn_net_catchup_val} proxy=${epoch_dyn_net_proxy_val}"
echo "epoch: tc RTT=${EPOCH_NETWORK_RTT_MS:-100}ms bw=${EPOCH_DYN_NET_BW_MODE:-uplimit} ${EPOCH_DYN_NET_BW_MBPS:-100}Mbps slow=${epoch_dyn_net_slow_nodes_val}"
verify_conf epoch_recv_diag "on"
verify_conf epoch_apply_profile "on"
verify_conf epoch_dyn_net_catchup "${epoch_dyn_net_catchup_val}"
verify_conf epoch_dyn_net_proxy "${epoch_dyn_net_proxy_val}"
if [ -z "${EPOCH_CONFIGURE_REMOTE:-}" ]; then
    epoch_apply_netem_from_profile
fi
echo "IMPORTANT: epoch GUCs are PGC_POSTMASTER — gs_ctl restart required (reload is NOT enough)."
