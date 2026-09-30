#include "postgres.h"

#include "epoch/e_config.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_types.h"

#include "occ/o_config.h"

#include "utils/guc.h"

#include <climits>

/*
 * Benchmark 三节点拓扑（scripts/epoch_benchmark/env.sh）：
 *   NODE_ID=1  node1  202.199.13.149
 *   NODE_ID=2  node2  202.199.13.61
 *   NODE_ID=3  node3  219.216.64.216
 * epoch_node_id GUC 为 0-based，与 peer_hosts 列表下标一致。
 * node2 关机时脚本写入 epoch_total_nodes=2，peer_hosts 仅含 node1+node3。
 */
#define EPOCH_BENCHMARK_PEER_HOSTS "202.199.13.149,202.199.13.61,219.216.64.216"
#define EPOCH_BENCHMARK_TOTAL_NODES 3

bool g_enable_epoch_multimaster = false;
/* 压测：改为 true 可跳过 worker apply（见 e_config.h） */
bool g_epoch_skip_apply = false;
/* 分析：只 apply 本地 wire；远端 MarkApplied 跳过（见 e_config.h） */
bool g_epoch_apply_local_only = false;
/* Phase 1：本地 tip 进 staging 哈希表（见 e_config.h） */
bool g_epoch_local_tip_staging = true;

bool g_epoch_sync_start_gate = true;
bool g_epoch_recv_diag = false;
bool g_epoch_recv_diag_verbose = false;
bool g_epoch_lifecycle_log = false;
bool g_epoch_diag = false;
bool g_epoch_perf_log = false;
int g_epoch_node_id = 0;
int g_epoch_total_nodes = EPOCH_BENCHMARK_TOTAL_NODES;
int g_epoch_cache_slots = EPOCH_DEFAULT_CACHE_SLOTS;
int g_epoch_physical_txns_per_epoch = 0;
int g_epoch_delay_slots = EPOCH_DEFAULT_DELAY;
int g_epoch_phys_logical_gap_limit = 5;
int g_epoch_merge_threads = 2;
int g_epoch_commit_threads = 2;
int g_epoch_tip_flush_threads = 1;
int g_epoch_pack_threads = 5;
int g_epoch_apply_lock_trace_ms = 0;
int g_epoch_txn_queue_capacity = EPOCH_DEFAULT_TXN_QUEUE_CAPACITY;
int g_epoch_abort_csn_per_epoch = EPOCH_DEFAULT_ABORT_CSN_PER_EPOCH;
int g_epoch_crdt_slot_count = EPOCH_DEFAULT_CRDT_SLOT_COUNT;
int g_epoch_crdt_rows_per_slot = EPOCH_DEFAULT_CRDT_ROWS_PER_SLOT;
int g_epoch_crdt_inserts_per_slot = EPOCH_DEFAULT_CRDT_INSERTS_PER_SLOT;
int g_epoch_merge_queue_capacity = EPOCH_DEFAULT_MERGE_QUEUE_CAPACITY;
int g_epoch_merge_done_capacity = EPOCH_DEFAULT_MERGE_DONE_CAPACITY;
int g_epoch_apply_queue_capacity = EPOCH_DEFAULT_APPLY_QUEUE_CAPACITY;
int g_epoch_apply_done_capacity = EPOCH_DEFAULT_APPLY_DONE_CAPACITY;
char* g_epoch_peer_hosts = NULL;
int g_epoch_data_port_base = 25600;
char* g_epoch_apply_database = NULL;
char* g_epoch_apply_user = NULL;

bool g_enable_epoch_dyn_net = false;
bool g_epoch_dyn_net_catchup = false;
bool g_epoch_dyn_net_proxy = false;
int g_epoch_dyn_net_lag_tolerance = 5;
int g_epoch_dyn_net_queue_tolerance = 20;
int g_epoch_dyn_net_catchup_k = 3;
int g_epoch_dyn_net_rtt_ms = 200;
char* g_epoch_dyn_net_slow_nodes = NULL;
double g_epoch_dyn_net_upload_bps = 0;
double g_epoch_dyn_net_download_bps = 0;
int g_epoch_dyn_net_bw_mode = EPOCH_DYN_NET_BW_UNLIMIT;

static bool EpochCheckMultimaster(bool* newval, void** extra, GucSource source)
{
    if (*newval && !g_enable_occ_disk) {
        GUC_check_errdetail("enable_epoch_multimaster requires enable_occ_disk=on.");
        return false;
    }
    return true;
}

static void EpochAssignMultimaster(bool newval, void* extra)
{
    if (newval) {
        g_enable_occ_disk = true;
    }
}

static const struct config_enum_entry epoch_dyn_net_bw_mode_options[] = {
    {"unlimit", EPOCH_DYN_NET_BW_UNLIMIT, false},
    {"uplimit", EPOCH_DYN_NET_BW_UP, false},
    {"downlimit", EPOCH_DYN_NET_BW_DOWN, false},
    {"bilimit", EPOCH_DYN_NET_BW_BI, false},
    {NULL, 0, false}
};

uint32_t EpochGetCacheSlots(void)
{
    if (g_epoch_cache_slots <= 0) {
        return EPOCH_DEFAULT_CACHE_SLOTS;
    }
    return static_cast<uint32_t>(g_epoch_cache_slots);
}

uint32_t EpochGetTotalNodes(void)
{
    if (g_epoch_total_nodes <= 0) {
        return 2;
    }
    if (g_epoch_total_nodes > static_cast<int>(EPOCH_MAX_NODES)) {
        return EPOCH_MAX_NODES;
    }
    return static_cast<uint32_t>(g_epoch_total_nodes);
}

uint32_t EpochGetNodeId(void)
{
    if (g_epoch_node_id < 0) {
        return 0;
    }
    if (g_epoch_node_id >= static_cast<int>(EPOCH_MAX_NODES)) {
        return EPOCH_MAX_NODE_ID;
    }
    return static_cast<uint32_t>(g_epoch_node_id);
}

uint64_t EpochGetPhysLogicalGapLimit(void)
{
    if (g_epoch_phys_logical_gap_limit <= 0) {
        return UINT64_MAX;
    }
    uint64_t limit = static_cast<uint64_t>(g_epoch_phys_logical_gap_limit);
    if (limit < 2) {
        limit = 2;
    }
    uint32_t delay = static_cast<uint32_t>(g_epoch_delay_slots);
    if (delay == 0) {
        delay = EPOCH_DEFAULT_DELAY;
    }
    /* logical 推进要求 physical > logical + delay */
    if (limit < static_cast<uint64_t>(delay) + 1) {
        limit = static_cast<uint64_t>(delay) + 1;
    }
    return limit;
}

bool EpochIsPhysLogicalGapDisabled(void)
{
    return g_epoch_phys_logical_gap_limit <= 0;
}

void EpochConfigInit(void)
{
    /* 占位：后续从配置文件或 catalog 加载节点拓扑 */
}

void EpochRegisterGucs(void)
{
    DefineCustomBoolVariable("enable_epoch_multimaster",
                             "Enable epoch-based CRDT multi-master on disk tables.",
                             NULL,
                             &g_enable_epoch_multimaster,
                             false,
                             PGC_POSTMASTER,
                             GUC_NO_RESET_ALL,
                             EpochCheckMultimaster,
                             EpochAssignMultimaster,
                             NULL);

    DefineCustomBoolVariable("epoch_sync_start_gate",
                             "When on (default), sync multi-master blocks BEGIN on START gate "
                             "until logical epoch catches up; extended-protocol Parse (prepare) "
                             "skips assign/gate/executing. When off, skip START gate for all paths.",
                             NULL,
                             &g_epoch_sync_start_gate,
                             true,
                             PGC_POSTMASTER,
                             GUC_NO_RESET_ALL,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomStringVariable("epoch_peer_hosts",
                               "Comma-separated peer host list aligned with node id.",
                               NULL,
                               &g_epoch_peer_hosts,
                               EPOCH_BENCHMARK_PEER_HOSTS,
                               PGC_POSTMASTER,
                               GUC_NO_RESET_ALL,
                               NULL,
                               NULL,
                               NULL);

    DefineCustomIntVariable("epoch_data_port_base",
                            "Base TCP port for epoch ZMQ data plane (port = base + node_id).",
                            NULL,
                            &g_epoch_data_port_base,
                            25600,
                            1024,
                            65500,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_node_id",
                            "Local node id in epoch multi-master cluster (0-based).",
                            NULL,
                            &g_epoch_node_id,
                            0,
                            0,
                            static_cast<int>(EPOCH_MAX_NODE_ID),
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_total_nodes",
                            "Total node count in epoch multi-master cluster.",
                            NULL,
                            &g_epoch_total_nodes,
                            EPOCH_BENCHMARK_TOTAL_NODES,
                            1,
                            static_cast<int>(EPOCH_MAX_NODES),
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_cache_slots",
                            "Ring buffer slot count for epoch counters.",
                            NULL,
                            &g_epoch_cache_slots,
                            EPOCH_DEFAULT_CACHE_SLOTS,
                            8,
                            8192,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_physical_txns_per_epoch",
                            "When >0, each postmaster allows at most this many backend assigns "
                            "before advancing its local physical epoch.",
                            NULL,
                            &g_epoch_physical_txns_per_epoch,
                            0,
                            0,
                            1000000,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_delay_slots",
                            "Logical epoch delay behind physical epoch.",
                            NULL,
                            &g_epoch_delay_slots,
                            EPOCH_DEFAULT_DELAY,
                            0,
                            32,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_phys_logical_gap_limit",
                            "Max slots physical epoch may lead logical epoch (backpressure). "
                            "0 disables physical backpressure.",
                            NULL,
                            &g_epoch_phys_logical_gap_limit,
                            5,
                            0,
                            8192,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_merge_threads",
                            "Worker thread count for epoch merge stage.",
                            NULL,
                            &g_epoch_merge_threads,
                            2,
                            1,
                            64,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_commit_threads",
                            "Worker thread count for epoch commit stage.",
                            NULL,
                            &g_epoch_commit_threads,
                            2,
                            1,
                            64,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_tip_flush_threads",
                            "Epoch tip-staging flush worker count (background reclaim to heap).",
                            NULL,
                            &g_epoch_tip_flush_threads,
                            1,
                            1,
                            16,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_pack_threads",
                            "Pack worker count (TxnQueue drain → merge enqueue + send queue; Pack#0 seals).",
                            NULL,
                            &g_epoch_pack_threads,
                            5,
                            1,
                            64,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                             NULL,
                             NULL);

    DefineCustomIntVariable("epoch_txn_queue_capacity",
                            "Shared-memory txn wire queue slot count.",
                            NULL,
                            &g_epoch_txn_queue_capacity,
                            EPOCH_DEFAULT_TXN_QUEUE_CAPACITY,
                            256,
                            1048576,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_abort_csn_per_epoch",
                            "Max aborted txn CSNs recorded per epoch cache slot.",
                            NULL,
                            &g_epoch_abort_csn_per_epoch,
                            EPOCH_DEFAULT_ABORT_CSN_PER_EPOCH,
                            1024,
                            1048576,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_crdt_slot_count",
                            "CRDT ring buffer slot count (epoch modulo).",
                            NULL,
                            &g_epoch_crdt_slot_count,
                            EPOCH_DEFAULT_CRDT_SLOT_COUNT,
                            4,
                            8192,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_crdt_rows_per_slot",
                            "Max merged row entries per CRDT slot.",
                            NULL,
                            &g_epoch_crdt_rows_per_slot,
                            EPOCH_DEFAULT_CRDT_ROWS_PER_SLOT,
                            256,
                            1048576,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_crdt_inserts_per_slot",
                            "Max insert-tracker entries per CRDT slot.",
                            NULL,
                            &g_epoch_crdt_inserts_per_slot,
                            EPOCH_DEFAULT_CRDT_INSERTS_PER_SLOT,
                            64,
                            1048576,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_merge_queue_capacity",
                            "Async merge task queue slot count.",
                            NULL,
                            &g_epoch_merge_queue_capacity,
                            EPOCH_DEFAULT_MERGE_QUEUE_CAPACITY,
                            64,
                            65536,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_merge_done_capacity",
                            "Async merge completion ring buffer size.",
                            NULL,
                            &g_epoch_merge_done_capacity,
                            EPOCH_DEFAULT_MERGE_DONE_CAPACITY,
                            64,
                            65536,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_apply_queue_capacity",
                            "Async apply task queue slot count.",
                            NULL,
                            &g_epoch_apply_queue_capacity,
                            EPOCH_DEFAULT_APPLY_QUEUE_CAPACITY,
                            64,
                            262144,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_apply_done_capacity",
                            "Async apply completion ring buffer size.",
                            NULL,
                            &g_epoch_apply_done_capacity,
                            EPOCH_DEFAULT_APPLY_DONE_CAPACITY,
                            64,
                            65536,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomBoolVariable("epoch_recv_diag",
                             "When on, write epoch_recv_diag_YYYYMMDD_HHMMSS.log for anomalies only. "
                             "Use epoch_recv_diag_verbose=on for full wire/backend tracing.",
                             NULL,
                             &g_epoch_recv_diag,
                             false,
                             PGC_POSTMASTER,
                             GUC_NO_RESET_ALL,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("epoch_recv_diag_verbose",
                             "When on with epoch_recv_diag, log per-txn/per-epoch verbose recv_diag (large files).",
                             NULL,
                             &g_epoch_recv_diag_verbose,
                             false,
                             PGC_POSTMASTER,
                             GUC_NO_RESET_ALL,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("epoch_lifecycle_log",
                             "When on, append epoch_lifecycle_YYYYMMDD_HHMMSS.log with per-epoch stage timings.",
                             NULL,
                             &g_epoch_lifecycle_log,
                             false,
                             PGC_POSTMASTER,
                             GUC_NO_RESET_ALL,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("epoch_diag",
                             "When on, periodically append epoch_diag_YYYYMMDD_HHMMSS.log with per-epoch counters.",
                             NULL,
                             &g_epoch_diag,
                             false,
                             PGC_POSTMASTER,
                             GUC_NO_RESET_ALL,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("epoch_perf_log",
                             "When on, accumulate gate/ZMQ/logical stall metrics and append epoch_perf_YYYYMMDD_HHMMSS.log.",
                             NULL,
                             &g_epoch_perf_log,
                             false,
                             PGC_POSTMASTER,
                             GUC_NO_RESET_ALL,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomIntVariable("epoch_apply_lock_trace_ms",
                            "Log APPLY_BUFLOCK_DETAIL/TXN when apply row lock/find IO exceeds this many ms; 0 disables.",
                            "Splits excl_wait/hold, share_wait, ReadBuffer (rb_*), WaitIO, btree LockBuffer (bt_lock_*).",
                            &g_epoch_apply_lock_trace_ms,
                            0,
                            0,
                            60000,
                            PGC_SIGHUP,
                            GUC_UNIT_MS,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomBoolVariable("epoch_apply_profile",
                             "When on, append epoch_apply_profile_*.log with per-phase apply timings.",
                             NULL,
                             &g_epoch_apply_profile,
                             false,
                             PGC_SIGHUP,
                             GUC_NO_RESET_ALL,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("epoch_merge_profile",
                             "When on, append epoch_merge_profile_*.log with per-phase merge worker timings.",
                             NULL,
                             &g_epoch_merge_profile,
                             false,
                             PGC_SIGHUP,
                             GUC_NO_RESET_ALL,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomStringVariable("epoch_apply_database",
                               "Database name for epoch worker remote apply.",
                               NULL,
                               &g_epoch_apply_database,
                               "test",
                               PGC_POSTMASTER,
                               GUC_NO_RESET_ALL,
                               NULL,
                               NULL,
                               NULL);

    DefineCustomStringVariable("epoch_apply_user",
                               "User name for epoch worker remote apply.",
                               NULL,
                               &g_epoch_apply_user,
                               "hcj2003",
                               PGC_POSTMASTER,
                               GUC_NO_RESET_ALL,
                               NULL,
                               NULL,
                               NULL);

    DefineCustomBoolVariable("enable_epoch_dyn_net",
                             "Enable heterogeneous network shaping (dynamic-net) on epoch wire plane.",
                             NULL,
                             &g_enable_epoch_dyn_net,
                             false,
                             PGC_POSTMASTER,
                             GUC_NO_RESET_ALL,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("epoch_dyn_net_catchup",
                             "Enable Pacer DBR: move unsent local write sets forward and close skipped epochs with EOF.",
                             NULL,
                             &g_epoch_dyn_net_catchup,
                             false,
                             PGC_POSTMASTER,
                             GUC_NO_RESET_ALL,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("epoch_dyn_net_proxy",
                             "Enable Pacer PA: slow node selects a proxy; healthy nodes route its copy there.",
                             NULL,
                             &g_epoch_dyn_net_proxy,
                             false,
                             PGC_POSTMASTER,
                             GUC_NO_RESET_ALL,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomIntVariable("epoch_dyn_net_lag_tolerance",
                            "CATCHUP trigger: projected_max - local_epoch threshold.",
                            NULL,
                            &g_epoch_dyn_net_lag_tolerance,
                            5,
                            1,
                            1000,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_dyn_net_queue_tolerance",
                            "CATCHUP trigger: outgoing queue size threshold.",
                            NULL,
                            &g_epoch_dyn_net_queue_tolerance,
                            20,
                            1,
                            100000,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_dyn_net_catchup_k",
                            "Extra epoch slots to jump ahead on CATCHUP.",
                            NULL,
                            &g_epoch_dyn_net_catchup_k,
                            3,
                            0,
                            1000,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_dyn_net_rtt_ms",
                            "Estimated RTT (ms) for epoch observer compensation.",
                            NULL,
                            &g_epoch_dyn_net_rtt_ms,
                            200,
                            1,
                            60000,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomStringVariable("epoch_dyn_net_slow_nodes",
                               "Comma-separated slow node ids (e.g. 0).",
                               NULL,
                               &g_epoch_dyn_net_slow_nodes,
                               "",
                               PGC_POSTMASTER,
                               GUC_NO_RESET_ALL,
                               NULL,
                               NULL,
                               NULL);

    DefineCustomRealVariable("epoch_dyn_net_upload_bps",
                               "Slow node upload bandwidth limit (bits/sec, 0=unlimited).",
                               NULL,
                               &g_epoch_dyn_net_upload_bps,
                               0,
                               0,
                               1e12,
                               PGC_POSTMASTER,
                               GUC_NO_RESET_ALL,
                               NULL,
                               NULL,
                               NULL);

    DefineCustomRealVariable("epoch_dyn_net_download_bps",
                               "Slow node download bandwidth limit (bits/sec, 0=unlimited).",
                               NULL,
                               &g_epoch_dyn_net_download_bps,
                               0,
                               0,
                               1e12,
                               PGC_POSTMASTER,
                               GUC_NO_RESET_ALL,
                               NULL,
                               NULL,
                               NULL);

    DefineCustomEnumVariable("epoch_dyn_net_bw_mode",
                             "Bandwidth limit mode for slow nodes.",
                             NULL,
                             &g_epoch_dyn_net_bw_mode,
                             EPOCH_DYN_NET_BW_UNLIMIT,
                             epoch_dyn_net_bw_mode_options,
                             PGC_POSTMASTER,
                             GUC_NO_RESET_ALL,
                             NULL,
                             NULL,
                             NULL);
}
