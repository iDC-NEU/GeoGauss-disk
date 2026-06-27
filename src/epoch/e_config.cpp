#include "postgres.h"

#include "epoch/e_config.h"
#include "epoch/e_types.h"

#include "occ/o_config.h"

#include "utils/guc.h"

#include <climits>

bool g_enable_epoch_multimaster = false;
bool g_epoch_commit_sync = true;
bool g_epoch_recv_diag = false;
bool g_epoch_diag = false;
bool g_epoch_perf_log = false;
int g_epoch_node_id = 0;
int g_epoch_total_nodes = 2;
int g_epoch_cache_slots = EPOCH_DEFAULT_CACHE_SLOTS;
int g_epoch_physical_interval_ms = 10;
int g_epoch_delay_slots = EPOCH_DEFAULT_DELAY;
int g_epoch_phys_logical_gap_limit = 5;
int g_epoch_merge_threads = 2;
int g_epoch_commit_threads = 2;
int g_epoch_pack_threads = 1;
int g_epoch_async_stale_pack_limit = 500;
int g_epoch_async_stagnation_ms = 500;
int g_epoch_async_assign_max_sleep_ms = 20;
int g_epoch_async_max_tpmc = 0;
int g_epoch_async_tpmc_write_ratio_percent = 180;
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

uint64_t EpochGetAsyncStalePackLimit(void)
{
    if (g_epoch_async_stale_pack_limit <= 0) {
        return UINT64_MAX;
    }
    return static_cast<uint64_t>(g_epoch_async_stale_pack_limit);
}

uint32_t EpochGetAsyncStagnationMs(void)
{
    if (g_epoch_async_stagnation_ms < 0) {
        return 0;
    }
    return static_cast<uint32_t>(g_epoch_async_stagnation_ms);
}

uint32_t EpochGetAsyncAssignMaxSleepMs(void)
{
    if (g_epoch_async_assign_max_sleep_ms < 0) {
        return 0;
    }
    return static_cast<uint32_t>(g_epoch_async_assign_max_sleep_ms);
}

uint32_t EpochGetAsyncMaxTpmc(void)
{
    if (g_epoch_async_max_tpmc < 0) {
        return 0;
    }
    return static_cast<uint32_t>(g_epoch_async_max_tpmc);
}

uint32_t EpochGetAsyncTpmcWriteRatioPercent(void)
{
    if (g_epoch_async_tpmc_write_ratio_percent < 100) {
        return 100;
    }
    if (g_epoch_async_tpmc_write_ratio_percent > 400) {
        return 400;
    }
    return static_cast<uint32_t>(g_epoch_async_tpmc_write_ratio_percent);
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

    DefineCustomBoolVariable("epoch_commit_sync",
                             "When on, COMMIT waits for logical epoch close and remote "
                             "exec/record (sync multi-master; strong cross-node visibility). "
                             "When off, pack at COMMIT and return immediately (async; higher tpmC).",
                             NULL,
                             &g_epoch_commit_sync,
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
                               "",
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
                            2,
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

    DefineCustomIntVariable("epoch_physical_interval_ms",
                            "Physical epoch advance interval in milliseconds.",
                            NULL,
                            &g_epoch_physical_interval_ms,
                            10,
                            1,
                            10000,
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
                            "Max slots physical epoch may lead logical epoch (backpressure).",
                            NULL,
                            &g_epoch_phys_logical_gap_limit,
                            5,
                            2,
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

    DefineCustomIntVariable("epoch_pack_threads",
                            "Worker thread count for epoch pack stage.",
                            NULL,
                            &g_epoch_pack_threads,
                            1,
                            1,
                            64,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_async_stale_pack_limit",
                            "Async: when phys-logical gap is full, hard cap on pack count "
                            "per cap epoch (commit_epoch==logical+gap); cross-backend atomic "
                            "reservation at COMMIT pack (0=disable).",
                            NULL,
                            &g_epoch_async_stale_pack_limit,
                            500,
                            0,
                            1000000,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_async_stagnation_ms",
                            "Async: after cap-epoch pack quota is saturated for this many ms, "
                            "allow a small overflow burst to avoid commit stalls.",
                            NULL,
                            &g_epoch_async_stagnation_ms,
                            500,
                            0,
                            600000,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_async_assign_max_sleep_ms",
                            "Async: max sleep per assign when stale-pack or stagnation throttle "
                            "fires (does not wait for logical close; 0=no sleep).",
                            NULL,
                            &g_epoch_async_assign_max_sleep_ms,
                            20,
                            0,
                            1000,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_async_max_tpmc",
                            "Async: per-node write COMMIT rate cap targeting BenchmarkSQL tpmC "
                            "(0=disable). Limits local pack commits via shared token bucket.",
                            NULL,
                            &g_epoch_async_max_tpmc,
                            0,
                            0,
                            1000000,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("epoch_async_tpmc_write_ratio_percent",
                            "Async: estimated write-txn / NewOrder ratio in percent for tpmC cap "
                            "(180 ≈ 56%% NewOrder in 50/50 TPCC with errors).",
                            NULL,
                            &g_epoch_async_tpmc_write_ratio_percent,
                            180,
                            100,
                            400,
                            PGC_POSTMASTER,
                            GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomBoolVariable("epoch_recv_diag",
                             "When on, write epoch_recv_diag_YYYYMMDD_HHMMSS.log (wire txn / seal / stall tracing). "
                             "Off by default for benchmark throughput.",
                             NULL,
                             &g_epoch_recv_diag,
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
                             "Enable CATCHUP epoch jump for configured slow nodes.",
                             NULL,
                             &g_epoch_dyn_net_catchup,
                             false,
                             PGC_POSTMASTER,
                             GUC_NO_RESET_ALL,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("epoch_dyn_net_proxy",
                             "Enable proxy merge path (PartialSnapshot) for slow nodes.",
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
