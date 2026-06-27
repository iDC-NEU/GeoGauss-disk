#ifndef OPENGAUSS_E_CONFIG_H
#define OPENGAUSS_E_CONFIG_H

#include <cstdint>

extern bool g_enable_epoch_multimaster;
extern bool g_epoch_commit_sync;
extern bool g_epoch_recv_diag;
extern bool g_epoch_diag;
extern bool g_epoch_perf_log;
extern int g_epoch_node_id;
extern int g_epoch_total_nodes;
extern int g_epoch_cache_slots;
extern int g_epoch_physical_interval_ms;
extern int g_epoch_delay_slots;
extern int g_epoch_phys_logical_gap_limit;
extern int g_epoch_merge_threads;
extern int g_epoch_commit_threads;
extern int g_epoch_pack_threads;
extern int g_epoch_async_stale_pack_limit;
extern int g_epoch_async_stagnation_ms;
extern int g_epoch_async_assign_max_sleep_ms;
extern int g_epoch_async_max_tpmc;
extern int g_epoch_async_tpmc_write_ratio_percent;

extern char* g_epoch_peer_hosts;
extern int g_epoch_data_port_base;
extern char* g_epoch_apply_database;
extern char* g_epoch_apply_user;

/* heterogeneous network (dynamic-net) */
extern bool g_enable_epoch_dyn_net;
extern bool g_epoch_dyn_net_catchup;
extern bool g_epoch_dyn_net_proxy;
extern int g_epoch_dyn_net_lag_tolerance;
extern int g_epoch_dyn_net_queue_tolerance;
extern int g_epoch_dyn_net_catchup_k;
extern int g_epoch_dyn_net_rtt_ms;
extern char* g_epoch_dyn_net_slow_nodes;
extern double g_epoch_dyn_net_upload_bps;
extern double g_epoch_dyn_net_download_bps;

enum EpochDynNetBwMode {
    EPOCH_DYN_NET_BW_UNLIMIT = 0,
    EPOCH_DYN_NET_BW_UP = 1,
    EPOCH_DYN_NET_BW_DOWN = 2,
    EPOCH_DYN_NET_BW_BI = 3
};
extern int g_epoch_dyn_net_bw_mode;

extern void EpochConfigInit(void);
extern uint32_t EpochGetCacheSlots(void);
extern uint32_t EpochGetTotalNodes(void);
extern uint32_t EpochGetNodeId(void);
extern uint64_t EpochGetPhysLogicalGapLimit(void);
extern uint64_t EpochGetAsyncStalePackLimit(void);
extern uint32_t EpochGetAsyncStagnationMs(void);
extern uint32_t EpochGetAsyncAssignMaxSleepMs(void);
extern uint32_t EpochGetAsyncMaxTpmc(void);
extern uint32_t EpochGetAsyncTpmcWriteRatioPercent(void);

#endif /* OPENGAUSS_E_CONFIG_H */
