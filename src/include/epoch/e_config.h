#ifndef OPENGAUSS_E_CONFIG_H
#define OPENGAUSS_E_CONFIG_H

#include <cstdint>

extern bool g_enable_epoch_multimaster;
/*
 * 压测开关（非 GUC）：true 时跳过 heap apply，仅推进 committed 计数以便 logical 关窗。
 * 修改 e_config.cpp 中初值后全量编译重启；数据不会落盘，仅用于测 apply 开销。
 */
extern bool g_epoch_skip_apply;
/*
 * 分析开关（非 GUC）：true 时 apply 只落本地 wire（server_id==本机），远端直接
 * MarkApplied 跳过，保证 logical 仍可推进。改初值后重编重启。
 */
extern bool g_epoch_apply_local_only;
/*
 * Phase 1：本地 tip staging（不用磁盘版本链写 UPDATE/DELETE）。
 * apply 与 backend WriteGlobalChanges（WR/DEL）共用 EpochTipStaging；
 * INSERT 仍落堆。不打开 enable_occ_staging。改初值后全量编译重启。
 */
extern bool g_epoch_local_tip_staging;
extern bool g_epoch_sync_start_gate;
extern bool g_epoch_recv_diag;
extern bool g_epoch_recv_diag_verbose;
extern bool g_epoch_lifecycle_log;
extern bool g_epoch_diag;
extern bool g_epoch_perf_log;
extern bool g_epoch_apply_profile;
extern bool g_epoch_merge_profile;
extern int g_epoch_node_id;
extern int g_epoch_total_nodes;
extern int g_epoch_cache_slots;
extern int g_epoch_physical_txns_per_epoch;
/* 客户端终端数（TPCC terminals）：时间片离开计数达到它才塞收尾包 */
extern int g_epoch_terminals;
extern int g_epoch_delay_slots;
extern int g_epoch_merge_threads;
extern int g_epoch_commit_threads;
extern int g_epoch_tip_flush_threads;
extern int g_epoch_send_threads;
/* apply 单步耗时超过该阈值（毫秒）时写 APPLY_LOCK_TRACE 到 pg_log；0=关闭 */
extern int g_epoch_apply_lock_trace_ms;

/* 共享内存容量（postmaster 启动时分配，运行中不可变） */
extern int g_epoch_txn_queue_capacity;
extern int g_epoch_abort_csn_per_epoch;
extern int g_epoch_crdt_slot_count;
extern int g_epoch_crdt_rows_per_slot;
extern int g_epoch_crdt_inserts_per_slot;
extern int g_epoch_merge_queue_capacity;
extern int g_epoch_merge_done_capacity;
extern int g_epoch_apply_queue_capacity;
extern int g_epoch_apply_done_capacity;

extern char* g_epoch_peer_hosts;
extern int g_epoch_data_port_base;
extern char* g_epoch_apply_database;
extern char* g_epoch_apply_user;

/* Pacer */
extern bool g_epoch_dyn_net_catchup;
extern bool g_epoch_dyn_net_proxy;
extern int g_epoch_dyn_net_lag_tolerance;
extern int g_epoch_dyn_net_queue_tolerance;
extern int g_epoch_dyn_net_catchup_k;
extern int g_epoch_dyn_net_rtt_ms;

/* DBR 触发阈值：上行速率达到配置带宽的百分之多少即判为拥塞 */
extern int g_epoch_dbr_threshold_pct;

/* DBR：每个片最多直发的写集条数，其余整笔改期 */
extern int g_epoch_dbr_admit_per_epoch;
extern char* g_epoch_dyn_net_slow_nodes;
extern double g_epoch_dyn_net_upload_bps;
extern double g_epoch_dyn_net_download_bps;

extern void EpochConfigInit(void);
extern uint32_t EpochGetCacheSlots(void);
extern uint32_t EpochGetTotalNodes(void);
/* 终端总数（至少 1） */
extern uint32_t EpochGetTerminals(void);
extern uint32_t EpochGetNodeId(void);

#endif /* OPENGAUSS_E_CONFIG_H */
