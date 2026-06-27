#ifndef OPENGAUSS_E_SHMEM_H
#define OPENGAUSS_E_SHMEM_H

#include <atomic>
#include <cstdint>

#include "epoch/e_types.h"
#include "epoch/e_abort_csn.h"

/*
 * 共享内存中的 epoch 计数器布局。
 * 采用扁平化 (slot, node) 索引，便于无锁原子更新。
 */
struct EpochCounterBank {
    /* 槽位当前绑定的 epoch 号，防止 logical 落后时同槽被更高 epoch 覆盖 */
    std::atomic<uint64_t> slot_epoch[EPOCH_DEFAULT_CACHE_SLOTS];
    /* GeoGauss LoadChangeSet：OCC 写事务 COMMIT pack 前 +1（与 pack 同步），用于 should_send */
    std::atomic<uint64_t> change_set_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* 本 epoch 内正在执行 SQL、尚未完成读写集采集的事务数（首次 OCC 访问 +1，pack/只读 commit/abort -1） */
    std::atomic<uint64_t> executing_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* GeoGauss local_txn_counters：进入 commit 路径 */
    std::atomic<uint64_t> local_produce_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* GeoGauss pack_txn_num */
    std::atomic<uint64_t> local_pack_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* 上次 epoch-end 广播的 pack 数；与 local_pack 不等时需重 seal */
    std::atomic<uint64_t> sealed_pack_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* GeoGauss record_commit_txn_counters */
    std::atomic<uint64_t> local_record_commit_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* GeoGauss record_committed_txn_counters */
    std::atomic<uint64_t> local_record_committed_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* GeoGauss total_abort_txn_num（简化：abort 后 +1，用于 seal 对账） */
    std::atomic<uint64_t> abort_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* GeoGauss read_committed_txn_num：只读事务 commit 完成 */
    std::atomic<uint64_t> read_committed_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* GeoGauss write_abort_after_send_txn_num：已 pack 后因 merge/abort 退出 */
    std::atomic<uint64_t> write_abort_after_send_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    std::atomic<uint64_t> store_should_receive_pack[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    std::atomic<uint64_t> store_received_pack[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* GeoGauss remote_merged_txn_counters */
    std::atomic<uint64_t> merged_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* GeoGauss local_txn_committed_counters */
    std::atomic<uint64_t> committed_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    std::atomic<bool> node_send_completed[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    std::atomic<bool> node_receive_completed[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
};

struct EpochSharedState {
    /* postmaster 启动时写入，全进程共用同一组带时间戳的 epoch 日志文件名 */
    char log_file_stamp[17];
    std::atomic<uint64_t> physical_epoch;
    std::atomic<uint64_t> logical_epoch;
    std::atomic<bool> record_committed;
    std::atomic<bool> remote_record_committed;
    /* GeoGauss remote_execed：epoch worker 进入 merge/exec 阶段 */
    std::atomic<bool> remote_execed;
    std::atomic<bool> epoch_advance_started;
    /*
     * async cap epoch 硬 pack 配额：async_cap_pack_reserved 为 cap 槽 in-flight pack 数；
     * async_cap_pack_blocked_ms 记录本 cap 槽 pack 数触顶后的起始时刻（stagnation 泄洪）；
     * drip_tokens / drip_refill_ms 在 backlog 超限时按 stagnation 周期滴漏放行，避免永久关断或全开。
     */
    std::atomic<uint64_t> async_cap_pack_epoch;
    std::atomic<uint64_t> async_cap_pack_reserved;
    std::atomic<uint64_t> async_cap_pack_blocked_ms;
    std::atomic<uint64_t> async_cap_pack_drip_tokens;
    std::atomic<uint64_t> async_cap_pack_drip_refill_ms;
    /* async COMMIT 限速（各 backend 共享 token bucket，估算 tpmC 上限） */
    std::atomic<uint64_t> async_commit_rate_tokens;
    std::atomic<uint64_t> async_commit_rate_last_ms;
    EpochCounterBank counters;
    EpochAbortCsnBank abort_csns;
    /* TPCC/RTT 分析：跨 backend + worker 累计的性能计数（见 e_perf.cpp） */
    struct EpochPerfCounters {
        std::atomic<uint64_t> gate_wait_us[EPOCH_PERF_GATE_COUNT];
        std::atomic<uint64_t> gate_wait_count[EPOCH_PERF_GATE_COUNT];
        std::atomic<uint64_t> gate_spin_count[EPOCH_PERF_GATE_COUNT];
        std::atomic<uint64_t> zmq_wire_txn_send_count;
        std::atomic<uint64_t> zmq_wire_txn_send_bytes;
        std::atomic<uint64_t> zmq_wire_row_bytes;
        std::atomic<uint64_t> zmq_wire_row_count;
        std::atomic<uint64_t> zmq_epoch_end_send_count;
        std::atomic<uint64_t> zmq_epoch_end_send_bytes;
        std::atomic<uint64_t> zmq_hello_send_count;
        std::atomic<uint64_t> zmq_txn_req_send_count;
        std::atomic<uint64_t> zmq_wire_recv_count;
        std::atomic<uint64_t> zmq_wire_recv_bytes;
        std::atomic<uint64_t> zmq_epoch_end_recv_count;
        std::atomic<uint64_t> logical_advance_ok;
        std::atomic<uint64_t> logical_advance_try;
        std::atomic<uint64_t> stall_peer_not_connected;
        std::atomic<uint64_t> stall_peer_lag;
        std::atomic<uint64_t> stall_physical_not_ready;
        std::atomic<uint64_t> stall_recv_incomplete;
        std::atomic<uint64_t> stall_merge_incomplete;
        std::atomic<uint64_t> stall_local_commit_pending;
        std::atomic<uint64_t> stall_send_incomplete;
        std::atomic<uint64_t> stall_reseal_pending;
        std::atomic<uint64_t> stall_commit_path_incomplete;
        std::atomic<uint64_t> stall_record_incomplete;
        std::atomic<uint64_t> seal_block_physical_delay;
        std::atomic<uint64_t> seal_block_peer_lag;
        std::atomic<uint64_t> seal_block_exec_pending;
        std::atomic<uint64_t> seal_block_change_set_pending;
        std::atomic<uint64_t> seal_block_epoch_active;
        std::atomic<uint64_t> seal_block_peer_txn_pending;
        std::atomic<uint64_t> seal_ok_count;
        std::atomic<uint64_t> worker_poll_count;
        std::atomic<uint64_t> physical_advance_count;
    } perf;
};

extern Size EpochShmemSize(void);
extern void EpochShmemInit(void);
extern EpochSharedState* EpochGetSharedState(void);

#endif /* OPENGAUSS_E_SHMEM_H */
