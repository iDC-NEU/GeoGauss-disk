#ifndef OPENGAUSS_E_SHMEM_H
#define OPENGAUSS_E_SHMEM_H

#include <atomic>
#include <cstdint>

#include "epoch/e_types.h"

/*
 * 共享内存中的 epoch 计数器布局。
 * 采用扁平化 (slot, node) 索引，便于无锁原子更新。
 */
struct EpochCounterBank {
    /* 槽位当前绑定的 epoch 号，防止 logical 落后时同槽被更高 epoch 覆盖 */
    std::atomic<uint64_t> slot_epoch[EPOCH_DEFAULT_CACHE_SLOTS];
    /* GeoGauss LoadChangeSet：OCC 写事务 pack 时与 local_pack 同步 +1，用于 should_send */
    std::atomic<uint64_t> change_set_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* 本 epoch 内正在执行 SQL、尚未完成读写集采集的事务数（首次 OCC 访问 +1，pack/只读 commit/abort -1） */
    std::atomic<uint64_t> executing_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* 本节点已 pack 待发 wire 数；epoch-end csn 亦取自本计数 */
    std::atomic<uint64_t> local_pack_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* 上次 seal 时的 pack 快照 */
    std::atomic<uint64_t> sealed_pack_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* abort 总数；与 change_set/pack 对账 */
    std::atomic<uint64_t> abort_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* GeoGauss write_abort_after_send_txn_num：已 pack 后因 merge/abort 退出 */
    std::atomic<uint64_t> write_abort_after_send_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    std::atomic<uint64_t> store_should_receive_pack[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    std::atomic<uint64_t> store_received_pack[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* GeoGauss remote_merged_txn_counters：每笔 wire txn merge 处理完成 +1（与 FFW merged_txn 无关） */
    std::atomic<uint64_t> wire_merge_done[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* FFW 写集净赢家计数（apply 对账用） */
    std::atomic<uint64_t> merged_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    /* GeoGauss local_txn_committed_counters */
    std::atomic<uint64_t> committed_txn[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    std::atomic<bool> node_send_completed[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
    std::atomic<bool> node_receive_completed[EPOCH_DEFAULT_CACHE_SLOTS][EPOCH_MAX_NODES];
};

struct EpochSharedState {
    /* postmaster 启动时写入，全进程共用同一组带时间戳的 epoch 日志文件名 */
    char log_file_stamp[17];
    /* 全节点 backend 共享的 commit CSN 分配器（避免 per-backend 撞车导致 wire_key dedup 丢 txn） */
    std::atomic<uint64_t> next_commit_csn;
    std::atomic<uint64_t> physical_epoch;
    std::atomic<uint64_t> logical_epoch;
    /* 本节点 backend assign 累计；达 epoch_physical_txns_per_epoch 后推进 local physical */
    std::atomic<uint64_t> physical_txn_since_advance;
    EpochCounterBank counters;
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
        std::atomic<uint64_t> stall_local_commit_pending;
        std::atomic<uint64_t> stall_seal_pending;
        std::atomic<uint64_t> stall_send_incomplete;
        std::atomic<uint64_t> stall_merge_incomplete;
        std::atomic<uint64_t> stall_apply_incomplete;
        std::atomic<uint64_t> seal_block_physical_delay;
        std::atomic<uint64_t> seal_block_peer_lag;
        std::atomic<uint64_t> seal_block_exec_pending;
        std::atomic<uint64_t> seal_block_change_set_pending;
        std::atomic<uint64_t> seal_block_peer_txn_pending;
        std::atomic<uint64_t> seal_ok_count;
        std::atomic<uint64_t> worker_poll_count;
        std::atomic<uint64_t> physical_advance_count;
        /* apply worker 落盘/merge 跳过后入账（sync 多主写 backend deferred COMMIT 不计） */
        std::atomic<uint64_t> apply_commit_txn;
        std::atomic<uint64_t> apply_abort_txn;
        std::atomic<uint64_t> merge_heap_abort;
    } perf;
    /*
     * nogate read_frontier 发布表：0=未钉扎，非 0=该 backend 的 pinned frontier。
     * prune 取全表非 0 最小值作为安全上界。
     */
    std::atomic<uint64_t> backend_read_frontier[EPOCH_FRONTIER_SLOTS];
};

extern Size EpochShmemSize(void);
extern void EpochShmemInit(void);
extern EpochSharedState* EpochGetSharedState(void);
extern uint64_t EpochGetNextCommitCsn(void);

#endif /* OPENGAUSS_E_SHMEM_H */
