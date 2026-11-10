#ifndef OPENGAUSS_E_SHMEM_H
#define OPENGAUSS_E_SHMEM_H

#include <atomic>
#include <cstdint>

#include "storage/spin.h"

#include "epoch/e_types.h"

/*
 * 共享内存中的 epoch 计数器布局。
 * 采用扁平化 (slot, node) 索引，便于无锁原子更新。
 */
struct EpochCounterBank {
    /* 槽位当前绑定的 epoch 号，防止 logical 落后时同槽被更高 epoch 覆盖 */
    std::atomic<uint64_t> slot_epoch[EPOCH_DEFAULT_CACHE_SLOTS];
    /*
     * 本时间片已发出的名额序号（backend 并发抢号）。
     *   取到 < 24   → 写集入队
     *   取到 == 24  → 由它塞收尾包并推进 physical
     *   取到 > 24   → 这一片满了，本事务放弃入队（计入 abort）
     * 这样每片最多 24 条写集 + 1 个收尾包，容量固定，不需要定时推进。
     */
    std::atomic<uint32_t> pack_slot_seq[EPOCH_DEFAULT_CACHE_SLOTS];
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
    /*
     * 发送游标：本节点“收尾包已经发到哪个时间片”（连续推进，从 1 开始）。
     * send_epoch > E 就表示 E 的全部写集和收尾包都已发出。
     * 用它替代原来按 epoch % 512 存的“本端已发完”布尔——布尔会被后面的
     * 时间片覆盖，游标不会。
     */
    /*
     * 每个 peer 一个发送游标：该 peer 的队列已经连续发到哪个时间片。
     * EpochGetSendEpoch() 返回所有 peer 的最小值，语义仍是"这一片已发给所有人"。
     */
    std::atomic<uint64_t> send_epoch_peer[EPOCH_MAX_NODES];
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

    /*
     * 时间片“离开计数”：每个格子记一个时间片，计数器记有多少个终端
     * 已经离开它（即不会再往它里面放事务）。计到终端总数，就把收尾包塞进
     * 该时间片的发送队列。
     *
     * 用 epoch 做格子标签：环形复用，遇到新时间片就把格子重置（持锁做，避免并发）。
     */
    struct EpochDepartSlot {
        slock_t lock;
        uint64_t epoch;
        uint32_t count;
        /* 该时间片的收尾包是否已经塞过：只允许塞一次，避免重复塞导致对端反复处理 */
        bool eof_pushed;
    } depart_slots[1024];
};

extern Size EpochShmemSize(void);

/* 某终端已离开某时间片（分配到了更新的时间片）：计数到终端总数就塞收尾包 */

/* 发送游标（所有 peer 的最小值）：这一片是否已发给全部对端 */
uint64_t EpochGetSendEpoch(void);
/* 单个 peer 的发送游标 */
uint64_t EpochGetSendEpochPeer(uint32_t peer);
/* 某个 peer 的某时间片收尾包已发出后调用；只允许连续推进 */
void EpochAdvanceSendEpoch(uint32_t peer, uint64_t epoch);
extern void EpochShmemInit(void);
extern EpochSharedState* EpochGetSharedState(void);
extern uint64_t EpochGetNextCommitCsn(void);

#endif /* OPENGAUSS_E_SHMEM_H */
