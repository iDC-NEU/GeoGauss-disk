#ifndef OPENGAUSS_E_MERGE_SHMEM_H
#define OPENGAUSS_E_MERGE_SHMEM_H

#include <atomic>
#include <cstdint>

#include "epoch/e_txn_wire.h"
#include "epoch/e_types.h"
#include "storage/spin.h"

static constexpr uint32_t EPOCH_MERGE_SLOT_FREE = 0;
static constexpr uint32_t EPOCH_MERGE_SLOT_READY = 1;
static constexpr uint32_t EPOCH_MERGE_SLOT_CLAIMED = 2;

static constexpr uint32_t EPOCH_MERGE_FLAG_IS_LOCAL = 0x01U;

/*
 * GeoGauss 式：[epoch_slot][worker_id] 私有格 + 共享 merge_epoch 游标（对齐 apply_epoch）。
 * Submit 可预填 [merge_epoch, physical)；TryClaim 只扫 merge_epoch 行本列（无 steal）。
 * 无全局 merge_lock；格内用 state CAS。
 */
struct EpochMergeEpochSlotMeta {
    std::atomic<uint64_t> bound_epoch;
    std::atomic<uint32_t> inflight;
    slock_t bind_lock; /* 仅绑/清 epoch 行时持有，极短 */
};

struct EpochMergeTaskSlot {
    std::atomic<uint32_t> state;
    uint64_t epoch;
    uint64_t wire_key;
    uint32_t flags;
    uint32_t preferred_worker;
    uint32_t payload_len;
    uint64_t submit_us;
    char payload[EPOCH_MAX_WIRE_TXN_BYTES];
};

struct EpochMergeDoneSlot {
    uint64_t epoch;
    uint64_t wire_key;
    uint64_t csn;
    uint32_t server_id;
    uint8_t ok;
    uint8_t txn_accepted;
    uint8_t any_new_merged;
    uint8_t pad;
};

struct EpochMergeQueueHeader {
    std::atomic<uint32_t> inflight;
    std::atomic<uint32_t> shutdown;
    std::atomic<uint64_t> merge_epoch; /* 当前执行/关窗游标；须 < physical */
    uint32_t slot_count;     /* epoch 行数 */
    uint32_t worker_count;   /* 列数（= merge_threads） */
    uint32_t depth;          /* 每格环形深度 */
    uint32_t tasks_per_slot; /* = worker_count * depth（兼容旧 throttle 语义） */
    uint32_t task_capacity;  /* = slot_count * worker_count * depth */
};

struct EpochMergeQueue {
    EpochMergeQueueHeader header;
    char blob[1];
};

extern Size EpochMergeQueueShmemSize(void);
extern void EpochMergeQueueShmemInit(void);
extern bool EpochMergeQueueSubmit(uint64_t epoch,
    uint64_t wire_key,
    uint32_t flags,
    const char* payload,
    uint32_t payload_len,
    uint32_t route_worker,
    uint32_t n_workers);
extern bool EpochMergeQueueTryClaim(uint32_t worker_id, uint32_t n_workers, EpochMergeTaskSlot** out);
/* 释放格；result 非空时由调用方在同进程内调 done hook（不再进全局 done 环） */
extern void EpochMergeQueueFinishTask(EpochMergeTaskSlot* task, const EpochMergeDoneSlot* result);
/* 保留 API：二维队列下恒为 false（完成在 Finish 路径内联） */
extern bool EpochMergeQueuePopDone(EpochMergeDoneSlot* out);
extern uint32_t EpochMergeQueueInflight(void);
extern uint32_t EpochMergeQueueInflightForEpoch(uint64_t epoch);
extern uint32_t EpochMergeQueueGetDepth(void);
extern uint32_t EpochMergeQueueGetWorkerCount(void);
extern uint64_t EpochMergeQueueGetMergeEpoch(void);
extern void EpochMergeQueueSetMergeEpoch(uint64_t epoch);
extern bool EpochMergeQueueTryAdvanceMergeEpoch(uint64_t completed_epoch);
extern void EpochMergeQueueRequestShutdown(void);
extern bool EpochMergeQueueShutdownRequested(void);
extern void EpochMergeQueueResetCoordinatorState(void);

#endif /* OPENGAUSS_E_MERGE_SHMEM_H */
