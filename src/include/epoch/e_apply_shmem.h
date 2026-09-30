#ifndef OPENGAUSS_E_APPLY_SHMEM_H
#define OPENGAUSS_E_APPLY_SHMEM_H

#include <atomic>
#include <cstdint>

#include "epoch/e_txn_wire.h"
#include "epoch/e_types.h"
#include "storage/spin.h"

static constexpr uint32_t EPOCH_APPLY_SLOT_FREE = 0;
static constexpr uint32_t EPOCH_APPLY_SLOT_READY = 1;
static constexpr uint32_t EPOCH_APPLY_SLOT_CLAIMED = 2;

static constexpr uint32_t EPOCH_APPLY_FLAG_IS_LOCAL = 0x01U;
static constexpr uint32_t EPOCH_APPLY_FLAG_IS_FINALIZE = 0x02U;
static constexpr uint32_t EPOCH_APPLY_FLAG_COUNT_COMMITTED = 0x04U;
/* payload 为原始 wire：apply 须在 merge_complete 后 BuildApplyTxn */
static constexpr uint32_t EPOCH_APPLY_FLAG_NEED_CRDT_BUILD = 0x08U;

/*
 * GeoGauss 式：[epoch_slot][worker_id] 私有格（与 Merge 同构）。
 * Submit → cells[epoch%slots][route_worker]；TryClaim 只扫 apply_epoch 行上本 worker 列。
 */
struct EpochApplyEpochSlotMeta {
    std::atomic<uint64_t> bound_epoch;
    std::atomic<uint32_t> inflight;
    slock_t bind_lock;
};

struct EpochApplyTaskSlot {
    std::atomic<uint32_t> state;
    uint64_t epoch;
    uint64_t wire_key;
    uint32_t local_node;
    uint32_t flags;
    uint32_t preferred_worker;
    uint32_t payload_len;
    uint64_t submit_us;
    char payload[EPOCH_MAX_WIRE_TXN_BYTES];
};

struct EpochApplyDoneSlot {
    uint64_t epoch;
    uint64_t wire_key;
    uint8_t ok;
    uint8_t pad[7];
};

struct EpochApplyQueueHeader {
    std::atomic<uint32_t> inflight;
    std::atomic<uint32_t> shutdown;
    std::atomic<uint64_t> apply_epoch;
    uint32_t slot_count;
    uint32_t worker_count;
    uint32_t depth;
    uint32_t tasks_per_slot; /* worker_count * depth */
    uint32_t task_capacity;
};

struct EpochApplyQueue {
    EpochApplyQueueHeader header;
    char blob[1];
};

extern Size EpochApplyQueueShmemSize(void);
extern void EpochApplyQueueShmemInit(void);
extern bool EpochApplyQueueSubmit(uint64_t epoch,
    uint64_t wire_key,
    uint32_t local_node,
    uint32_t flags,
    const char* payload,
    uint32_t payload_len,
    uint32_t route_worker,
    uint32_t n_workers);
extern bool EpochApplyQueueTryClaim(uint32_t worker_id, uint32_t n_workers, EpochApplyTaskSlot** out);
extern void EpochApplyQueueFinishTask(EpochApplyTaskSlot* task, bool ok);
/* 二维队列：恒 false；完成在 Finish 内联 hook */
extern bool EpochApplyQueuePopDone(EpochApplyDoneSlot* out);
extern uint32_t EpochApplyQueueInflight(void);
extern uint32_t EpochApplyQueueInflightForEpoch(uint64_t epoch);
extern uint32_t EpochApplyQueueGetTasksPerSlot(void);
extern uint32_t EpochApplyQueueGetDepth(void);
extern uint32_t EpochApplyQueueGetWorkerCount(void);
extern uint64_t EpochApplyQueueGetApplyEpoch(void);
extern void EpochApplyQueueSetApplyEpoch(uint64_t epoch);
extern bool EpochApplyQueueTryAdvanceApplyEpoch(uint64_t completed_epoch);
extern void EpochApplyQueueRequestShutdown(void);
extern bool EpochApplyQueueShutdownRequested(void);
extern void EpochApplyQueueResetCoordinatorState(void);
/* 由 EpochApplyPoolSetTaskDoneHook 注册；FinishTask 同进程内联 */
extern void EpochApplyQueueSetFinishHook(void (*hook)(uint64_t epoch, uint64_t wire_key, bool ok));

#endif /* OPENGAUSS_E_APPLY_SHMEM_H */
