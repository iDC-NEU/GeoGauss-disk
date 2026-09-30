#include "postgres.h"

#include "epoch/e_merge_shmem.h"
#include "epoch/e_config.h"
#include "epoch/e_epoch_capacity.h"

#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/elog.h"

#include <sys/time.h>

static EpochMergeQueue* g_epochMergeQueue = nullptr;

static uint64_t EpochMergeQueueWallUsNow(void)
{
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000ULL + static_cast<uint64_t>(tv.tv_usec);
}

static constexpr const char* EPOCH_MERGE_QUEUE_SHMEM_NAME = "Epoch Merge Queue";

static uint32_t EpochMergeResolveWorkerCountFromGuc(void)
{
    int n = g_epoch_merge_threads;
    if (n < 1) {
        n = 1;
    }
    if (n > static_cast<int>(EPOCH_MAX_MERGE_WORKERS)) {
        n = static_cast<int>(EPOCH_MAX_MERGE_WORKERS);
    }
    return static_cast<uint32_t>(n);
}

static uint32_t EpochMergeQueueSlotCount(void)
{
    if (g_epochMergeQueue == nullptr) {
        return EpochGetWorkQueueSlotCount();
    }
    return g_epochMergeQueue->header.slot_count;
}

static uint32_t EpochMergeQueueWorkerCount(void)
{
    if (g_epochMergeQueue == nullptr) {
        return EpochMergeResolveWorkerCountFromGuc();
    }
    return g_epochMergeQueue->header.worker_count;
}

static uint32_t EpochMergeQueueDepth(void)
{
    if (g_epochMergeQueue == nullptr) {
        return 8;
    }
    return g_epochMergeQueue->header.depth;
}

uint32_t EpochMergeQueueGetDepth(void)
{
    return EpochMergeQueueDepth();
}

uint32_t EpochMergeQueueGetWorkerCount(void)
{
    return EpochMergeQueueWorkerCount();
}

static EpochMergeEpochSlotMeta* EpochMergeQueueEpochMetas(void)
{
    return reinterpret_cast<EpochMergeEpochSlotMeta*>(g_epochMergeQueue->blob);
}

static EpochMergeTaskSlot* EpochMergeQueueTaskSlots(void)
{
    size_t meta_bytes = static_cast<size_t>(EpochMergeQueueSlotCount()) * sizeof(EpochMergeEpochSlotMeta);
    return reinterpret_cast<EpochMergeTaskSlot*>(g_epochMergeQueue->blob + meta_bytes);
}

static uint32_t EpochMergeEpochSlotIndex(uint64_t epoch)
{
    uint32_t n = EpochMergeQueueSlotCount();
    if (n == 0) {
        return 0;
    }
    return static_cast<uint32_t>(epoch % n);
}

/* tasks[slot][worker][depth] 线性下标 */
static uint32_t EpochMergeCellBase(uint32_t slot_idx, uint32_t worker_id)
{
    const uint32_t workers = EpochMergeQueueWorkerCount();
    const uint32_t depth = EpochMergeQueueDepth();
    return (slot_idx * workers + worker_id) * depth;
}

static void EpochMergeComputeLayout(uint32_t* out_slots, uint32_t* out_workers, uint32_t* out_depth, uint32_t* out_task_cap)
{
    uint32_t slots = EpochGetWorkQueueSlotCount();
    uint32_t cap = EpochGetMergeQueueCapacity();
    uint32_t workers = EpochMergeResolveWorkerCountFromGuc();
    if (slots == 0) {
        slots = 1;
    }
    if (workers == 0) {
        workers = 1;
    }
    if (cap < slots) {
        cap = slots;
    }
    /* depth = max(8, capacity / (slots * workers))，总 task ≈ capacity 量级 */
    uint32_t denom = slots * workers;
    uint32_t depth = cap / denom;
    if (depth < 8) {
        depth = 8;
    }
    uint32_t task_cap = slots * workers * depth;
    *out_slots = slots;
    *out_workers = workers;
    *out_depth = depth;
    *out_task_cap = task_cap;
}

Size EpochMergeQueueShmemSize(void)
{
    uint32_t slots = 0;
    uint32_t workers = 0;
    uint32_t depth = 0;
    uint32_t task_cap = 0;
    EpochMergeComputeLayout(&slots, &workers, &depth, &task_cap);
    return offsetof(EpochMergeQueue, blob) + static_cast<size_t>(slots) * sizeof(EpochMergeEpochSlotMeta) +
           static_cast<size_t>(task_cap) * sizeof(EpochMergeTaskSlot);
}

static void EpochMergeClearEpochRow(uint32_t slot_idx)
{
    const uint32_t workers = EpochMergeQueueWorkerCount();
    const uint32_t depth = EpochMergeQueueDepth();
    EpochMergeTaskSlot* tasks = EpochMergeQueueTaskSlots();
    EpochMergeEpochSlotMeta& meta = EpochMergeQueueEpochMetas()[slot_idx];
    for (uint32_t w = 0; w < workers; ++w) {
        uint32_t base = EpochMergeCellBase(slot_idx, w);
        for (uint32_t j = 0; j < depth; ++j) {
            EpochMergeTaskSlot& t = tasks[base + j];
            uint32_t st = t.state.load(std::memory_order_relaxed);
            if (st == EPOCH_MERGE_SLOT_READY || st == EPOCH_MERGE_SLOT_CLAIMED) {
                t.payload_len = 0;
                t.epoch = 0;
                t.state.store(EPOCH_MERGE_SLOT_FREE, std::memory_order_relaxed);
                if (g_epochMergeQueue->header.inflight.load(std::memory_order_relaxed) > 0) {
                    g_epochMergeQueue->header.inflight.fetch_sub(1, std::memory_order_relaxed);
                }
            } else {
                t.state.store(EPOCH_MERGE_SLOT_FREE, std::memory_order_relaxed);
                t.payload_len = 0;
                t.epoch = 0;
            }
        }
    }
    meta.inflight.store(0, std::memory_order_relaxed);
}

static bool EpochMergeEnsureEpochRow(uint32_t slot_idx, uint64_t epoch)
{
    EpochMergeEpochSlotMeta& meta = EpochMergeQueueEpochMetas()[slot_idx];
    uint64_t bound = meta.bound_epoch.load(std::memory_order_acquire);
    if (bound == epoch) {
        return true;
    }
    SpinLockAcquire(&meta.bind_lock);
    bound = meta.bound_epoch.load(std::memory_order_relaxed);
    if (bound == epoch) {
        SpinLockRelease(&meta.bind_lock);
        return true;
    }
    if (meta.inflight.load(std::memory_order_relaxed) > 0) {
        SpinLockRelease(&meta.bind_lock);
        return false;
    }
    EpochMergeClearEpochRow(slot_idx);
    meta.bound_epoch.store(epoch, std::memory_order_release);
    SpinLockRelease(&meta.bind_lock);
    return true;
}

static void EpochMergeQueueInitHeader(void)
{
    g_epochMergeQueue->header.inflight.store(0, std::memory_order_relaxed);
    g_epochMergeQueue->header.shutdown.store(0, std::memory_order_relaxed);
    g_epochMergeQueue->header.merge_epoch.store(0, std::memory_order_relaxed);

    uint32_t slots = 0;
    uint32_t workers = 0;
    uint32_t depth = 0;
    uint32_t task_cap = 0;
    EpochMergeComputeLayout(&slots, &workers, &depth, &task_cap);
    g_epochMergeQueue->header.slot_count = slots;
    g_epochMergeQueue->header.worker_count = workers;
    g_epochMergeQueue->header.depth = depth;
    g_epochMergeQueue->header.tasks_per_slot = workers * depth;
    g_epochMergeQueue->header.task_capacity = task_cap;

    for (uint32_t i = 0; i < slots; ++i) {
        EpochMergeQueueEpochMetas()[i].bound_epoch.store(0, std::memory_order_relaxed);
        EpochMergeQueueEpochMetas()[i].inflight.store(0, std::memory_order_relaxed);
        SpinLockInit(&EpochMergeQueueEpochMetas()[i].bind_lock);
    }
    EpochMergeTaskSlot* tasks = EpochMergeQueueTaskSlots();
    for (uint32_t i = 0; i < task_cap; ++i) {
        tasks[i].state.store(EPOCH_MERGE_SLOT_FREE, std::memory_order_relaxed);
        tasks[i].payload_len = 0;
        tasks[i].preferred_worker = UINT32_MAX;
        tasks[i].submit_us = 0;
        tasks[i].epoch = 0;
    }
}

void EpochMergeQueueShmemInit(void)
{
    bool found = false;
    g_epochMergeQueue = static_cast<EpochMergeQueue*>(
        ShmemInitStruct(EPOCH_MERGE_QUEUE_SHMEM_NAME, EpochMergeQueueShmemSize(), &found));
    if (g_epochMergeQueue == nullptr) {
        return;
    }
    if (!found) {
        EpochMergeQueueInitHeader();
    }
}

bool EpochMergeQueueSubmit(uint64_t epoch,
    uint64_t wire_key,
    uint32_t flags,
    const char* payload,
    uint32_t payload_len,
    uint32_t route_worker,
    uint32_t n_workers)
{
    if (g_epochMergeQueue == nullptr || epoch == 0) {
        ereport(ERROR,
            (errmsg("epoch merge queue submit: queue not ready epoch=%lu",
                static_cast<unsigned long>(epoch))));
    }
    if (payload == nullptr || payload_len == 0 || payload_len > EPOCH_MAX_WIRE_TXN_BYTES) {
        ereport(ERROR,
            (errmsg("epoch merge queue submit: bad payload epoch=%lu len=%u",
                static_cast<unsigned long>(epoch),
                payload_len)));
    }

    const uint32_t workers = EpochMergeQueueWorkerCount();
    uint32_t wid = route_worker;
    if (n_workers > 0 && wid >= n_workers) {
        wid = wid % n_workers;
    }
    if (wid >= workers) {
        wid = wid % workers;
    }

    const uint32_t slot_idx = EpochMergeEpochSlotIndex(epoch);
    if (!EpochMergeEnsureEpochRow(slot_idx, epoch)) {
        ereport(ERROR,
            (errmsg("epoch merge queue submit: ensure row failed epoch=%lu (bound/inflight conflict); "
                    "increase epoch_merge_queue_capacity",
                static_cast<unsigned long>(epoch))));
    }

    const uint32_t depth = EpochMergeQueueDepth();
    const uint32_t base = EpochMergeCellBase(slot_idx, wid);
    EpochMergeTaskSlot* tasks = EpochMergeQueueTaskSlots();

    EpochMergeTaskSlot* chosen = nullptr;
    for (uint32_t j = 0; j < depth; ++j) {
        EpochMergeTaskSlot& slot = tasks[base + j];
        uint32_t expected = EPOCH_MERGE_SLOT_FREE;
        if (slot.state.compare_exchange_strong(expected, EPOCH_MERGE_SLOT_CLAIMED, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            chosen = &slot;
            break;
        }
    }
    if (chosen == nullptr) {
        ereport(ERROR,
            (errmsg("epoch merge queue full: epoch=%lu worker=%u depth=%u capacity=%u; "
                    "increase epoch_merge_queue_capacity (no recv defer)",
                static_cast<unsigned long>(epoch),
                wid,
                depth,
                EpochGetMergeQueueCapacity())));
    }

    chosen->epoch = epoch;
    chosen->wire_key = wire_key;
    chosen->flags = flags;
    chosen->preferred_worker = wid;
    chosen->payload_len = payload_len;
    chosen->submit_us = EpochMergeQueueWallUsNow();
    errno_t rc = memcpy_s(chosen->payload, sizeof(chosen->payload), payload, payload_len);
    securec_check(rc, "\0", "\0");
    chosen->state.store(EPOCH_MERGE_SLOT_READY, std::memory_order_release);
    g_epochMergeQueue->header.inflight.fetch_add(1, std::memory_order_relaxed);
    EpochMergeQueueEpochMetas()[slot_idx].inflight.fetch_add(1, std::memory_order_relaxed);
    return true;
}

bool EpochMergeQueueTryClaim(uint32_t worker_id, uint32_t n_workers, EpochMergeTaskSlot** out)
{
    if (g_epochMergeQueue == nullptr || out == nullptr) {
        return false;
    }
    const uint32_t workers = EpochMergeQueueWorkerCount();
    if (worker_id >= workers) {
        return false;
    }
    (void)n_workers;

    const uint64_t merge_epoch = g_epochMergeQueue->header.merge_epoch.load(std::memory_order_acquire);
    if (merge_epoch == 0) {
        return false;
    }

    const uint32_t slot_idx = EpochMergeEpochSlotIndex(merge_epoch);
    EpochMergeEpochSlotMeta& meta = EpochMergeQueueEpochMetas()[slot_idx];
    if (meta.bound_epoch.load(std::memory_order_acquire) != merge_epoch) {
        return false;
    }

    const uint32_t depth = EpochMergeQueueDepth();
    const uint32_t base = EpochMergeCellBase(slot_idx, worker_id);
    EpochMergeTaskSlot* tasks = EpochMergeQueueTaskSlots();

    for (uint32_t j = 0; j < depth; ++j) {
        EpochMergeTaskSlot& slot = tasks[base + j];
        if (slot.state.load(std::memory_order_acquire) != EPOCH_MERGE_SLOT_READY) {
            continue;
        }
        if (slot.epoch != merge_epoch) {
            continue;
        }
        uint32_t expected = EPOCH_MERGE_SLOT_READY;
        if (!slot.state.compare_exchange_strong(expected, EPOCH_MERGE_SLOT_CLAIMED, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            continue;
        }
        if (slot.epoch != g_epochMergeQueue->header.merge_epoch.load(std::memory_order_acquire) ||
            meta.bound_epoch.load(std::memory_order_acquire) != merge_epoch) {
            slot.state.store(EPOCH_MERGE_SLOT_READY, std::memory_order_release);
            continue;
        }
        *out = &slot;
        return true;
    }
    return false;
}

void EpochMergeQueueFinishTask(EpochMergeTaskSlot* task, const EpochMergeDoneSlot* result)
{
    if (g_epochMergeQueue == nullptr || task == nullptr) {
        return;
    }
    (void)result; /* 调用方同进程内联 done hook */
    const uint64_t epoch = task->epoch;
    const uint32_t slot_idx = EpochMergeEpochSlotIndex(epoch);
    task->payload_len = 0;
    task->state.store(EPOCH_MERGE_SLOT_FREE, std::memory_order_release);
    if (g_epochMergeQueue->header.inflight.load(std::memory_order_relaxed) > 0) {
        g_epochMergeQueue->header.inflight.fetch_sub(1, std::memory_order_relaxed);
    }
    EpochMergeEpochSlotMeta& meta = EpochMergeQueueEpochMetas()[slot_idx];
    if (meta.inflight.load(std::memory_order_relaxed) > 0) {
        meta.inflight.fetch_sub(1, std::memory_order_relaxed);
    }
}

bool EpochMergeQueuePopDone(EpochMergeDoneSlot* out)
{
    (void)out;
    return false;
}

uint32_t EpochMergeQueueInflight(void)
{
    if (g_epochMergeQueue == nullptr) {
        return 0;
    }
    return g_epochMergeQueue->header.inflight.load(std::memory_order_acquire);
}

uint32_t EpochMergeQueueInflightForEpoch(uint64_t epoch)
{
    if (g_epochMergeQueue == nullptr || epoch == 0) {
        return 0;
    }
    uint32_t slot_idx = EpochMergeEpochSlotIndex(epoch);
    EpochMergeEpochSlotMeta& meta = EpochMergeQueueEpochMetas()[slot_idx];
    if (meta.bound_epoch.load(std::memory_order_acquire) != epoch) {
        return 0;
    }
    return meta.inflight.load(std::memory_order_acquire);
}

void EpochMergeQueueRequestShutdown(void)
{
    if (g_epochMergeQueue == nullptr) {
        return;
    }
    g_epochMergeQueue->header.shutdown.store(1, std::memory_order_release);
}

bool EpochMergeQueueShutdownRequested(void)
{
    if (g_epochMergeQueue == nullptr) {
        return false;
    }
    return g_epochMergeQueue->header.shutdown.load(std::memory_order_acquire) != 0;
}

void EpochMergeQueueResetCoordinatorState(void)
{
    if (g_epochMergeQueue == nullptr) {
        return;
    }
    g_epochMergeQueue->header.shutdown.store(0, std::memory_order_release);
    g_epochMergeQueue->header.merge_epoch.store(0, std::memory_order_release);
}

uint64_t EpochMergeQueueGetMergeEpoch(void)
{
    if (g_epochMergeQueue == nullptr) {
        return 0;
    }
    return g_epochMergeQueue->header.merge_epoch.load(std::memory_order_acquire);
}

void EpochMergeQueueSetMergeEpoch(uint64_t epoch)
{
    if (g_epochMergeQueue == nullptr) {
        return;
    }
    g_epochMergeQueue->header.merge_epoch.store(epoch, std::memory_order_release);
}

bool EpochMergeQueueTryAdvanceMergeEpoch(uint64_t completed_epoch)
{
    if (g_epochMergeQueue == nullptr || completed_epoch == 0) {
        return false;
    }
    uint64_t cur = completed_epoch;
    return g_epochMergeQueue->header.merge_epoch.compare_exchange_strong(cur,
        completed_epoch + 1,
        std::memory_order_acq_rel,
        std::memory_order_acquire);
}
