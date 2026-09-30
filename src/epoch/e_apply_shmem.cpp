#include "postgres.h"

#include "epoch/e_apply_shmem.h"
#include "epoch/e_config.h"
#include "epoch/e_epoch_capacity.h"

#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/elog.h"

#include <sys/time.h>

static EpochApplyQueue* g_epochApplyQueue = nullptr;
static void (*g_apply_finish_hook)(uint64_t epoch, uint64_t wire_key, bool ok) = nullptr;

void EpochApplyQueueSetFinishHook(void (*hook)(uint64_t epoch, uint64_t wire_key, bool ok))
{
    g_apply_finish_hook = hook;
}

static uint64_t EpochApplyQueueWallUsNow(void)
{
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000ULL + static_cast<uint64_t>(tv.tv_usec);
}

static constexpr const char* EPOCH_APPLY_QUEUE_SHMEM_NAME = "Epoch Apply Queue";

static uint32_t EpochApplyResolveWorkerCountFromGuc(void)
{
    int n = g_epoch_commit_threads;
    if (n < 1) {
        n = 1;
    }
    if (n > static_cast<int>(EPOCH_MAX_APPLY_WORKERS)) {
        n = static_cast<int>(EPOCH_MAX_APPLY_WORKERS);
    }
    return static_cast<uint32_t>(n);
}

static uint32_t EpochApplyQueueSlotCount(void)
{
    if (g_epochApplyQueue == nullptr) {
        return EpochGetWorkQueueSlotCount();
    }
    return g_epochApplyQueue->header.slot_count;
}

static uint32_t EpochApplyQueueWorkerCount(void)
{
    if (g_epochApplyQueue == nullptr) {
        return EpochApplyResolveWorkerCountFromGuc();
    }
    return g_epochApplyQueue->header.worker_count;
}

static uint32_t EpochApplyQueueDepth(void)
{
    if (g_epochApplyQueue == nullptr) {
        return 8;
    }
    return g_epochApplyQueue->header.depth;
}

uint32_t EpochApplyQueueGetDepth(void)
{
    return EpochApplyQueueDepth();
}

uint32_t EpochApplyQueueGetWorkerCount(void)
{
    return EpochApplyQueueWorkerCount();
}

static uint32_t EpochApplyQueueTasksPerSlot(void)
{
    if (g_epochApplyQueue == nullptr) {
        return EpochApplyResolveWorkerCountFromGuc() * 8U;
    }
    return g_epochApplyQueue->header.tasks_per_slot;
}

uint32_t EpochApplyQueueGetTasksPerSlot(void)
{
    return EpochApplyQueueTasksPerSlot();
}

static EpochApplyEpochSlotMeta* EpochApplyQueueEpochMetas(void)
{
    return reinterpret_cast<EpochApplyEpochSlotMeta*>(g_epochApplyQueue->blob);
}

static EpochApplyTaskSlot* EpochApplyQueueTaskSlots(void)
{
    size_t meta_bytes = static_cast<size_t>(EpochApplyQueueSlotCount()) * sizeof(EpochApplyEpochSlotMeta);
    return reinterpret_cast<EpochApplyTaskSlot*>(g_epochApplyQueue->blob + meta_bytes);
}

static uint32_t EpochApplyEpochSlotIndex(uint64_t epoch)
{
    uint32_t n = EpochApplyQueueSlotCount();
    if (n == 0) {
        return 0;
    }
    return static_cast<uint32_t>(epoch % n);
}

static uint32_t EpochApplyCellBase(uint32_t slot_idx, uint32_t worker_id)
{
    const uint32_t workers = EpochApplyQueueWorkerCount();
    const uint32_t depth = EpochApplyQueueDepth();
    return (slot_idx * workers + worker_id) * depth;
}

static void EpochApplyComputeLayout(uint32_t* out_slots, uint32_t* out_workers, uint32_t* out_depth, uint32_t* out_task_cap)
{
    uint32_t slots = EpochGetWorkQueueSlotCount();
    uint32_t cap = EpochGetApplyQueueCapacity();
    uint32_t workers = EpochApplyResolveWorkerCountFromGuc();
    if (slots == 0) {
        slots = 1;
    }
    if (workers == 0) {
        workers = 1;
    }
    if (cap < slots) {
        cap = slots;
    }
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

Size EpochApplyQueueShmemSize(void)
{
    uint32_t slots = 0;
    uint32_t workers = 0;
    uint32_t depth = 0;
    uint32_t task_cap = 0;
    EpochApplyComputeLayout(&slots, &workers, &depth, &task_cap);
    return offsetof(EpochApplyQueue, blob) + static_cast<size_t>(slots) * sizeof(EpochApplyEpochSlotMeta) +
           static_cast<size_t>(task_cap) * sizeof(EpochApplyTaskSlot);
}

static void EpochApplyClearEpochRow(uint32_t slot_idx)
{
    const uint32_t workers = EpochApplyQueueWorkerCount();
    const uint32_t depth = EpochApplyQueueDepth();
    EpochApplyTaskSlot* tasks = EpochApplyQueueTaskSlots();
    EpochApplyEpochSlotMeta& meta = EpochApplyQueueEpochMetas()[slot_idx];
    for (uint32_t w = 0; w < workers; ++w) {
        uint32_t base = EpochApplyCellBase(slot_idx, w);
        for (uint32_t j = 0; j < depth; ++j) {
            EpochApplyTaskSlot& t = tasks[base + j];
            uint32_t st = t.state.load(std::memory_order_relaxed);
            if (st == EPOCH_APPLY_SLOT_READY || st == EPOCH_APPLY_SLOT_CLAIMED) {
                t.payload_len = 0;
                t.epoch = 0;
                t.state.store(EPOCH_APPLY_SLOT_FREE, std::memory_order_relaxed);
                if (g_epochApplyQueue->header.inflight.load(std::memory_order_relaxed) > 0) {
                    g_epochApplyQueue->header.inflight.fetch_sub(1, std::memory_order_relaxed);
                }
            } else {
                t.state.store(EPOCH_APPLY_SLOT_FREE, std::memory_order_relaxed);
                t.payload_len = 0;
                t.epoch = 0;
            }
        }
    }
    meta.inflight.store(0, std::memory_order_relaxed);
}

static bool EpochApplyEnsureEpochRow(uint32_t slot_idx, uint64_t epoch)
{
    EpochApplyEpochSlotMeta& meta = EpochApplyQueueEpochMetas()[slot_idx];
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
    EpochApplyClearEpochRow(slot_idx);
    meta.bound_epoch.store(epoch, std::memory_order_release);
    SpinLockRelease(&meta.bind_lock);
    return true;
}

static void EpochApplyQueueInitHeader(void)
{
    g_epochApplyQueue->header.inflight.store(0, std::memory_order_relaxed);
    g_epochApplyQueue->header.shutdown.store(0, std::memory_order_relaxed);
    g_epochApplyQueue->header.apply_epoch.store(0, std::memory_order_relaxed);

    uint32_t slots = 0;
    uint32_t workers = 0;
    uint32_t depth = 0;
    uint32_t task_cap = 0;
    EpochApplyComputeLayout(&slots, &workers, &depth, &task_cap);
    g_epochApplyQueue->header.slot_count = slots;
    g_epochApplyQueue->header.worker_count = workers;
    g_epochApplyQueue->header.depth = depth;
    g_epochApplyQueue->header.tasks_per_slot = workers * depth;
    g_epochApplyQueue->header.task_capacity = task_cap;

    for (uint32_t i = 0; i < slots; ++i) {
        EpochApplyQueueEpochMetas()[i].bound_epoch.store(0, std::memory_order_relaxed);
        EpochApplyQueueEpochMetas()[i].inflight.store(0, std::memory_order_relaxed);
        SpinLockInit(&EpochApplyQueueEpochMetas()[i].bind_lock);
    }
    EpochApplyTaskSlot* tasks = EpochApplyQueueTaskSlots();
    for (uint32_t i = 0; i < task_cap; ++i) {
        tasks[i].state.store(EPOCH_APPLY_SLOT_FREE, std::memory_order_relaxed);
        tasks[i].payload_len = 0;
        tasks[i].preferred_worker = UINT32_MAX;
        tasks[i].submit_us = 0;
        tasks[i].epoch = 0;
    }
}

void EpochApplyQueueShmemInit(void)
{
    bool found = false;
    g_epochApplyQueue = static_cast<EpochApplyQueue*>(
        ShmemInitStruct(EPOCH_APPLY_QUEUE_SHMEM_NAME, EpochApplyQueueShmemSize(), &found));
    if (g_epochApplyQueue == nullptr) {
        return;
    }
    if (!found) {
        EpochApplyQueueInitHeader();
    }
}

bool EpochApplyQueueSubmit(uint64_t epoch,
    uint64_t wire_key,
    uint32_t local_node,
    uint32_t flags,
    const char* payload,
    uint32_t payload_len,
    uint32_t route_worker,
    uint32_t n_workers)
{
    if (g_epochApplyQueue == nullptr || epoch == 0) {
        ereport(ERROR,
            (errmsg("epoch apply queue submit: queue not ready epoch=%lu",
                static_cast<unsigned long>(epoch))));
    }
    if (payload != nullptr) {
        if (payload_len == 0 || payload_len > EPOCH_MAX_WIRE_TXN_BYTES) {
            ereport(ERROR,
                (errmsg("epoch apply queue submit: bad payload epoch=%lu len=%u",
                    static_cast<unsigned long>(epoch),
                    payload_len)));
        }
    }

    const uint32_t workers = EpochApplyQueueWorkerCount();
    uint32_t wid = route_worker;
    if (n_workers > 0 && wid >= n_workers) {
        wid = wid % n_workers;
    }
    if (wid >= workers) {
        wid = wid % workers;
    }

    const uint32_t slot_idx = EpochApplyEpochSlotIndex(epoch);
    if (!EpochApplyEnsureEpochRow(slot_idx, epoch)) {
        ereport(ERROR,
            (errmsg("epoch apply queue submit: ensure row failed epoch=%lu (bound/inflight conflict); "
                    "increase epoch_apply_queue_capacity or fix epoch reuse",
                static_cast<unsigned long>(epoch))));
    }

    const uint32_t depth = EpochApplyQueueDepth();
    const uint32_t base = EpochApplyCellBase(slot_idx, wid);
    EpochApplyTaskSlot* tasks = EpochApplyQueueTaskSlots();

    EpochApplyTaskSlot* chosen = nullptr;
    for (uint32_t j = 0; j < depth; ++j) {
        EpochApplyTaskSlot& slot = tasks[base + j];
        uint32_t expected = EPOCH_APPLY_SLOT_FREE;
        if (slot.state.compare_exchange_strong(expected, EPOCH_APPLY_SLOT_CLAIMED, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            chosen = &slot;
            break;
        }
    }
    if (chosen == nullptr) {
        ereport(ERROR,
            (errmsg("epoch apply queue full: epoch=%lu worker=%u depth=%u capacity=%u; "
                    "increase epoch_apply_queue_capacity (no pending retry)",
                static_cast<unsigned long>(epoch),
                wid,
                depth,
                EpochGetApplyQueueCapacity())));
    }

    chosen->epoch = epoch;
    chosen->wire_key = wire_key;
    chosen->local_node = local_node;
    chosen->flags = flags;
    chosen->preferred_worker = wid;
    chosen->payload_len = payload_len;
    chosen->submit_us = EpochApplyQueueWallUsNow();
    if (payload != nullptr && payload_len > 0) {
        errno_t rc = memcpy_s(chosen->payload, sizeof(chosen->payload), payload, payload_len);
        securec_check(rc, "\0", "\0");
    }
    chosen->state.store(EPOCH_APPLY_SLOT_READY, std::memory_order_release);
    g_epochApplyQueue->header.inflight.fetch_add(1, std::memory_order_relaxed);
    EpochApplyQueueEpochMetas()[slot_idx].inflight.fetch_add(1, std::memory_order_relaxed);
    return true;
}

bool EpochApplyQueueTryClaim(uint32_t worker_id, uint32_t n_workers, EpochApplyTaskSlot** out)
{
    if (g_epochApplyQueue == nullptr || out == nullptr) {
        return false;
    }
    (void)n_workers;
    const uint32_t workers = EpochApplyQueueWorkerCount();
    if (worker_id >= workers) {
        return false;
    }

    const uint64_t apply_epoch = g_epochApplyQueue->header.apply_epoch.load(std::memory_order_acquire);
    if (apply_epoch == 0) {
        return false;
    }

    const uint32_t slot_idx = EpochApplyEpochSlotIndex(apply_epoch);
    EpochApplyEpochSlotMeta& meta = EpochApplyQueueEpochMetas()[slot_idx];
    if (meta.bound_epoch.load(std::memory_order_acquire) != apply_epoch) {
        return false;
    }

    const uint32_t depth = EpochApplyQueueDepth();
    const uint32_t base = EpochApplyCellBase(slot_idx, worker_id);
    EpochApplyTaskSlot* tasks = EpochApplyQueueTaskSlots();

    for (uint32_t j = 0; j < depth; ++j) {
        EpochApplyTaskSlot& slot = tasks[base + j];
        if (slot.state.load(std::memory_order_acquire) != EPOCH_APPLY_SLOT_READY) {
            continue;
        }
        if (slot.epoch != apply_epoch) {
            continue;
        }
        uint32_t expected = EPOCH_APPLY_SLOT_READY;
        if (!slot.state.compare_exchange_strong(expected, EPOCH_APPLY_SLOT_CLAIMED, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            continue;
        }
        if (slot.epoch != g_epochApplyQueue->header.apply_epoch.load(std::memory_order_acquire) ||
            meta.bound_epoch.load(std::memory_order_acquire) != apply_epoch) {
            slot.state.store(EPOCH_APPLY_SLOT_READY, std::memory_order_release);
            continue;
        }
        *out = &slot;
        return true;
    }
    return false;
}

void EpochApplyQueueFinishTask(EpochApplyTaskSlot* task, bool ok)
{
    if (g_epochApplyQueue == nullptr || task == nullptr) {
        return;
    }
    const uint64_t epoch = task->epoch;
    const uint64_t wire_key = task->wire_key;
    const uint32_t slot_idx = EpochApplyEpochSlotIndex(epoch);
    task->payload_len = 0;
    task->state.store(EPOCH_APPLY_SLOT_FREE, std::memory_order_release);
    if (g_epochApplyQueue->header.inflight.load(std::memory_order_relaxed) > 0) {
        g_epochApplyQueue->header.inflight.fetch_sub(1, std::memory_order_relaxed);
    }
    EpochApplyEpochSlotMeta& meta = EpochApplyQueueEpochMetas()[slot_idx];
    if (meta.inflight.load(std::memory_order_relaxed) > 0) {
        meta.inflight.fetch_sub(1, std::memory_order_relaxed);
    }
    if (g_apply_finish_hook != nullptr) {
        g_apply_finish_hook(epoch, wire_key, ok);
    }
}

bool EpochApplyQueuePopDone(EpochApplyDoneSlot* out)
{
    (void)out;
    return false;
}

uint32_t EpochApplyQueueInflight(void)
{
    if (g_epochApplyQueue == nullptr) {
        return 0;
    }
    return g_epochApplyQueue->header.inflight.load(std::memory_order_acquire);
}

uint32_t EpochApplyQueueInflightForEpoch(uint64_t epoch)
{
    if (g_epochApplyQueue == nullptr || epoch == 0) {
        return 0;
    }
    uint32_t slot_idx = EpochApplyEpochSlotIndex(epoch);
    EpochApplyEpochSlotMeta& meta = EpochApplyQueueEpochMetas()[slot_idx];
    if (meta.bound_epoch.load(std::memory_order_acquire) != epoch) {
        return 0;
    }
    return meta.inflight.load(std::memory_order_acquire);
}

uint64_t EpochApplyQueueGetApplyEpoch(void)
{
    if (g_epochApplyQueue == nullptr) {
        return 0;
    }
    return g_epochApplyQueue->header.apply_epoch.load(std::memory_order_acquire);
}

void EpochApplyQueueSetApplyEpoch(uint64_t epoch)
{
    if (g_epochApplyQueue == nullptr) {
        return;
    }
    g_epochApplyQueue->header.apply_epoch.store(epoch, std::memory_order_release);
}

bool EpochApplyQueueTryAdvanceApplyEpoch(uint64_t completed_epoch)
{
    if (g_epochApplyQueue == nullptr || completed_epoch == 0) {
        return false;
    }
    uint64_t cur = completed_epoch;
    return g_epochApplyQueue->header.apply_epoch.compare_exchange_strong(cur,
        completed_epoch + 1,
        std::memory_order_acq_rel,
        std::memory_order_acquire);
}

void EpochApplyQueueRequestShutdown(void)
{
    if (g_epochApplyQueue == nullptr) {
        return;
    }
    g_epochApplyQueue->header.shutdown.store(1, std::memory_order_release);
}

bool EpochApplyQueueShutdownRequested(void)
{
    if (g_epochApplyQueue == nullptr) {
        return false;
    }
    return g_epochApplyQueue->header.shutdown.load(std::memory_order_acquire) != 0;
}

void EpochApplyQueueResetCoordinatorState(void)
{
    if (g_epochApplyQueue == nullptr) {
        return;
    }
    g_epochApplyQueue->header.shutdown.store(0, std::memory_order_release);
    g_epochApplyQueue->header.apply_epoch.store(0, std::memory_order_release);
}
