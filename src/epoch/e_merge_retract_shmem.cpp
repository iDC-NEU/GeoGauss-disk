#include "postgres.h"

#include "epoch/e_epoch_capacity.h"
#include "epoch/e_merge_retract_shmem.h"

#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/spin.h"

#include "utils/elog.h"

static constexpr const char* EPOCH_MERGE_RETRACT_SHMEM_NAME = "Epoch Merge Retract Ring";

struct EpochMergeRetractEntry {
    uint64_t epoch;
    uint64_t wire_key;
};

struct EpochMergeRetractRing {
    slock_t lock;
    uint32_t head;
    uint32_t tail;
    uint32_t capacity;
    EpochMergeRetractEntry entries[1];
};

static EpochMergeRetractRing* g_mergeRetractRing = nullptr;

Size EpochMergeRetractShmemSize(void)
{
    uint32_t cap = EpochGetMergeDoneCapacity();
    return offsetof(EpochMergeRetractRing, entries) + static_cast<size_t>(cap) * sizeof(EpochMergeRetractEntry);
}

void EpochMergeRetractShmemInit(void)
{
    bool found = false;
    g_mergeRetractRing = static_cast<EpochMergeRetractRing*>(
        ShmemInitStruct(EPOCH_MERGE_RETRACT_SHMEM_NAME, EpochMergeRetractShmemSize(), &found));
    if (g_mergeRetractRing == nullptr) {
        return;
    }
    if (!found) {
        SpinLockInit(&g_mergeRetractRing->lock);
        g_mergeRetractRing->head = 0;
        g_mergeRetractRing->tail = 0;
        g_mergeRetractRing->capacity = EpochGetMergeDoneCapacity();
    }
}

void EpochMergeRetractPush(uint64_t epoch, uint64_t wire_key)
{
    if (g_mergeRetractRing == nullptr || wire_key == 0) {
        return;
    }
    SpinLockAcquire(&g_mergeRetractRing->lock);
    uint32_t cap = g_mergeRetractRing->capacity;
    if (cap == 0) {
        SpinLockRelease(&g_mergeRetractRing->lock);
        return;
    }
    uint32_t head = g_mergeRetractRing->head;
    uint32_t tail = g_mergeRetractRing->tail;
    uint32_t next = (tail + 1) % cap;
    if (next == head) {
        SpinLockRelease(&g_mergeRetractRing->lock);
        ereport(WARNING,
            (errmsg("epoch: merge retract ring full (capacity %u), epoch=%lu wire_key=%lu",
                cap,
                epoch,
                wire_key)));
        return;
    }
    g_mergeRetractRing->entries[tail].epoch = epoch;
    g_mergeRetractRing->entries[tail].wire_key = wire_key;
    g_mergeRetractRing->tail = next;
    SpinLockRelease(&g_mergeRetractRing->lock);
}

void EpochMergeRetractDrain(void (*fn)(uint64_t epoch, uint64_t wire_key))
{
    if (g_mergeRetractRing == nullptr || fn == nullptr) {
        return;
    }
    for (;;) {
        EpochMergeRetractEntry entry;
        entry.epoch = 0;
        entry.wire_key = 0;
        SpinLockAcquire(&g_mergeRetractRing->lock);
        uint32_t cap = g_mergeRetractRing->capacity;
        if (cap == 0) {
            SpinLockRelease(&g_mergeRetractRing->lock);
            return;
        }
        uint32_t head = g_mergeRetractRing->head;
        uint32_t tail = g_mergeRetractRing->tail;
        if (head == tail) {
            SpinLockRelease(&g_mergeRetractRing->lock);
            return;
        }
        entry = g_mergeRetractRing->entries[head];
        g_mergeRetractRing->head = (head + 1) % cap;
        SpinLockRelease(&g_mergeRetractRing->lock);
        fn(entry.epoch, entry.wire_key);
    }
}
