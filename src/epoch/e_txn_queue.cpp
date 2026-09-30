#include "postgres.h"

#include "epoch/e_txn_queue.h"
#include "epoch/e_config.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_txn_wire.h"

#include "storage/ipc.h"
#include "storage/shmem.h"

struct EpochTxnQueueItem {
    uint32_t len;
    char data[EPOCH_MAX_WIRE_TXN_BYTES];
};

struct EpochTxnQueue {
    EpochTxnQueueHeader header;
    char blob[1];
};

static EpochTxnQueue* g_epochTxnQueue = nullptr;
static uint32_t g_epochTxnDrainRr = 0;

static void EpochTxnComputeLayout(uint32_t capacity, uint32_t* out_slots, uint32_t* out_spe, uint32_t* out_total)
{
    uint32_t slots = EpochGetWorkQueueSlotCount();
    if (slots == 0) {
        slots = 1;
    }
    if (capacity == 0) {
        capacity = EPOCH_DEFAULT_TXN_QUEUE_CAPACITY;
    }
    if (capacity < slots) {
        slots = capacity;
        if (slots == 0) {
            slots = 1;
            capacity = 1;
        }
    }
    uint32_t spe = capacity / slots;
    if (spe == 0) {
        spe = 1;
        capacity = slots;
    } else {
        capacity = spe * slots;
    }
    *out_slots = slots;
    *out_spe = spe;
    *out_total = capacity;
}

static EpochTxnEpochRing* EpochTxnRings(void)
{
    return reinterpret_cast<EpochTxnEpochRing*>(g_epochTxnQueue->blob);
}

static EpochTxnQueueItem* EpochTxnItems(void)
{
    size_t ring_bytes = static_cast<size_t>(g_epochTxnQueue->header.slot_count) * sizeof(EpochTxnEpochRing);
    return reinterpret_cast<EpochTxnQueueItem*>(g_epochTxnQueue->blob + ring_bytes);
}

static uint32_t EpochTxnSlotIndex(uint64_t epoch)
{
    uint32_t n = g_epochTxnQueue->header.slot_count;
    if (n == 0) {
        return 0;
    }
    return static_cast<uint32_t>(epoch % n);
}

Size EpochTxnQueueShmemSize(uint32_t capacity)
{
    uint32_t slots = 0;
    uint32_t spe = 0;
    uint32_t total = 0;
    EpochTxnComputeLayout(capacity, &slots, &spe, &total);
    return offsetof(EpochTxnQueue, blob) + static_cast<size_t>(slots) * sizeof(EpochTxnEpochRing) +
           static_cast<size_t>(total) * sizeof(EpochTxnQueueItem);
}

static void EpochTxnQueueInitHeader(uint32_t capacity)
{
    uint32_t slots = 0;
    uint32_t spe = 0;
    uint32_t total = 0;
    EpochTxnComputeLayout(capacity, &slots, &spe, &total);
    g_epochTxnQueue->header.slot_count = slots;
    g_epochTxnQueue->header.slots_per_epoch = spe;
    g_epochTxnQueue->header.total_capacity = total;
    g_epochTxnQueue->header.lock = LWLockAssign(LWTRANCHE_WAL_INSERT);
    EpochTxnEpochRing* rings = EpochTxnRings();
    for (uint32_t i = 0; i < slots; ++i) {
        rings[i].bound_epoch = 0;
        rings[i].head = 0;
        rings[i].tail = 0;
        rings[i].count = 0;
    }
}

void EpochTxnQueueShmemInit(uint32_t capacity)
{
    if (capacity == 0) {
        capacity = EPOCH_DEFAULT_TXN_QUEUE_CAPACITY;
    }
    bool found = false;
    g_epochTxnQueue = static_cast<EpochTxnQueue*>(
        ShmemInitStruct("Epoch Txn Queue", EpochTxnQueueShmemSize(capacity), &found));
    if (g_epochTxnQueue == nullptr) {
        return;
    }
    if (!found) {
        EpochTxnQueueInitHeader(capacity);
    } else if (g_epochTxnQueue->header.lock == nullptr) {
        if (g_epochTxnQueue->header.total_capacity == 0) {
            EpochTxnQueueInitHeader(capacity);
        } else {
            g_epochTxnQueue->header.lock = LWLockAssign(LWTRANCHE_WAL_INSERT);
        }
    }
}

static void EpochTxnEnsureRingLocked(EpochTxnEpochRing* ring, uint64_t epoch)
{
    if (ring->bound_epoch == epoch) {
        return;
    }
    ring->bound_epoch = epoch;
    ring->head = 0;
    ring->tail = 0;
    ring->count = 0;
}

bool EpochTxnQueuePush(uint64_t epoch, const char* data, uint32_t len)
{
    if (g_epochTxnQueue == nullptr || g_epochTxnQueue->header.lock == nullptr || data == nullptr || len == 0 ||
        len > EPOCH_MAX_WIRE_TXN_BYTES || epoch == 0) {
        return false;
    }
    LWLockAcquire(g_epochTxnQueue->header.lock, LW_EXCLUSIVE);
    uint32_t slot_idx = EpochTxnSlotIndex(epoch);
    EpochTxnEpochRing& ring = EpochTxnRings()[slot_idx];
    EpochTxnEnsureRingLocked(&ring, epoch);
    uint32_t spe = g_epochTxnQueue->header.slots_per_epoch;
    if (ring.count >= spe) {
        LWLockRelease(g_epochTxnQueue->header.lock);
        return false;
    }
    uint32_t item_idx = slot_idx * spe + ring.tail;
    EpochTxnQueueItem& item = EpochTxnItems()[item_idx];
    item.len = len;
    errno_t rc = memcpy_s(item.data, sizeof(item.data), data, len);
    securec_check(rc, "\0", "\0");
    ring.tail = (ring.tail + 1) % spe;
    ring.count++;
    LWLockRelease(g_epochTxnQueue->header.lock);
    return true;
}

static bool EpochTxnPopFromRingLocked(uint32_t slot_idx, char* data, uint32_t max_len, uint32_t* out_len,
    uint64_t* out_epoch)
{
    EpochTxnEpochRing& ring = EpochTxnRings()[slot_idx];
    if (ring.count == 0 || ring.bound_epoch == 0) {
        return false;
    }
    uint32_t spe = g_epochTxnQueue->header.slots_per_epoch;
    uint32_t item_idx = slot_idx * spe + ring.head;
    EpochTxnQueueItem& item = EpochTxnItems()[item_idx];
    if (item.len > max_len) {
        return false;
    }
    errno_t rc = memcpy_s(data, max_len, item.data, item.len);
    securec_check(rc, "\0", "\0");
    *out_len = item.len;
    if (out_epoch != nullptr) {
        *out_epoch = ring.bound_epoch;
    }
    ring.head = (ring.head + 1) % spe;
    ring.count--;
    return true;
}

bool EpochTxnQueuePop(uint64_t epoch, char* data, uint32_t max_len, uint32_t* out_len)
{
    if (g_epochTxnQueue == nullptr || g_epochTxnQueue->header.lock == nullptr || data == nullptr ||
        out_len == nullptr || epoch == 0) {
        return false;
    }
    LWLockAcquire(g_epochTxnQueue->header.lock, LW_EXCLUSIVE);
    uint32_t slot_idx = EpochTxnSlotIndex(epoch);
    EpochTxnEpochRing& ring = EpochTxnRings()[slot_idx];
    if (ring.bound_epoch != epoch) {
        LWLockRelease(g_epochTxnQueue->header.lock);
        return false;
    }
    bool ok = EpochTxnPopFromRingLocked(slot_idx, data, max_len, out_len, nullptr);
    LWLockRelease(g_epochTxnQueue->header.lock);
    return ok;
}

bool EpochTxnQueuePopAny(char* data, uint32_t max_len, uint32_t* out_len, uint64_t* out_epoch)
{
    if (g_epochTxnQueue == nullptr || g_epochTxnQueue->header.lock == nullptr || data == nullptr ||
        out_len == nullptr) {
        return false;
    }
    LWLockAcquire(g_epochTxnQueue->header.lock, LW_EXCLUSIVE);
    uint32_t n = g_epochTxnQueue->header.slot_count;
    if (n == 0) {
        LWLockRelease(g_epochTxnQueue->header.lock);
        return false;
    }
    uint32_t start = g_epochTxnDrainRr % n;
    g_epochTxnDrainRr++;
    for (uint32_t k = 0; k < n; ++k) {
        uint32_t i = (start + k) % n;
        if (EpochTxnPopFromRingLocked(i, data, max_len, out_len, out_epoch)) {
            LWLockRelease(g_epochTxnQueue->header.lock);
            return true;
        }
    }
    LWLockRelease(g_epochTxnQueue->header.lock);
    return false;
}

uint32_t EpochTxnQueueCount(void)
{
    if (g_epochTxnQueue == nullptr || g_epochTxnQueue->header.lock == nullptr) {
        return 0;
    }
    LWLockAcquire(g_epochTxnQueue->header.lock, LW_SHARED);
    uint32_t count = 0;
    uint32_t n = g_epochTxnQueue->header.slot_count;
    EpochTxnEpochRing* rings = EpochTxnRings();
    for (uint32_t i = 0; i < n; ++i) {
        count += rings[i].count;
    }
    LWLockRelease(g_epochTxnQueue->header.lock);
    return count;
}
