#include "postgres.h"

#include "epoch/e_txn_queue.h"
#include "epoch/e_config.h"
#include "epoch/e_txn_wire.h"

#include "storage/ipc.h"
#include "storage/shmem.h"

struct EpochTxnQueueSlot {
    uint32_t len;
    char data[EPOCH_MAX_WIRE_TXN_BYTES];
};

struct EpochTxnQueue {
    EpochTxnQueueHeader header;
    EpochTxnQueueSlot slots[1]; /* flexible array via shmem sizing */
};

static EpochTxnQueue* g_epochTxnQueue = nullptr;
static uint32_t g_epochTxnQueueCapacity = 4096;

Size EpochTxnQueueShmemSize(uint32_t capacity)
{
    if (capacity == 0) {
        capacity = 4096;
    }
    return offsetof(EpochTxnQueue, slots) + sizeof(EpochTxnQueueSlot) * capacity;
}

static void EpochTxnQueueInitHeader(uint32_t capacity)
{
    g_epochTxnQueue->header.capacity = capacity;
    g_epochTxnQueue->header.head = 0;
    g_epochTxnQueue->header.tail = 0;
    g_epochTxnQueue->header.count = 0;
    g_epochTxnQueue->header.lock = LWLockAssign(LWTRANCHE_WAL_INSERT);
}

void EpochTxnQueueShmemInit(uint32_t capacity)
{
    if (capacity == 0) {
        capacity = 4096;
    }
    g_epochTxnQueueCapacity = capacity;
    bool found = false;
    g_epochTxnQueue = static_cast<EpochTxnQueue*>(
        ShmemInitStruct("Epoch Txn Queue", EpochTxnQueueShmemSize(capacity), &found));
    if (g_epochTxnQueue == nullptr) {
        return;
    }
    if (!found) {
        EpochTxnQueueInitHeader(capacity);
    } else if (g_epochTxnQueue->header.lock == nullptr) {
        /* epoch worker 线程 reattach 时 found=true 但 lock 未初始化 */
        if (g_epochTxnQueue->header.capacity == 0) {
            EpochTxnQueueInitHeader(capacity);
        } else {
            g_epochTxnQueue->header.lock = LWLockAssign(LWTRANCHE_WAL_INSERT);
        }
    }
}

bool EpochTxnQueuePush(const char* data, uint32_t len)
{
    if (g_epochTxnQueue == nullptr || g_epochTxnQueue->header.lock == nullptr || data == nullptr || len == 0 ||
        len > EPOCH_MAX_WIRE_TXN_BYTES) {
        return false;
    }
    LWLockAcquire(g_epochTxnQueue->header.lock, LW_EXCLUSIVE);
    if (g_epochTxnQueue->header.count >= g_epochTxnQueue->header.capacity) {
        LWLockRelease(g_epochTxnQueue->header.lock);
        return false;
    }
    EpochTxnQueueSlot& slot = g_epochTxnQueue->slots[g_epochTxnQueue->header.tail];
    slot.len = len;
    errno_t rc = memcpy_s(slot.data, sizeof(slot.data), data, len);
    securec_check(rc, "\0", "\0");
    g_epochTxnQueue->header.tail = (g_epochTxnQueue->header.tail + 1) % g_epochTxnQueue->header.capacity;
    g_epochTxnQueue->header.count++;
    LWLockRelease(g_epochTxnQueue->header.lock);
    return true;
}

bool EpochTxnQueuePop(char* data, uint32_t max_len, uint32_t* out_len)
{
    if (g_epochTxnQueue == nullptr || g_epochTxnQueue->header.lock == nullptr || data == nullptr ||
        out_len == nullptr) {
        return false;
    }
    LWLockAcquire(g_epochTxnQueue->header.lock, LW_EXCLUSIVE);
    if (g_epochTxnQueue->header.count == 0) {
        LWLockRelease(g_epochTxnQueue->header.lock);
        return false;
    }
    EpochTxnQueueSlot& slot = g_epochTxnQueue->slots[g_epochTxnQueue->header.head];
    if (slot.len > max_len) {
        LWLockRelease(g_epochTxnQueue->header.lock);
        return false;
    }
    errno_t rc = memcpy_s(data, max_len, slot.data, slot.len);
    securec_check(rc, "\0", "\0");
    *out_len = slot.len;
    g_epochTxnQueue->header.head = (g_epochTxnQueue->header.head + 1) % g_epochTxnQueue->header.capacity;
    g_epochTxnQueue->header.count--;
    LWLockRelease(g_epochTxnQueue->header.lock);
    return true;
}

uint32_t EpochTxnQueueCount(void)
{
    if (g_epochTxnQueue == nullptr) {
        return 0;
    }
    LWLockAcquire(g_epochTxnQueue->header.lock, LW_SHARED);
    uint32_t count = g_epochTxnQueue->header.count;
    LWLockRelease(g_epochTxnQueue->header.lock);
    return count;
}
