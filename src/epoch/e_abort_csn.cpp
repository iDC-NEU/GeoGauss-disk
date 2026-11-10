#include "postgres.h"

#include "epoch/e_abort_csn.h"
#include "epoch/e_config.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_shmem.h"
#include "epoch/e_statistician.h"

#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/spin.h"

#include <cstring>

struct EpochAbortCsnSlotHeader {
    slock_t lock;
    std::atomic<uint32_t> count;
};

struct EpochAbortCsnBankHeader {
    uint32_t cache_slot_count;
    uint32_t entries_per_slot;
};

static constexpr const char* EPOCH_ABORT_CSN_SHMEM_NAME = "Epoch Abort CSN Bank";

static EpochAbortCsnBankHeader* g_epochAbortCsnBank = nullptr;

static uint32_t EpochAbortSlotIndex(uint64_t epochId)
{
    uint32_t slots = EpochGetCacheSlots();
    if (slots == 0) {
        slots = EPOCH_DEFAULT_CACHE_SLOTS;
    }
    return static_cast<uint32_t>(epochId % slots);
}

static size_t EpochAbortCsnSlotStride(uint32_t entries_per_slot)
{
    return sizeof(EpochAbortCsnSlotHeader) + static_cast<size_t>(entries_per_slot) * sizeof(EpochAbortCsnEntry);
}

static EpochAbortCsnSlotHeader* EpochAbortCsnSlotHeaderAt(uint32_t slot_idx)
{
    if (g_epochAbortCsnBank == nullptr) {
        return nullptr;
    }
    char* base = reinterpret_cast<char*>(g_epochAbortCsnBank + 1);
    return reinterpret_cast<EpochAbortCsnSlotHeader*>(base + static_cast<size_t>(slot_idx) *
                                                                 EpochAbortCsnSlotStride(
                                                                     g_epochAbortCsnBank->entries_per_slot));
}

static EpochAbortCsnEntry* EpochAbortCsnSlotEntries(EpochAbortCsnSlotHeader* slot_hdr)
{
    return reinterpret_cast<EpochAbortCsnEntry*>(slot_hdr + 1);
}

Size EpochAbortCsnShmemSize(void)
{
    uint32_t cache_slots = EpochGetCacheSlots();
    uint32_t entries_per_epoch = EpochGetAbortCsnPerEpoch();
    size_t slots_bytes = static_cast<size_t>(cache_slots) * EpochAbortCsnSlotStride(entries_per_epoch);
    return sizeof(EpochAbortCsnBankHeader) + slots_bytes;
}

static void EpochAbortCsnResetSlot(uint32_t slot_idx)
{
    EpochAbortCsnSlotHeader* slot_hdr = EpochAbortCsnSlotHeaderAt(slot_idx);
    if (slot_hdr == nullptr || g_epochAbortCsnBank == nullptr) {
        return;
    }
    SpinLockInit(&slot_hdr->lock);
    uint32_t capacity = g_epochAbortCsnBank->entries_per_slot;
    EpochAbortCsnEntry* entries = EpochAbortCsnSlotEntries(slot_hdr);
    for (uint32_t i = 0; i < capacity; ++i) {
        entries[i].csn = 0;
        entries[i].server_id = 0;
    }
    slot_hdr->count.store(0, std::memory_order_release);
}

static void EpochAbortCsnBankInitLocked(void)
{
    if (g_epochAbortCsnBank == nullptr) {
        return;
    }
    for (uint32_t slot = 0; slot < g_epochAbortCsnBank->cache_slot_count; ++slot) {
        EpochAbortCsnResetSlot(slot);
    }
}

void EpochAbortCsnShmemInit(void)
{
    bool found = false;
    g_epochAbortCsnBank = static_cast<EpochAbortCsnBankHeader*>(
        ShmemInitStruct(EPOCH_ABORT_CSN_SHMEM_NAME, EpochAbortCsnShmemSize(), &found));
    if (g_epochAbortCsnBank == nullptr) {
        return;
    }
    if (!found) {
        g_epochAbortCsnBank->cache_slot_count = EpochGetCacheSlots();
        g_epochAbortCsnBank->entries_per_slot = EpochGetAbortCsnPerEpoch();
        EpochAbortCsnBankInitLocked();
    }
}

/*
 * 统计某时间片被判输的事务数（abort csn 的条数）。
 * 一个 csn 代表一个事务，所以这个数就是该时间片的 merge abort 事务数。
 * 调用方必须在 EpochAbortCsnClear 之前取（apply 推进时取，清理在其后）。
 */
uint64_t EpochAbortCsnCount(uint64_t epochId)
{
    if (g_epochAbortCsnBank == nullptr) {
        return 0;
    }
    EpochAbortCsnSlotHeader* slot_hdr = EpochAbortCsnSlotHeaderAt(EpochAbortSlotIndex(epochId));
    if (slot_hdr == nullptr) {
        return 0;
    }
    return static_cast<uint64_t>(slot_hdr->count.load(std::memory_order_acquire));
}

void EpochAbortCsnClear(uint64_t epochId)
{
    if (g_epochAbortCsnBank == nullptr) {
        return;
    }
    EpochAbortCsnResetSlot(EpochAbortSlotIndex(epochId));
}

/*
 * 把一个事务标记为输家。
 * 返回 true 表示本次是新插入（之前没有），调用方据此只统计一次，
 * 避免同一笔事务因重复投递等原因被重复计入失败率。
 */
bool EpochAbortCsnInsert(uint64_t epochId, uint64_t csn, uint32_t serverId)
{
    if (csn == 0 || g_epochAbortCsnBank == nullptr) {
        return false;
    }
    if (EpochAbortCsnContains(epochId, csn, serverId)) {
        return false;
    }

    E_NodeManager::EnsureEpochSlot(epochId);
    uint32_t slot_idx = EpochAbortSlotIndex(epochId);
    EpochAbortCsnSlotHeader* slot_hdr = EpochAbortCsnSlotHeaderAt(slot_idx);
    if (slot_hdr == nullptr) {
        return false;
    }

    SpinLockAcquire(&slot_hdr->lock);
    uint32_t idx = slot_hdr->count.load(std::memory_order_relaxed);
    if (idx >= g_epochAbortCsnBank->entries_per_slot) {
        SpinLockRelease(&slot_hdr->lock);
        ereport(WARNING,
            (errmsg("epoch: abort_csn_set overflow for epoch %lu (csn %lu node %u, capacity %u)",
                epochId,
                csn,
                serverId,
                g_epochAbortCsnBank->entries_per_slot)));
        return false;
    }
    EpochAbortCsnEntry* entries = EpochAbortCsnSlotEntries(slot_hdr);
    entries[idx].csn = csn;
    entries[idx].server_id = serverId;
    slot_hdr->count.store(idx + 1, std::memory_order_release);
    SpinLockRelease(&slot_hdr->lock);
    return true;
}

bool EpochAbortCsnContains(uint64_t epochId, uint64_t csn, uint32_t serverId)
{
    if (csn == 0 || g_epochAbortCsnBank == nullptr) {
        return false;
    }

    uint32_t slot_idx = EpochAbortSlotIndex(epochId);
    EpochSharedState* state = EpochGetSharedState();
    if (state != nullptr &&
        state->counters.slot_epoch[slot_idx].load(std::memory_order_acquire) != epochId) {
        return false;
    }

    EpochAbortCsnSlotHeader* slot_hdr = EpochAbortCsnSlotHeaderAt(slot_idx);
    if (slot_hdr == nullptr) {
        return false;
    }

    SpinLockAcquire(&slot_hdr->lock);
    uint32_t n = slot_hdr->count.load(std::memory_order_acquire);
    if (n > g_epochAbortCsnBank->entries_per_slot) {
        n = g_epochAbortCsnBank->entries_per_slot;
    }
    EpochAbortCsnEntry* entries = EpochAbortCsnSlotEntries(slot_hdr);
    for (uint32_t i = 0; i < n; ++i) {
        if (entries[i].csn == csn && entries[i].server_id == serverId) {
            SpinLockRelease(&slot_hdr->lock);
            return true;
        }
    }
    SpinLockRelease(&slot_hdr->lock);
    return false;
}
