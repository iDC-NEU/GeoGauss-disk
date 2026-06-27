#include "postgres.h"

#include "epoch/e_abort_csn.h"
#include "epoch/e_config.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_shmem.h"

static uint32_t EpochAbortSlotIndex(uint64_t epochId)
{
    uint32_t slots = EpochGetCacheSlots();
    if (slots == 0) {
        slots = EPOCH_DEFAULT_CACHE_SLOTS;
    }
    return static_cast<uint32_t>(epochId % slots);
}

static EpochAbortCsnBank* EpochAbortBank(void)
{
    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr) {
        return nullptr;
    }
    return &state->abort_csns;
}

static EpochAbortCsnSlot* EpochAbortSlotForEpoch(uint64_t epochId, bool bind)
{
    EpochAbortCsnBank* bank = EpochAbortBank();
    if (bank == nullptr) {
        return nullptr;
    }
    uint32_t slot = EpochAbortSlotIndex(epochId);
    if (slot >= EPOCH_DEFAULT_CACHE_SLOTS) {
        return nullptr;
    }
    if (bind) {
        E_NodeManager::EnsureEpochSlot(epochId);
    }
    return &bank->slots[slot];
}

void EpochAbortCsnBankInit(EpochAbortCsnBank* bank)
{
    if (bank == nullptr) {
        return;
    }
    for (uint32_t slot = 0; slot < EPOCH_DEFAULT_CACHE_SLOTS; ++slot) {
        bank->slots[slot].count.store(0, std::memory_order_relaxed);
        for (uint32_t i = 0; i < EPOCH_ABORT_CSNS_PER_SLOT; ++i) {
            bank->slots[slot].entries[i].csn.store(0, std::memory_order_relaxed);
            bank->slots[slot].entries[i].server_id.store(0, std::memory_order_relaxed);
        }
    }
}

void EpochAbortCsnClear(uint64_t epochId)
{
    EpochAbortCsnSlot* slot = EpochAbortSlotForEpoch(epochId, false);
    if (slot == nullptr) {
        return;
    }
    uint32_t n = slot->count.load(std::memory_order_acquire);
    if (n > EPOCH_ABORT_CSNS_PER_SLOT) {
        n = EPOCH_ABORT_CSNS_PER_SLOT;
    }
    for (uint32_t i = 0; i < n; ++i) {
        slot->entries[i].csn.store(0, std::memory_order_relaxed);
        slot->entries[i].server_id.store(0, std::memory_order_relaxed);
    }
    slot->count.store(0, std::memory_order_release);
}

void EpochAbortCsnInsert(uint64_t epochId, uint64_t csn, uint32_t serverId)
{
    if (csn == 0) {
        return;
    }
    if (EpochAbortCsnContains(epochId, csn, serverId)) {
        return;
    }
    EpochAbortCsnSlot* slot = EpochAbortSlotForEpoch(epochId, true);
    if (slot == nullptr) {
        return;
    }
    uint32_t idx = slot->count.fetch_add(1, std::memory_order_acq_rel);
    if (idx >= EPOCH_ABORT_CSNS_PER_SLOT) {
        ereport(WARNING,
            (errmsg("epoch: abort_csn_set overflow for epoch %lu (csn %lu node %u)",
                epochId,
                csn,
                serverId)));
        return;
    }
    slot->entries[idx].csn.store(csn, std::memory_order_release);
    slot->entries[idx].server_id.store(serverId, std::memory_order_release);
}

bool EpochAbortCsnContains(uint64_t epochId, uint64_t csn, uint32_t serverId)
{
    if (csn == 0) {
        return false;
    }
    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr) {
        return false;
    }
    uint32_t slot_idx = EpochAbortSlotIndex(epochId);
    if (slot_idx >= EPOCH_DEFAULT_CACHE_SLOTS) {
        return false;
    }
    if (state->counters.slot_epoch[slot_idx].load(std::memory_order_acquire) != epochId) {
        return false;
    }
    EpochAbortCsnSlot* slot = &state->abort_csns.slots[slot_idx];
    uint32_t n = slot->count.load(std::memory_order_acquire);
    if (n > EPOCH_ABORT_CSNS_PER_SLOT) {
        n = EPOCH_ABORT_CSNS_PER_SLOT;
    }
    for (uint32_t i = 0; i < n; ++i) {
        uint64_t ec = slot->entries[i].csn.load(std::memory_order_acquire);
        uint32_t es = slot->entries[i].server_id.load(std::memory_order_acquire);
        if (ec == csn && es == serverId) {
            return true;
        }
    }
    return false;
}
