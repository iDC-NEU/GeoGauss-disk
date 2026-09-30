#include "postgres.h"

#include "epoch/e_node_manager.h"

#include "epoch/e_config.h"
#include "epoch/e_abort_csn.h"
#include "epoch/e_shmem.h"
#include "epoch/e_statistician.h"

static EpochSharedState* EpochState(void)
{
    return EpochGetSharedState();
}

static bool EpochSlotIsCurrent(EpochSharedState* state, uint64_t epochId, uint32_t slot)
{
    if (state == nullptr) {
        return false;
    }
    return state->counters.slot_epoch[slot].load(std::memory_order_acquire) == epochId;
}

static void ClearEpochSlotCells(EpochSharedState* state, uint32_t slot)
{
    for (uint32_t node = 0; node < EPOCH_MAX_NODES; ++node) {
        state->counters.change_set_txn[slot][node].store(0, std::memory_order_relaxed);
        state->counters.executing_txn[slot][node].store(0, std::memory_order_relaxed);
        state->counters.local_pack_txn[slot][node].store(0, std::memory_order_relaxed);
        state->counters.sealed_pack_txn[slot][node].store(0, std::memory_order_relaxed);
        state->counters.abort_txn[slot][node].store(0, std::memory_order_relaxed);
        state->counters.write_abort_after_send_txn[slot][node].store(0, std::memory_order_relaxed);
        state->counters.store_should_receive_pack[slot][node].store(0, std::memory_order_relaxed);
        state->counters.store_received_pack[slot][node].store(0, std::memory_order_relaxed);
        state->counters.wire_merge_done[slot][node].store(0, std::memory_order_relaxed);
        state->counters.merged_txn[slot][node].store(0, std::memory_order_relaxed);
        state->counters.committed_txn[slot][node].store(0, std::memory_order_relaxed);
        state->counters.node_send_completed[slot][node].store(false, std::memory_order_relaxed);
        state->counters.node_receive_completed[slot][node].store(false, std::memory_order_relaxed);
    }
}

void E_NodeManager::EnsureEpochSlot(uint64_t epochId)
{
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return;
    }
    uint32_t slot = GetSlot(epochId);
    for (;;) {
        uint64_t tagged = state->counters.slot_epoch[slot].load(std::memory_order_acquire);
        if (tagged == epochId) {
            return;
        }
        if (tagged == 0) {
            if (state->counters.slot_epoch[slot].compare_exchange_weak(
                    tagged, epochId, std::memory_order_acq_rel, std::memory_order_acquire)) {
                EpochRestorePastEpochPeerFlags(epochId);
                return;
            }
            continue;
        }
        /*
         * 单赢家 reclaim：避免并发 ClearEpochSlot 把 executing 等计数清零两次，
         * 导致 inc 丢失而 session 仍 exec_counted=1。
         */
        if (state->counters.slot_epoch[slot].compare_exchange_weak(
                tagged, 0, std::memory_order_acq_rel, std::memory_order_acquire)) {
            ClearEpochSlotCells(state, slot);
            state->counters.slot_epoch[slot].store(epochId, std::memory_order_release);
            EpochAbortCsnClear(epochId);
            EpochRestorePastEpochPeerFlags(epochId);
            return;
        }
    }
}

uint32_t E_NodeManager::GetSlot(uint64_t epochId)
{
    uint32_t slots = EpochGetCacheSlots();
    if (slots == 0) {
        return 0;
    }
    return static_cast<uint32_t>(epochId % slots);
}

size_t E_NodeManager::GetIndex(uint64_t epochId, uint64_t nodeId)
{
    uint32_t slot = GetSlot(epochId);
    uint32_t node = static_cast<uint32_t>(nodeId);
    if (node >= EPOCH_MAX_NODES) {
        node = EPOCH_MAX_NODE_ID;
    }
    return static_cast<size_t>(slot) * EPOCH_MAX_NODES + node;
}

uint64_t E_NodeManager::GetPhysicalEpoch(void)
{
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return 1;
    }
    return state->physical_epoch.load(std::memory_order_acquire);
}

uint64_t E_NodeManager::AddPhysicalEpoch(void)
{
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return 1;
    }
    return state->physical_epoch.fetch_add(1, std::memory_order_acq_rel) + 1;
}

void E_NodeManager::SetPhysicalEpoch(uint64_t value)
{
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return;
    }
    state->physical_epoch.store(value, std::memory_order_release);
}

uint64_t E_NodeManager::GetLogicalEpoch(void)
{
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return 0;
    }
    return state->logical_epoch.load(std::memory_order_acquire);
}

uint64_t E_NodeManager::AddLogicalEpoch(void)
{
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return 0;
    }
    return state->logical_epoch.fetch_add(1, std::memory_order_acq_rel) + 1;
}

void E_NodeManager::SetLogicalEpoch(uint64_t value)
{
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return;
    }
    state->logical_epoch.store(value, std::memory_order_release);
}

void E_NodeManager::SetStoreShouldReceivedPackCounters(uint64_t epochId, uint64_t nodeId, uint64_t value)
{
    EnsureEpochSlot(epochId);
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return;
    }
    uint32_t slot = GetSlot(epochId);
    uint32_t node = static_cast<uint32_t>(nodeId);
    if (node >= EPOCH_MAX_NODES) {
        node = EPOCH_MAX_NODE_ID;
    }
    state->counters.store_should_receive_pack[slot][node].store(value, std::memory_order_release);
}

void E_NodeManager::SetStoreReceivedPackCounters(uint64_t epochId, uint64_t nodeId, uint64_t value)
{
    EnsureEpochSlot(epochId);
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return;
    }
    uint32_t slot = GetSlot(epochId);
    uint32_t node = static_cast<uint32_t>(nodeId);
    if (node >= EPOCH_MAX_NODES) {
        node = EPOCH_MAX_NODE_ID;
    }
    state->counters.store_received_pack[slot][node].store(value, std::memory_order_release);
}

void E_NodeManager::SetLocalPackTxnCounters(uint64_t epochId, uint64_t nodeId, uint64_t value)
{
    EnsureEpochSlot(epochId);
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return;
    }
    uint32_t slot = GetSlot(epochId);
    uint32_t node = static_cast<uint32_t>(nodeId);
    if (node >= EPOCH_MAX_NODES) {
        node = EPOCH_MAX_NODE_ID;
    }
    state->counters.local_pack_txn[slot][node].store(value, std::memory_order_release);
}

#define EPOCH_COUNTER_OP(name, field, op, extra)                                           \
    uint64_t E_NodeManager::name(uint64_t epochId, uint64_t nodeId) extra                  \
    {                                                                                      \
        EpochSharedState* state = EpochState();                                            \
        if (state == nullptr) {                                                            \
            return 0;                                                                      \
        }                                                                                  \
        uint32_t slot = GetSlot(epochId);                                                  \
        if (!EpochSlotIsCurrent(state, epochId, slot)) {                                   \
            return 0;                                                                      \
        }                                                                                  \
        uint32_t node = static_cast<uint32_t>(nodeId);                                     \
        if (node >= EPOCH_MAX_NODES) {                                                     \
            node = EPOCH_MAX_NODE_ID;                                                      \
        }                                                                                  \
        return state->counters.field[slot][node] op;                                       \
    }

#define EPOCH_COUNTER_INC(name, field)                                                     \
    uint64_t E_NodeManager::name(uint64_t epochId, uint64_t nodeId)                        \
    {                                                                                      \
        EnsureEpochSlot(epochId);                                                        \
        EpochSharedState* state = EpochState();                                            \
        if (state == nullptr) {                                                            \
            return 0;                                                                      \
        }                                                                                  \
        uint32_t slot = GetSlot(epochId);                                                  \
        uint32_t node = static_cast<uint32_t>(nodeId);                                     \
        if (node >= EPOCH_MAX_NODES) {                                                     \
            node = EPOCH_MAX_NODE_ID;                                                      \
        }                                                                                  \
        return state->counters.field[slot][node].fetch_add(1, std::memory_order_acq_rel) + 1; \
    }

#define EPOCH_COUNTER_DEC(name, field)                                                     \
    uint64_t E_NodeManager::name(uint64_t epochId, uint64_t nodeId)                        \
    {                                                                                      \
        EnsureEpochSlot(epochId);                                                          \
        EpochSharedState* state = EpochState();                                            \
        if (state == nullptr) {                                                            \
            return 0;                                                                      \
        }                                                                                  \
        uint32_t slot = GetSlot(epochId);                                                  \
        uint32_t node = static_cast<uint32_t>(nodeId);                                     \
        if (node >= EPOCH_MAX_NODES) {                                                     \
            node = EPOCH_MAX_NODE_ID;                                                      \
        }                                                                                  \
        uint64_t prev = state->counters.field[slot][node].load(std::memory_order_acquire); \
        if (prev == 0) {                                                                   \
            return 0;                                                                      \
        }                                                                                  \
        return state->counters.field[slot][node].fetch_sub(1, std::memory_order_acq_rel) - 1; \
    }

EPOCH_COUNTER_INC(IncChangeSetTxnCounters, change_set_txn)
EPOCH_COUNTER_DEC(DecChangeSetTxnCounters, change_set_txn)
EPOCH_COUNTER_OP(GetChangeSetTxnCounters, change_set_txn, .load(std::memory_order_acquire), )

uint64_t E_NodeManager::IncExecutingTxnCounters(uint64_t epochId, uint64_t nodeId)
{
    EnsureEpochSlot(epochId);
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return 0;
    }
    uint32_t slot = GetSlot(epochId);
    if (!EpochSlotIsCurrent(state, epochId, slot)) {
        return 0;
    }
    uint32_t node = static_cast<uint32_t>(nodeId);
    if (node >= EPOCH_MAX_NODES) {
        node = EPOCH_MAX_NODE_ID;
    }
    return state->counters.executing_txn[slot][node].fetch_add(1, std::memory_order_acq_rel) + 1;
}

bool E_NodeManager::DecExecutingTxnCounters(uint64_t epochId, uint64_t nodeId)
{
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return false;
    }
    uint32_t slot = GetSlot(epochId);
    if (!EpochSlotIsCurrent(state, epochId, slot)) {
        return false;
    }
    uint32_t node = static_cast<uint32_t>(nodeId);
    if (node >= EPOCH_MAX_NODES) {
        node = EPOCH_MAX_NODE_ID;
    }
    std::atomic<uint64_t>* counter = &state->counters.executing_txn[slot][node];
    for (;;) {
        uint64_t prev = counter->load(std::memory_order_acquire);
        if (prev == 0) {
            return false;
        }
        if (counter->compare_exchange_weak(prev, prev - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return true;
        }
    }
}

EPOCH_COUNTER_OP(GetExecutingTxnCounters, executing_txn, .load(std::memory_order_acquire), )

EPOCH_COUNTER_INC(IncLocalPackTxnCounters, local_pack_txn)
EPOCH_COUNTER_DEC(DecLocalPackTxnCounters, local_pack_txn)
EPOCH_COUNTER_OP(GetLocalPackTxnCounters, local_pack_txn, .load(std::memory_order_acquire), )

uint64_t E_NodeManager::GetSealedPackTxnCounters(uint64_t epochId, uint64_t nodeId)
{
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return 0;
    }
    uint32_t slot = GetSlot(epochId);
    uint32_t node = static_cast<uint32_t>(nodeId);
    if (node >= EPOCH_MAX_NODES) {
        node = EPOCH_MAX_NODE_ID;
    }
    return state->counters.sealed_pack_txn[slot][node].load(std::memory_order_acquire);
}

void E_NodeManager::SetSealedPackTxnCounters(uint64_t epochId, uint64_t nodeId, uint64_t value)
{
    EnsureEpochSlot(epochId);
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return;
    }
    uint32_t slot = GetSlot(epochId);
    uint32_t node = static_cast<uint32_t>(nodeId);
    if (node >= EPOCH_MAX_NODES) {
        node = EPOCH_MAX_NODE_ID;
    }
    state->counters.sealed_pack_txn[slot][node].store(value, std::memory_order_release);
}

EPOCH_COUNTER_INC(IncAbortTxnCounters, abort_txn)
EPOCH_COUNTER_OP(GetAbortTxnCounters, abort_txn, .load(std::memory_order_acquire), )

EPOCH_COUNTER_INC(IncWriteAbortAfterSendTxnCounters, write_abort_after_send_txn)
EPOCH_COUNTER_OP(GetWriteAbortAfterSendTxnCounters, write_abort_after_send_txn, .load(std::memory_order_acquire), )

uint64_t E_NodeManager::UpdateStoreShouldReceivedPackCounters(uint64_t epochId, uint64_t nodeId, uint64_t increment)
{
    EnsureEpochSlot(epochId);
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return 0;
    }
    uint32_t slot = GetSlot(epochId);
    uint32_t node = static_cast<uint32_t>(nodeId);
    if (node >= EPOCH_MAX_NODES) {
        node = EPOCH_MAX_NODE_ID;
    }
    return state->counters.store_should_receive_pack[slot][node].fetch_add(increment, std::memory_order_acq_rel) +
           increment;
}

EPOCH_COUNTER_OP(GetStoreShouldReceivedPackCounters, store_should_receive_pack, .load(std::memory_order_acquire), )

uint64_t E_NodeManager::UpdateStoreReceivedPackCounters(uint64_t epochId, uint64_t nodeId, uint64_t increment)
{
    EnsureEpochSlot(epochId);
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return 0;
    }
    uint32_t slot = GetSlot(epochId);
    uint32_t node = static_cast<uint32_t>(nodeId);
    if (node >= EPOCH_MAX_NODES) {
        node = EPOCH_MAX_NODE_ID;
    }
    return state->counters.store_received_pack[slot][node].fetch_add(increment, std::memory_order_acq_rel) + increment;
}

EPOCH_COUNTER_OP(GetStoreReceivedPackCounters, store_received_pack, .load(std::memory_order_acquire), )

void E_NodeManager::SetNodeSendCompleted(uint64_t epochId, uint64_t nodeId, bool completed)
{
    EnsureEpochSlot(epochId);
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return;
    }
    uint32_t slot = GetSlot(epochId);
    uint32_t node = static_cast<uint32_t>(nodeId);
    if (node >= EPOCH_MAX_NODES) {
        node = EPOCH_MAX_NODE_ID;
    }
    state->counters.node_send_completed[slot][node].store(completed, std::memory_order_release);
}

bool E_NodeManager::IsNodeSendCompleted(uint64_t epochId, uint64_t nodeId)
{
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return false;
    }
    uint32_t slot = GetSlot(epochId);
    if (!EpochSlotIsCurrent(state, epochId, slot)) {
        /*
         * ClearEpochSlot 后 slot_epoch=0；logical 已越过该 epoch 表示当时 send/recv 已齐。
         * 否则 IsNodeReceiveCompleted(1) 在 logical=5000+ 时恒 false，TXN_REQ 会死盯 epoch=1。
         */
        if (epochId < GetLogicalEpoch()) {
            return true;
        }
        return false;
    }
    uint32_t node = static_cast<uint32_t>(nodeId);
    if (node >= EPOCH_MAX_NODES) {
        node = EPOCH_MAX_NODE_ID;
    }
    return state->counters.node_send_completed[slot][node].load(std::memory_order_acquire);
}

void E_NodeManager::SetNodeReceiveCompleted(uint64_t epochId, uint64_t nodeId, bool completed)
{
    EnsureEpochSlot(epochId);
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return;
    }
    uint32_t slot = GetSlot(epochId);
    uint32_t node = static_cast<uint32_t>(nodeId);
    if (node >= EPOCH_MAX_NODES) {
        node = EPOCH_MAX_NODE_ID;
    }
    state->counters.node_receive_completed[slot][node].store(completed, std::memory_order_release);
}

bool E_NodeManager::IsNodeReceiveCompleted(uint64_t epochId, uint64_t nodeId)
{
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return false;
    }
    uint32_t slot = GetSlot(epochId);
    if (!EpochSlotIsCurrent(state, epochId, slot)) {
        if (epochId < GetLogicalEpoch()) {
            return true;
        }
        return false;
    }
    uint32_t node = static_cast<uint32_t>(nodeId);
    if (node >= EPOCH_MAX_NODES) {
        node = EPOCH_MAX_NODE_ID;
    }
    return state->counters.node_receive_completed[slot][node].load(std::memory_order_acquire);
}

uint64_t E_NodeManager::IncWireMergeDoneCounters(uint64_t epochId, uint64_t nodeId)
{
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return 0;
    }
    uint32_t slot = GetSlot(epochId);
    if (!EpochSlotIsCurrent(state, epochId, slot)) {
        return 0;
    }
    uint32_t node = static_cast<uint32_t>(nodeId);
    if (node >= EPOCH_MAX_NODES) {
        node = EPOCH_MAX_NODE_ID;
    }
    return state->counters.wire_merge_done[slot][node].fetch_add(1, std::memory_order_acq_rel) + 1;
}

EPOCH_COUNTER_OP(GetWireMergeDoneCounters, wire_merge_done, .load(std::memory_order_acquire), )
EPOCH_COUNTER_INC(IncMergedTxnCounters, merged_txn)
EPOCH_COUNTER_DEC(DecMergedTxnCounters, merged_txn)
EPOCH_COUNTER_OP(GetMergedTxnCounters, merged_txn, .load(std::memory_order_acquire), )

void E_NodeManager::SetMergedTxnCounters(uint64_t epochId, uint64_t nodeId, uint64_t value)
{
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return;
    }
    EnsureEpochSlot(epochId);
    uint32_t slot = GetSlot(epochId);
    uint32_t node = static_cast<uint32_t>(nodeId);
    if (node >= EPOCH_MAX_NODES) {
        node = EPOCH_MAX_NODE_ID;
    }
    state->counters.merged_txn[slot][node].store(value, std::memory_order_release);
}
EPOCH_COUNTER_INC(IncCommittedTxnCounters, committed_txn)
EPOCH_COUNTER_OP(GetCommittedTxnCounters, committed_txn, .load(std::memory_order_acquire), )

void E_NodeManager::ResetEpochCounters(uint64_t epochId)
{
    ClearEpochSlot(epochId);
}

void E_NodeManager::ClearEpochSlot(uint64_t epochId)
{
    EpochSharedState* state = EpochState();
    if (state == nullptr) {
        return;
    }
    uint32_t slot = GetSlot(epochId);
    ClearEpochSlotCells(state, slot);
    state->counters.slot_epoch[slot].store(0, std::memory_order_release);
    EpochAbortCsnClear(epochId);
}

#undef EPOCH_COUNTER_OP
