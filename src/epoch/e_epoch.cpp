#include "postgres.h"

#include "epoch/e_epoch.h"
#include "epoch/e_net.h"

#include "epoch/e_config.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_shmem.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_merge_pool.h"
#include "epoch/e_epoch_lifecycle.h"
#include "epoch/e_statistician.h"
#include "epoch/e_pacer.h"
#include "epoch/e_perf.h"

#include <unordered_map>

#include "miscadmin.h"

#include "portability/instr_time.h"

static void EpochMaybeWriteOffStaleChangeSet(uint64_t epoch);

/* physical assign 计数（含 since=0）连续不变超过该时长，视为无客户端，允许未满 limit 也推进 physical */
static constexpr long EPOCH_PHYSICAL_TXN_IDLE_MS = 1000;

static uint64_t g_physical_txn_idle_last_since = UINT64_MAX;
static instr_time g_physical_txn_idle_last_change;
static bool g_physical_txn_idle_tracker_inited = false;

static bool EpochPhysicalTxnAssignIdleReady(uint64_t since, uint64_t limit)
{
    if (since >= limit) {
        g_physical_txn_idle_last_since = since;
        INSTR_TIME_SET_CURRENT(g_physical_txn_idle_last_change);
        g_physical_txn_idle_tracker_inited = true;
        return false;
    }

    instr_time now;
    INSTR_TIME_SET_CURRENT(now);
    if (!g_physical_txn_idle_tracker_inited || since != g_physical_txn_idle_last_since) {
        g_physical_txn_idle_last_since = since;
        g_physical_txn_idle_last_change = now;
        g_physical_txn_idle_tracker_inited = true;
        return false;
    }

    instr_time delta = now;
    INSTR_TIME_SUBTRACT(delta, g_physical_txn_idle_last_change);
    return INSTR_TIME_GET_MILLISEC(delta) >= EPOCH_PHYSICAL_TXN_IDLE_MS;
}

static void EpochPhysicalTxnIdleTrackerReset(void)
{
    g_physical_txn_idle_last_since = UINT64_MAX;
    g_physical_txn_idle_tracker_inited = false;
}

void EpochAdvancePhysicalOnce(void)
{
    if (!g_enable_epoch_multimaster) {
        return;
    }

    if (EpochGetTotalNodes() > 1) {
        uint64_t physical = E_NodeManager::GetPhysicalEpoch();
        uint64_t logical = E_NodeManager::GetLogicalEpoch();
        uint64_t gap = (physical > logical) ? (physical - logical) : 0;
        if (gap >= EpochGetPhysLogicalGapLimit()) {
            return;
        }
        if (EpochLogicalEpochHasPendingPack(logical)) {
            return;
        }
    }

    if (g_epoch_physical_txns_per_epoch > 0) {
        EpochSharedState* state = EpochGetSharedState();
        if (state == nullptr) {
            return;
        }
        const uint64_t txn_limit = static_cast<uint64_t>(g_epoch_physical_txns_per_epoch);
        const uint64_t since = state->physical_txn_since_advance.load(std::memory_order_acquire);
        const bool idle_force = EpochPhysicalTxnAssignIdleReady(since, txn_limit);
        if (since < txn_limit && !idle_force) {
            return;
        }
        const uint64_t from_epoch = E_NodeManager::GetPhysicalEpoch();
        EpochRecvDiagLogPhysicalAdvance(from_epoch, from_epoch + 1, since, txn_limit, idle_force ? 1 : 0);
        (void)E_NodeManager::AddPhysicalEpoch();
        state->physical_txn_since_advance.store(0, std::memory_order_release);
        EpochPhysicalTxnIdleTrackerReset();
        EpochPerfNotePhysicalAdvance();
        return;
    }

    (void)E_NodeManager::AddPhysicalEpoch();
    EpochPerfNotePhysicalAdvance();
}

void EpochAdvancePhysicalBurst(void)
{
    EpochAdvancePhysicalOnce();
}

uint64_t EpochGetPhysicalTxnSinceAdvance(void)
{
    if (!g_enable_epoch_multimaster || g_epoch_physical_txns_per_epoch <= 0) {
        return 0;
    }
    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr) {
        return 0;
    }
    return state->physical_txn_since_advance.load(std::memory_order_acquire);
}

bool EpochTryReservePhysicalTxnSlot(uint64_t* out_slot_num)
{
    if (!g_enable_epoch_multimaster || g_epoch_physical_txns_per_epoch <= 0) {
        if (out_slot_num != nullptr) {
            *out_slot_num = 0;
        }
        return true;
    }
    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr) {
        return false;
    }
    const uint64_t limit = static_cast<uint64_t>(g_epoch_physical_txns_per_epoch);
    for (;;) {
        uint64_t cur = state->physical_txn_since_advance.load(std::memory_order_acquire);
        if (cur >= limit) {
            return false;
        }
        if (state->physical_txn_since_advance.compare_exchange_weak(
                cur, cur + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
            if (out_slot_num != nullptr) {
                *out_slot_num = cur + 1;
            }
            return true;
        }
    }
}

bool EpochIsLogicalReady(uint64_t logicalEpoch)
{
    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    uint32_t delay = static_cast<uint32_t>(g_epoch_delay_slots);
    if (delay == 0) {
        delay = EPOCH_DEFAULT_DELAY;
    }
    return physical > logicalEpoch + delay;
}

bool EpochAllNodesSendCompleted(uint64_t epochId)
{
    uint32_t totalNodes = EpochGetTotalNodes();
    for (uint32_t node = 0; node < totalNodes; ++node) {
        if (node != EpochGetNodeId() && !EpochPeerIsConnected(node)) {
            return false;
        }
        if (!E_NodeManager::IsNodeSendCompleted(epochId, node)) {
            return false;
        }
    }
    return true;
}

/* change_set（仅写）<= pack + abort */
bool EpochLocalTxnExcComplete(uint64_t epochId)
{
    uint32_t node_id = EpochGetNodeId();
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epochId, node_id);
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epochId, node_id);
    uint64_t aborts = E_NodeManager::GetAbortTxnCounters(epochId, node_id);
    return change_set <= packed + aborts;
}

static void EpochHealLocalTxnExc(uint64_t epoch, uint32_t node_id)
{
    if (!EpochIsSyncBackgroundWorkerCommit()) {
        return;
    }
    EpochMaybeWriteOffStaleChangeSet(epoch);
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epoch, node_id);
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    uint64_t aborts = E_NodeManager::GetAbortTxnCounters(epoch, node_id);
    while (change_set > packed + aborts) {
        E_NodeManager::IncAbortTxnCounters(epoch, node_id);
        aborts++;
    }
}

static void EpochMaybeWriteOffStaleChangeSet(uint64_t epoch)
{
    uint32_t node_id = EpochGetNodeId();
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epoch, node_id);
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    uint64_t aborts = E_NodeManager::GetAbortTxnCounters(epoch, node_id);
    uint64_t write_abort_after = E_NodeManager::GetWriteAbortAfterSendTxnCounters(epoch, node_id);
    uint64_t abort_net = (aborts > write_abort_after) ? (aborts - write_abort_after) : 0;
    uint64_t should_send = (change_set > abort_net) ? (change_set - abort_net) : 0;
    if (should_send > 0 && packed < should_send) {
        return;
    }
    if (change_set <= packed + aborts) {
        return;
    }
    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t gap = (physical > logical) ? (physical - logical) : 0;
    if (gap < EpochGetPhysLogicalGapLimit()) {
        return;
    }
    uint64_t orphan = change_set - packed - aborts;
    for (uint64_t i = 0; i < orphan; ++i) {
        E_NodeManager::IncAbortTxnCounters(epoch, node_id);
    }
}

bool EpochIsLocalCommitStageReady(uint64_t epoch)
{
    EpochMaybeWriteOffStaleChangeSet(epoch);
    if (EpochLocalTxnExcComplete(epoch)) {
        return true;
    }
    if (EpochIsSyncBackgroundWorkerCommit()) {
        EpochHealLocalTxnExc(epoch, EpochGetNodeId());
    }
    return EpochLocalTxnExcComplete(epoch);
}

bool EpochTryAdvanceLogical(void)
{
    if (!g_enable_epoch_multimaster) {
        return false;
    }

    EpochPerfNoteLogicalAdvanceTry();

    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t targetEpoch = logical;
    E_NodeManager::EnsureEpochSlot(targetEpoch);

    /*
     * 单节点：无 ZMQ/seal，physical 越过 delay 后即可推进 logical。
     */
    if (EpochGetTotalNodes() <= 1) {
        if (!EpochIsLogicalReady(targetEpoch)) {
            return false;
        }
        E_NodeManager::AddLogicalEpoch();
        E_NodeManager::ClearEpochSlot(targetEpoch);
        EpochPerfNoteLogicalAdvanceOk();
        return true;
    }

    EpochLogicalAdvanceTraceReset(targetEpoch);

    if (!EpochPipelineAllStagesReady(targetEpoch)) {
        EpochPipelineNoteLogicalAdvanceStall(targetEpoch);
        return false;
    }

    /*
     * AllStagesReady 已含 merge_complete(target)。必须在 ClearStageFlags 之前把
     * merge_epoch 推过本 epoch：否则 Clear 抹掉 merge_complete 后，
     * MaybeAdvanceMergeEpoch 永远看到 false，merge_epoch 永久卡在已清空的格
     * （实测 node3：merge_epoch=1、logical=2 → peer epoch-end 断档 → 对端 Assign 槽满）。
     * 这里只是 merge_complete 之后的 ++，不是按 logical Ensure。
     */
    if (!EpochMergePoolUsesInlineMerge()) {
        (void)EpochMergePoolTryAdvanceMergeEpoch(targetEpoch);
    }

    E_NodeManager::AddLogicalEpoch();
    EpochLifecycleEmitOnAdvance(targetEpoch);
    E_NodeManager::ClearEpochSlot(targetEpoch);
    EpochPipelineClearStageFlags(targetEpoch);
    EpochPerfNoteLogicalAdvanceOk();
    EpochLogicalAdvanceTraceOnOk(targetEpoch);
    return true;
}

bool EpochWaitForCommitGate(uint64_t commitEpoch)
{
    if (!g_enable_epoch_multimaster) {
        return true;
    }

    /*
     * GeoGauss is_sync_exec COMMIT gate: commit_epoch <= logical.
     */
    const long waitUs = 200;
    while (commitEpoch > E_NodeManager::GetLogicalEpoch()) {
        pg_usleep(waitUs);
        CHECK_FOR_INTERRUPTS();
    }
    return true;
}

bool EpochWaitForLogicalPast(uint64_t commitEpoch)
{
    if (!g_enable_epoch_multimaster) {
        return true;
    }

    const long waitUs = 200;
    while (commitEpoch >= E_NodeManager::GetLogicalEpoch()) {
        pg_usleep(waitUs);
        CHECK_FOR_INTERRUPTS();
    }
    return true;
}

uint64_t EpochAssignTxnEpoch(void)
{
    if (!g_enable_epoch_multimaster) {
        return 0;
    }
    /*
     * GeoGauss：直接用 physical 发号。physical 领先 logical 的上限由
     * EpochAdvancePhysicalOnce 的 gap_limit 背压保证；sync START gate 再约束执行时机。
     * DBR 抬过分配下界后，新写集不再填回已经让出的 epoch。
     */
    uint64_t epoch = E_NodeManager::GetPhysicalEpoch();
    uint64_t floor = EpochPacerAssignmentFloor();
    if (floor > epoch) {
        epoch = floor;
    }
    return epoch;
}
