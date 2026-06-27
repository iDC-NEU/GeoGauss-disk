#include "postgres.h"

#include "epoch/e_epoch.h"
#include "epoch/e_net.h"

#include "epoch/e_config.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_shmem.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_statistician.h"
#include "epoch/e_perf.h"

#include "miscadmin.h"

static void EpochMaybeWriteOffStaleChangeSet(uint64_t epoch);

void EpochAdvancePhysicalOnce(void)
{
    if (g_enable_epoch_multimaster && EpochGetTotalNodes() > 1) {
        uint64_t physical = E_NodeManager::GetPhysicalEpoch();
        uint64_t logical = E_NodeManager::GetLogicalEpoch();
        uint64_t gap = (physical > logical) ? (physical - logical) : 0;
        uint64_t limit = EpochGetPhysLogicalGapLimit();

        if (gap >= limit) {
            return;
        }
        /*
         * logical 关窗因 change_set_pending 卡住时，禁止继续推高 physical，
         * 否则新事务 START gate 的 commit_epoch 远超 logical，形成不可恢复自旋。
         */
        if (EpochLogicalEpochHasPendingPack(logical)) {
            return;
        }
    }
    (void)E_NodeManager::AddPhysicalEpoch();
    EpochPerfNotePhysicalAdvance();
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

static bool EpochAllNodesReceiveCompleted(uint64_t epochId)
{
    uint32_t totalNodes = EpochGetTotalNodes();
    for (uint32_t node = 0; node < totalNodes; ++node) {
        if (node != EpochGetNodeId() && !EpochPeerIsConnected(node)) {
            return false;
        }
        if (!E_NodeManager::IsNodeReceiveCompleted(epochId, node)) {
            return false;
        }
    }
    return true;
}

static bool EpochAllNodesSendCompleted(uint64_t epochId)
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

/* GeoGauss: change_set（仅写）<= produce + abort */
bool EpochLocalTxnExcComplete(uint64_t epochId)
{
    uint32_t node_id = EpochGetNodeId();
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epochId, node_id);
    uint64_t produced = E_NodeManager::GetLocalProduceTxnCounters(epochId, node_id);
    uint64_t aborts = E_NodeManager::GetAbortTxnCounters(epochId, node_id);
    return change_set <= produced + aborts;
}

static bool EpochLocalCommitPathComplete(uint64_t epochId)
{
    uint32_t node_id = EpochGetNodeId();
    uint64_t merged = E_NodeManager::GetMergedTxnCounters(epochId, node_id);
    uint64_t committed = E_NodeManager::GetCommittedTxnCounters(epochId, node_id);
    /*
     * Async best-effort：只要求 merge/apply 计数对齐即可关窗；pack/produce/record
     * 由 heal 补齐，避免 abort 计数漂移导致 commit_path_incomplete 永久卡住。
     */
    if (EpochIsAsyncMultimasterMode()) {
        return merged <= committed;
    }
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epochId, node_id);
    uint64_t produced = E_NodeManager::GetLocalProduceTxnCounters(epochId, node_id);
    uint64_t record_commit = E_NodeManager::GetLocalRecordCommitCounters(epochId, node_id);
    if (packed > 0 && produced < packed) {
        return false;
    }
    if (packed > 0 && record_commit < packed) {
        uint64_t write_abort_after = E_NodeManager::GetWriteAbortAfterSendTxnCounters(epochId, node_id);
        if (record_commit + write_abort_after < packed) {
            return false;
        }
    }
    if (merged > committed) {
        return false;
    }
    return true;
}

static void EpochAsyncHealLocalCommitCounters(uint64_t epoch, uint32_t node_id)
{
    if (!EpochIsAsyncMultimasterMode()) {
        return;
    }
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    while (E_NodeManager::GetLocalProduceTxnCounters(epoch, node_id) < packed) {
        E_NodeManager::IncLocalProduceTxnCounters(epoch, node_id);
    }
    while (E_NodeManager::GetLocalRecordCommitCounters(epoch, node_id) < packed) {
        E_NodeManager::IncLocalRecordCommitCounters(epoch, node_id);
        E_NodeManager::IncLocalRecordCommittedCounters(epoch, node_id);
    }
}

static void EpochAsyncAlignMergedCommitted(uint64_t epoch, uint32_t node_id)
{
    if (!EpochIsAsyncMultimasterMode()) {
        return;
    }
    uint64_t merged = E_NodeManager::GetMergedTxnCounters(epoch, node_id);
    while (E_NodeManager::GetCommittedTxnCounters(epoch, node_id) < merged) {
        E_NodeManager::IncCommittedTxnCounters(epoch, node_id);
    }
}

static void EpochAsyncHealLocalTxnExc(uint64_t epoch, uint32_t node_id)
{
    if (!EpochIsAsyncMultimasterMode()) {
        return;
    }
    EpochMaybeWriteOffStaleChangeSet(epoch);
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epoch, node_id);
    uint64_t produced = E_NodeManager::GetLocalProduceTxnCounters(epoch, node_id);
    uint64_t aborts = E_NodeManager::GetAbortTxnCounters(epoch, node_id);
    while (change_set > produced + aborts) {
        E_NodeManager::IncAbortTxnCounters(epoch, node_id);
        aborts++;
    }
}

static bool EpochLocalRecordApplied(uint64_t epochId)
{
    uint32_t node_id = EpochGetNodeId();
    uint64_t record_committed = E_NodeManager::GetLocalRecordCommittedCounters(epochId, node_id);
    uint64_t record_commit = E_NodeManager::GetLocalRecordCommitCounters(epochId, node_id);
    uint64_t write_abort_after = E_NodeManager::GetWriteAbortAfterSendTxnCounters(epochId, node_id);
    /*
     * COMMIT 已 IncLocalRecordCommit 但 RECORD_COMMIT 前 revalidate 失败/aborted 的路径
     * 只计 write_abort_after，不会 IncLocalRecordCommitted。
     */
    return record_committed + write_abort_after >= record_commit;
}

/*
 * GeoGauss 用 merged 与 should_receive 对齐；本实现 merged 仅统计含写且 CRDT 接受的 txn，
 * 只读/merge 输家 wire 仍计入 pack/recv。merge 在 OnRemoteTxnReceived 中与 recv 同步完成，
 * 故以 peer recv 对齐为准，不能用 merged >= pack。
 */
static bool EpochRemoteMergeReady(uint64_t epochId)
{
    if (EpochGetTotalNodes() <= 1) {
        return true;
    }
    uint32_t node_id = EpochGetNodeId();
    uint32_t total = EpochGetTotalNodes();
    for (uint32_t n = 0; n < total; ++n) {
        if (n == node_id) {
            continue;
        }
        if (!EpochPeerIsConnected(n)) {
            return false;
        }
        uint64_t should = E_NodeManager::GetStoreShouldReceivedPackCounters(epochId, n);
        uint64_t received = E_NodeManager::GetStoreReceivedPackCounters(epochId, n);
        if (received < should) {
            return false;
        }
    }
    return true;
}

/*
 * GeoGauss mot_internal.cpp:3257「Merge结束 开始Commit」：
 * 仅在 commit 路径就绪后短暂置 record_committed=false，并在同函数内等到
 * record 落盘计数对齐；若本轮未凑齐必须恢复 true，避免 START gate 永久冻住连接。
 * 返回 true 表示可 AddLogicalEpoch。
 */
static bool EpochTryFinishCommitPhase(uint64_t targetEpoch)
{
    if (!EpochLocalCommitPathComplete(targetEpoch)) {
        return false;
    }

    /*
     * Async：produce/record_commit/record_committed 均在 COMMIT pack 时入账，
     * 无 backend 等 record_committed gate；勿在此自旋，否则 logical 卡死。
     */
    if (EpochIsAsyncMultimasterMode()) {
        return true;
    }

    E_NodeManager::SetRecordCommitted(false);
    E_NodeManager::SetRemoteRecordCommitted(false);

    static const int kRecordSpin = 500;
    for (int spin = 0; spin < kRecordSpin && !EpochLocalRecordApplied(targetEpoch); ++spin) {
        pg_usleep(200L);
    }
    if (!EpochLocalRecordApplied(targetEpoch)) {
        E_NodeManager::SetRecordCommitted(true);
        EpochRecvDiagLogStall(targetEpoch, "record_incomplete");
        return false;
    }
    return true;
}

bool EpochIsRemoteMergeReady(uint64_t epochId)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return true;
    }
    if (!EpochAllNodesSendCompleted(epochId)) {
        return false;
    }
    return EpochAllNodesReceiveCompleted(epochId);
}

static void EpochMaybeWriteOffStaleChangeSet(uint64_t epoch)
{
    uint32_t node_id = EpochGetNodeId();
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epoch, node_id);
    uint64_t produced = E_NodeManager::GetLocalProduceTxnCounters(epoch, node_id);
    uint64_t aborts = E_NodeManager::GetAbortTxnCounters(epoch, node_id);
    uint64_t write_abort_after = E_NodeManager::GetWriteAbortAfterSendTxnCounters(epoch, node_id);
    uint64_t abort_net = (aborts > write_abort_after) ? (aborts - write_abort_after) : 0;
    uint64_t should_send = (change_set > abort_net) ? (change_set - abort_net) : 0;
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    if (should_send > 0 && packed < should_send) {
        return;
    }
    if (change_set <= produced + aborts) {
        return;
    }
    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t gap = (physical > logical) ? (physical - logical) : 0;
    if (gap < EpochGetPhysLogicalGapLimit()) {
        return;
    }
    uint64_t orphan = change_set - produced - aborts;
    for (uint64_t i = 0; i < orphan; ++i) {
        E_NodeManager::IncAbortTxnCounters(epoch, node_id);
    }
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
        E_NodeManager::SetRecordCommitted(true);
        E_NodeManager::ClearEpochSlot(targetEpoch);
        EpochPerfNoteLogicalAdvanceOk();
        return true;
    }

    if (!EpochAllPeersConnected()) {
        EpochRecvDiagLogStall(targetEpoch, "peer_not_connected");
        return false;
    }

    {
        uint32_t self = EpochGetNodeId();
        uint32_t total = EpochGetTotalNodes();
        for (uint32_t peer = 0; peer < total; ++peer) {
            if (peer == self) {
                continue;
            }
            if (targetEpoch > EpochGetPeerMaxSealedEpoch(peer) + 2) {
                EpochRecvDiagLogStall(targetEpoch, "peer_lag");
                return false;
            }
        }
    }

    /* GeoGauss 1: physical > logical + delay */
    if (!EpochIsLogicalReady(targetEpoch)) {
        EpochRecvDiagLogStall(targetEpoch, "physical_not_ready");
        return false;
    }

    /*
     * GeoGauss commit 阶段：对端 recv/merge 就绪后 SetRemoteExeced，backend 在
     * self seal 前 WaitForRemoteExeced → pack/produce。
     * 本端 recv 在 seal 后才完成，此处不能要求 self recv，否则 recv_incomplete
     * 永久成立 → chg=N pack=0 → SEAL_BLOCK change_set_pending。
     */
    if (!EpochIsAllPeersReceiveCompleted(targetEpoch)) {
        EpochRecvDiagLogStall(targetEpoch, "recv_incomplete");
        return false;
    }
    if (!EpochRemoteMergeReady(targetEpoch)) {
        EpochRecvDiagLogStall(targetEpoch, "merge_incomplete");
        return false;
    }
    if (!E_NodeManager::IsRemoteExeced()) {
        E_NodeManager::SetRemoteExeced(true);
    }

    /* GeoGauss 2: 写 change_set 已全部 produce/abort（remote_exec 已开，backend 可 pack） */
    EpochMaybeWriteOffStaleChangeSet(targetEpoch);
    if (!EpochLocalTxnExcComplete(targetEpoch)) {
        if (EpochIsAsyncMultimasterMode()) {
            EpochAsyncHealLocalTxnExc(targetEpoch, EpochGetNodeId());
        } else {
            EpochRecvDiagLogStall(targetEpoch, "local_commit_pending");
            return false;
        }
    }

    /* GeoGauss 3: seal 当前 logical epoch（TrySealEpoch 内处理 executing 与对端握手互锁） */
    EpochMaybeInvalidateStaleSeal(targetEpoch);
    if (!E_NodeManager::IsNodeSendCompleted(targetEpoch, EpochGetNodeId())) {
        (void)EpochTrySealEpoch(targetEpoch);
    }

    if (!EpochAllNodesSendCompleted(targetEpoch)) {
        EpochRecvDiagLogStall(targetEpoch, "send_incomplete");
        return false;
    }

    {
        uint32_t self = EpochGetNodeId();
        uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(targetEpoch, self);
        uint64_t sealed = E_NodeManager::GetSealedPackTxnCounters(targetEpoch, self);
        EpochMaybeInvalidateStaleSeal(targetEpoch);
        if (packed != sealed) {
            (void)EpochTrySealEpoch(targetEpoch);
            sealed = E_NodeManager::GetSealedPackTxnCounters(targetEpoch, self);
            if (packed != sealed) {
                EpochRecvDiagLogStall(targetEpoch, "reseal_pending");
                return false;
            }
        }
        if (!E_NodeManager::IsNodeSendCompleted(targetEpoch, self)) {
            EpochRecvDiagLogStall(targetEpoch, "reseal_pending");
            return false;
        }
    }

    if (!EpochIsAllPeersReceiveCompleted(targetEpoch)) {
        EpochRecvDiagLogStall(targetEpoch, "recv_incomplete");
        return false;
    }

    /*
     * Poll 里 ApplyReadyEpochs 往往早于 self seal，recv 未齐时跳过 apply →
     * merged>0 但 committed 恒 0 → commit_path_incomplete。关窗前再 apply 一次。
     */
    (void)EpochPipelineTryApplyMergedEpoch(targetEpoch);
    if (EpochIsAsyncMultimasterMode()) {
        EpochAsyncHealLocalCommitCounters(targetEpoch, EpochGetNodeId());
        EpochAsyncAlignMergedCommitted(targetEpoch, EpochGetNodeId());
    }

    if (!EpochLocalCommitPathComplete(targetEpoch)) {
        EpochRecvDiagLogStall(targetEpoch, "commit_path_incomplete");
        return false;
    }
    if (!EpochTryFinishCommitPhase(targetEpoch)) {
        return false;
    }

    E_NodeManager::SetRemoteExeced(false);
    E_NodeManager::SetRemoteRecordCommitted(false);
    E_NodeManager::AddLogicalEpoch();
    E_NodeManager::SetRecordCommitted(true);
    E_NodeManager::ClearEpochSlot(targetEpoch);
    EpochPerfNoteLogicalAdvanceOk();
    return true;
}

bool EpochWaitForCommitGate(uint64_t commitEpoch)
{
    if (!g_enable_epoch_multimaster) {
        return true;
    }

    /*
     * GeoGauss is_sync_exec COMMIT gate:
     * commit_epoch <= logical && record_committed.
     */
    const long waitUs = 200;
    while (commitEpoch > E_NodeManager::GetLogicalEpoch() ||
           !E_NodeManager::IsRecordCommitted()) {
        pg_usleep(waitUs);
        CHECK_FOR_INTERRUPTS();
    }
    return true;
}

bool EpochWaitForRemoteMerge(uint64_t commitEpoch)
{
    if (!g_enable_epoch_multimaster) {
        return true;
    }

    const long waitUs = 200;
    while (!EpochIsRemoteMergeReady(commitEpoch)) {
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

bool EpochWaitForSyncCommit(uint64_t commitEpoch)
{
    (void)commitEpoch;
    /*
     * GeoGauss 同步等待在 RECORD_COMMIT 之后、END 里 EndTransaction 之后
     * 等待 remote_record_committed。
     */
    return true;
}

bool EpochWaitForRecordCommitted(uint64_t commitEpoch)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return true;
    }
    uint32_t self = EpochGetNodeId();
    for (uint32_t node = 0; node < EpochGetTotalNodes(); ++node) {
        if (node != self && !EpochPeerIsConnected(node)) {
            return true;
        }
    }
    const long waitUs = 200;
    while (!E_NodeManager::IsRemoteRecordCommitted()) {
        (void)commitEpoch;
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
     * GeoGauss 用 physical 发号；cap 到 logical+gap_limit 避免重启后 physical 远超 logical。
     * sync START/COMMIT gate 要求 commit_epoch <= logical，事务在 gate 自旋直到 logical 追上。
     */
    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t cap = logical + EpochGetPhysLogicalGapLimit();
    return (physical > cap) ? cap : physical;
}
