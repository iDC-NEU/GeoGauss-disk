#include "postgres.h"

#include "epoch/e_epoch.h"
#include "epoch/e_net.h"

#include "epoch/e_config.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_shmem.h"
#include "epoch/e_statistician.h"

#include "miscadmin.h"

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

/* GeoGauss: LoadChangeSet <= GetLocalTxnExcCounters */
static bool EpochLocalTxnExcComplete(uint64_t epochId)
{
    uint32_t node_id = EpochGetNodeId();
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epochId, node_id);
    uint64_t produced = E_NodeManager::GetLocalProduceTxnCounters(epochId, node_id);
    uint64_t read_committed = E_NodeManager::GetReadCommittedTxnCounters(epochId, node_id);
    uint64_t aborts = E_NodeManager::GetAbortTxnCounters(epochId, node_id);
    return change_set <= produced + read_committed + aborts;
}

static bool EpochLocalCommitPathComplete(uint64_t epochId)
{
    uint32_t node_id = EpochGetNodeId();
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
    uint64_t merged = E_NodeManager::GetMergedTxnCounters(epochId, node_id);
    uint64_t committed = E_NodeManager::GetCommittedTxnCounters(epochId, node_id);
    if (merged > committed) {
        return false;
    }
    return true;
}

static bool EpochLocalRecordApplied(uint64_t epochId)
{
    uint32_t node_id = EpochGetNodeId();
    return E_NodeManager::GetLocalRecordCommittedCounters(epochId, node_id) >=
           E_NodeManager::GetLocalRecordCommitCounters(epochId, node_id);
}

/*
 * GeoGauss: GetRemoteMergedTxnCounters >= GetShouldReceiveTxnNum（按 pack 计数近似）
 */
static bool EpochRemoteMergeReady(uint64_t epochId)
{
    if (EpochGetTotalNodes() <= 1) {
        return true;
    }
    uint32_t node_id = EpochGetNodeId();
    uint64_t expect = 0;
    uint32_t total = EpochGetTotalNodes();
    for (uint32_t n = 0; n < total; ++n) {
        if (n == node_id) {
            continue;
        }
        if (!EpochPeerIsConnected(n)) {
            return false;
        }
        expect += E_NodeManager::GetStoreShouldReceivedPackCounters(epochId, n);
    }
    return E_NodeManager::GetMergedTxnCounters(epochId, node_id) >= expect;
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

bool EpochTryAdvanceLogical(void)
{
    if (!g_enable_epoch_multimaster) {
        return false;
    }

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
        return true;
    }

    if (!EpochAllPeersConnected()) {
        EpochRecvDiagLogStall(targetEpoch, "peer_not_connected");
        return false;
    }

    /* GeoGauss 1: physical > logical + delay */
    if (!EpochIsLogicalReady(targetEpoch)) {
        EpochRecvDiagLogStall(targetEpoch, "physical_not_ready");
        return false;
    }

    /* GeoGauss 2: change_set <= local_txn_exc */
    if (!EpochLocalTxnExcComplete(targetEpoch)) {
        EpochRecvDiagLogStall(targetEpoch, "local_commit_pending");
        return false;
    }

    /* GeoGauss 3: seal 当前 logical epoch（发 epoch-end + 写集已在 drain 时发出） */
    if (!E_NodeManager::IsNodeSendCompleted(targetEpoch, EpochGetNodeId())) {
        (void)EpochTrySealEpoch(targetEpoch);
    }

    if (!EpochAllNodesSendCompleted(targetEpoch)) {
        EpochRecvDiagLogStall(targetEpoch, "send_incomplete");
        return false;
    }
    if (!EpochAllNodesReceiveCompleted(targetEpoch)) {
        EpochRecvDiagLogStall(targetEpoch, "recv_incomplete");
        return false;
    }

    /* GeoGauss 4: 远端 merge 完成 */
    if (!EpochRemoteMergeReady(targetEpoch)) {
        EpochRecvDiagLogStall(targetEpoch, "merge_incomplete");
        return false;
    }

    /*
     * 先置 remote_exec，让已 pack 的 backend 能走完 record_commit（GeoGauss 在 commit 阶段前置 true）。
     * 此阶段保持 record_committed=true，新连接可过 START gate 的第二条检查。
     */
    if (!E_NodeManager::IsRemoteExeced()) {
        E_NodeManager::SetRemoteExeced(true);
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
     * GeoGauss 用 GetPhysicalEpoch() 发号；再 cap 到 logical+gap_limit，
     * 避免重启/滞后时 physical 远超 logical 导致 START 长时间自旋。
     */
    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t cap = logical + EpochGetPhysLogicalGapLimit();
    return (physical > cap) ? cap : physical;
}
