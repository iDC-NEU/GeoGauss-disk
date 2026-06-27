#include "postgres.h"

#include <atomic>

#include "epoch/e_statistician.h"
#include "epoch/e_config.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_types.h"
#include "epoch/e_net.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_txn_queue.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_trace.h"
#include "epoch/e_perf.h"
#include "epoch/e_log_path.h"

#include "miscadmin.h"
#include "knl/knl_variable.h"

#include <cstdio>
#include <cstring>
#include <ctime>
#include <unordered_map>
#include <unordered_set>

static uint64_t g_last_diag_physical = 0;
static std::unordered_map<uint64_t, uint64_t> g_recvdiag_applied_rows;
static std::unordered_map<uint64_t, uint64_t> g_recvdiag_applied_txns;
static uint64_t g_recvdiag_last_stall_epoch = 0;
static uint64_t g_recvdiag_stall_count = 0;
static std::unordered_map<uint64_t, int> g_txn_request_retry_tick;
static std::atomic<uint64_t> g_peer_max_sealed_epoch[EPOCH_MAX_NODES] = {};

static uint64_t EpochComputeShouldSend(uint64_t epoch, uint32_t node_id);

bool EpochIsAsyncMultimasterMode(void)
{
    return !g_epoch_commit_sync && g_enable_epoch_multimaster && EpochGetTotalNodes() > 1;
}

uint64_t EpochGetPeerMaxSealedEpoch(uint32_t peer_node)
{
    if (peer_node >= EPOCH_MAX_NODES) {
        return 0;
    }
    return g_peer_max_sealed_epoch[peer_node].load(std::memory_order_acquire);
}

void EpochNotePeerSealedEpoch(uint32_t peer_node, uint64_t epoch)
{
    if (peer_node >= EPOCH_MAX_NODES || epoch == 0) {
        return;
    }
    uint64_t cur = g_peer_max_sealed_epoch[peer_node].load(std::memory_order_relaxed);
    while (epoch > cur &&
           !g_peer_max_sealed_epoch[peer_node].compare_exchange_weak(
               cur, epoch, std::memory_order_release, std::memory_order_relaxed)) {
    }
}

void EpochRestorePastEpochPeerFlags(uint64_t epoch)
{
    if (EpochGetTotalNodes() <= 1 || epoch == 0) {
        return;
    }
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    if (epoch >= logical) {
        return;
    }
    uint32_t self = EpochGetNodeId();
    for (uint32_t node = 0; node < EpochGetTotalNodes(); ++node) {
        if (node == self) {
            continue;
        }
        E_NodeManager::SetNodeSendCompleted(epoch, node, true);
        E_NodeManager::SetNodeReceiveCompleted(epoch, node, true);
    }
}

/*
 * 较快节点不可比任一 peer 已 seal 的 epoch 超前太多就关窗，否则 self_send=1 而对端
 * 永远发不出 peer_send=1 → recv_incomplete 永久成立（本次 epoch=2113 卡死）。
 */
static bool EpochSealBlockedByPeerLag(uint64_t epoch, uint32_t node_id)
{
    if (EpochGetTotalNodes() <= 1) {
        return false;
    }
    uint32_t total = EpochGetTotalNodes();
    for (uint32_t peer = 0; peer < total; ++peer) {
        if (peer == node_id) {
            continue;
        }
        if (!EpochPeerIsConnected(peer)) {
            continue;
        }
        if (E_NodeManager::IsNodeSendCompleted(epoch, peer)) {
            continue;
        }
        uint64_t peer_max = EpochGetPeerMaxSealedEpoch(peer);
        if (epoch > peer_max + 1) {
            return true;
        }
    }
    return false;
}

static void EpochRecvDiagAppendLine(const char* line)
{
    if (!g_epoch_recv_diag) {
        return;
    }
    char path[MAXPGPATH];
    if (!EpochFormatLogPath(path, sizeof(path), "epoch_recv_diag")) {
        return;
    }

    FILE* fp = fopen(path, "a");
    if (fp == nullptr) {
        return;
    }

    time_t now = time(nullptr);
    struct tm tm_buf;
    (void)localtime_r(&now, &tm_buf);
    char ts[32];
    (void)strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_buf);

    fprintf(fp, "[%s] node=%u %s\n", ts, EpochGetNodeId(), line);
    (void)fclose(fp);
}

void EpochWireDiagLogTxnSend(const E_WireTxn& txn)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    if (txn.start_epoch == 0 && txn.rows.empty()) {
        return;
    }
    char buf[192];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "SEND_TXN epoch=%lu csn=%lu from=%u rows=%zu",
        txn.commit_epoch,
        txn.csn,
        txn.server_id,
        txn.rows.size());
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochWireDiagLogTxnRecv(const E_WireTxn& txn)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    if (txn.start_epoch == 0 && txn.rows.empty()) {
        return;
    }
    char buf[192];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "RECV_TXN epoch=%lu csn=%lu from=%u rows=%zu",
        txn.commit_epoch,
        txn.csn,
        txn.server_id,
        txn.rows.size());
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogHello(const char* tag, uint32_t from_node)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 || tag == nullptr) {
        return;
    }
    char buf[64];
    int rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "%s from=%u", tag, from_node);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

bool EpochBuildEpochEndWire(uint64_t epoch, E_WireTxn* out)
{
    if (out == nullptr) {
        return false;
    }
    out->csn = E_NodeManager::GetLocalPackTxnCounters(epoch, EpochGetNodeId());
    out->start_epoch = 0;
    out->commit_epoch = epoch;
    out->server_id = EpochGetNodeId();
    out->txn_state = EPOCH_TXN_EMPTY;
    out->rows.clear();
    return true;
}

void EpochOnEpochEndWireReceived(const E_WireTxn& txn)
{
    if (txn.start_epoch != 0) {
        return;
    }
    uint64_t epoch = txn.commit_epoch;
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint32_t from_node = txn.server_id;
    /*
     * 双节点 ZMQ 到达顺序不确定，较快节点可能先把 epoch-end 发到较慢节点尚未关窗的 epoch
     * （epoch > logical）或已关窗的 epoch（epoch < logical）。
     * 旧逻辑仅接受 epoch==logical，导致 RECV_FUTURE/RECV_STALE 丢弃对端 seal 信号，
     * peer_send 永不为 1 → send_incomplete → logical 卡死 → gsql START gate 自旋。
     * 允许 [logical, logical+1] 的未来一包，以及 epoch<logical 的迟到补录。
     */
    if (epoch + 1 < logical) {
        const char* tag = "RECV_STALE";
        char buf[160];
        int rc = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            "%s epoch=%lu logical=%lu peer=%u pack=%lu",
            tag,
            epoch,
            logical,
            from_node,
            txn.csn);
        securec_check_ss(rc, "\0", "\0");
        EpochRecvDiagAppendLine(buf);
    } else if (epoch > logical + EpochGetPhysLogicalGapLimit() + 1) {
        const char* tag = "RECV_FUTURE";
        char buf[160];
        int rc = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            "%s epoch=%lu logical=%lu peer=%u pack=%lu",
            tag,
            epoch,
            logical,
            from_node,
            txn.csn);
        securec_check_ss(rc, "\0", "\0");
        EpochRecvDiagAppendLine(buf);
        return;
    }

    E_NodeManager::EnsureEpochSlot(epoch);
    uint64_t pack_count = txn.csn;
    uint64_t prev_should = E_NodeManager::GetStoreShouldReceivedPackCounters(epoch, from_node);
    bool already_send = E_NodeManager::IsNodeSendCompleted(epoch, from_node);
    /*
     * TXN_REQ 重播 epoch-end 时 send_completed 可能已为 1；若仍 recv 不齐须更新 should_receive
     * 并重新判定 recv，不能因 early return 丢弃重播。
     * 本端 reopen seal 后 pack 数增加时，也须用更大的 pack_count 刷新对端 should_receive。
     */
    if (already_send && pack_count == prev_should) {
        EpochPipelineResyncPeerReceived(epoch, from_node);
        (void)EpochCheckAndSetNodeReceiveCompleted(epoch, from_node);
        EpochNotePeerSealedEpoch(from_node, epoch);
        return;
    }
    if (already_send && pack_count < prev_should) {
        EpochPipelineResyncPeerReceived(epoch, from_node);
        (void)EpochCheckAndSetNodeReceiveCompleted(epoch, from_node);
        EpochNotePeerSealedEpoch(from_node, epoch);
        return;
    }

    E_NodeManager::SetStoreShouldReceivedPackCounters(epoch, from_node, pack_count);
    /* 对端 epoch-end 的 pack 数同步到本节点视图，便于 seal / logical 推进 */
    E_NodeManager::SetLocalPackTxnCounters(epoch, from_node, pack_count);
    E_NodeManager::SetLocalProduceTxnCounters(epoch, from_node, pack_count);
    E_NodeManager::SetNodeSendCompleted(epoch, from_node, true);
    /*
     * 重 seal 抬高 should 时，slot 可能已被 Clear 导致 received 归零，
     * 但 dedupe 会丢弃重传 txn；用 g_peer_received_counts 恢复 received。
     */
    EpochPipelineResyncPeerReceived(epoch, from_node);
    (void)EpochCheckAndSetNodeReceiveCompleted(epoch, from_node);
    EpochNotePeerSealedEpoch(from_node, epoch);
    EpochRecvDiagLogEpochEnd(epoch, from_node, pack_count);
    EpochTraceMaybeLogPeerReceived(epoch);
}

bool EpochCheckAndSetNodeReceiveCompleted(uint64_t epochId, uint64_t nodeId)
{
    uint64_t should_receive = E_NodeManager::GetStoreShouldReceivedPackCounters(epochId, nodeId);
    uint64_t received = E_NodeManager::GetStoreReceivedPackCounters(epochId, nodeId);
    bool done = (should_receive == received);
    /*
     * 空 epoch（pack=0）时 should==received==0 不能单独判定完成；
     * 须等对端真正 seal 并发出 epoch-end（IsNodeSendCompleted）。
     */
    if (done && should_receive == 0 && received == 0) {
        done = E_NodeManager::IsNodeSendCompleted(epochId, nodeId);
    }
    E_NodeManager::SetNodeReceiveCompleted(epochId, nodeId, done);
    return done;
}

bool EpochIsAllPeersReceiveCompleted(uint64_t epochId)
{
    uint32_t self = EpochGetNodeId();
    uint32_t total = EpochGetTotalNodes();
    for (uint32_t node = 0; node < total; ++node) {
        if (node == self) {
            continue;
        }
        if (!EpochPeerIsConnected(node)) {
            return false;
        }
        if (!E_NodeManager::IsNodeReceiveCompleted(epochId, node)) {
            return false;
        }
    }
    return true;
}

bool EpochIsAllNodesReceiveCompleted(uint64_t epochId)
{
    if (!E_NodeManager::IsNodeReceiveCompleted(epochId, EpochGetNodeId())) {
        return false;
    }
    return EpochIsAllPeersReceiveCompleted(epochId);
}

static bool EpochSealEpoch(uint64_t epoch)
{
    E_WireTxn end_wire;
    if (!EpochBuildEpochEndWire(epoch, &end_wire)) {
        return false;
    }
    std::string payload;
    if (!E_WireSerializeTxn(end_wire, &payload)) {
        return false;
    }
    /* worker 内直接发布 epoch-end，避免仅入队时 drain 滞后一轮 */
    (void)EpochNetPublish(payload);
    EpochPerfNoteEpochEndSend(payload.size());
    if (!EpochTxnQueuePush(payload.data(), static_cast<uint32_t>(payload.size()))) {
        ereport(WARNING, (errmsg("epoch: failed to enqueue epoch-end marker for epoch %lu", epoch)));
    }
    return true;
}

static uint64_t EpochComputeShouldSend(uint64_t epoch, uint32_t node_id)
{
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epoch, node_id);
    uint64_t aborts = E_NodeManager::GetAbortTxnCounters(epoch, node_id);
    uint64_t write_abort_after = E_NodeManager::GetWriteAbortAfterSendTxnCounters(epoch, node_id);
    uint64_t abort_net = (aborts > write_abort_after) ? (aborts - write_abort_after) : 0;
    /* change_set 仅 OCC 写事务 COMMIT pack 时 +1；只读不参与 should_send */
    return (change_set > abort_net) ? (change_set - abort_net) : 0;
}

/*
 * 推迟空 seal 仅看本端是否还有未 pack 的写事务。
 * 对端 wire txn / merge 不能阻止本端 pack=0 关窗，否则双写时 node1 无本地写却
 * 永远不发 epoch-end → node0 peer_send=0 → recv_incomplete 死锁。
 */
static bool EpochShouldDeferEmptySeal(uint64_t epoch, uint32_t node_id)
{
    uint64_t should_send = EpochComputeShouldSend(epoch, node_id);
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    if (should_send > 0 || packed > 0) {
        return false;
    }
    if (E_NodeManager::GetChangeSetTxnCounters(epoch, node_id) > 0) {
        return true;
    }
    /*
     * Async 无 WaitForRemoteExeced 互锁：允许空 seal，在途 SQL 后续 pack 触发 re-seal。
     */
    if (EpochIsAsyncMultimasterMode()) {
        return false;
    }
    if (E_NodeManager::GetExecutingTxnCounters(epoch, node_id) > 0) {
        return true;
    }
    return false;
}

/*
 * 对端已 seal 且本端已收齐该 epoch txn，但对端仍等本端 epoch-end（peer_send=1, peer_recv=1）。
 * 若本端 should_send=0 却因 executing>0 无法 seal，会形成 recv 互锁；此时强制空 seal，
 * 在途事务后续 pack 会触发 re-seal。
 */
static bool EpochShouldForceEmptySealForPeerHandshake(uint64_t epoch, uint32_t node_id)
{
    if (EpochGetTotalNodes() <= 1) {
        return false;
    }
    uint64_t should_send = EpochComputeShouldSend(epoch, node_id);
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    if (should_send > 0 || packed > 0) {
        return false;
    }
    if (E_NodeManager::GetChangeSetTxnCounters(epoch, node_id) > 0) {
        return false;
    }
    uint32_t total = EpochGetTotalNodes();
    bool saw_peer = false;
    for (uint32_t peer = 0; peer < total; ++peer) {
        if (peer == node_id || !EpochPeerIsConnected(peer)) {
            continue;
        }
        saw_peer = true;
        if (!E_NodeManager::IsNodeSendCompleted(epoch, peer)) {
            return false;
        }
        if (!E_NodeManager::IsNodeReceiveCompleted(epoch, peer)) {
            return false;
        }
    }
    return saw_peer;
}

void EpochMaybeInvalidateStaleSeal(uint64_t epoch)
{
    if (EpochGetTotalNodes() <= 1) {
        return;
    }
    uint32_t node_id = EpochGetNodeId();
    if (!E_NodeManager::IsNodeSendCompleted(epoch, node_id)) {
        return;
    }
    uint64_t sealed = E_NodeManager::GetSealedPackTxnCounters(epoch, node_id);
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    uint64_t should_send = EpochComputeShouldSend(epoch, node_id);
    /*
     * 空 seal 后 change_set 增加但尚未 pack 时 packed==sealed==0，须撤销 send 以便 re-seal。
     */
    if (should_send > sealed || packed > sealed) {
        E_NodeManager::SetNodeSendCompleted(epoch, node_id, false);
        E_NodeManager::SetNodeReceiveCompleted(epoch, node_id, false);
    }
}

/*
 * 关窗前要求本 epoch 无仍在执行 SQL、尚未 pack 读写集的事务。
 * 不等 OCC produce / END_TRANSACTION。
 * gap 顶满且 should_send=0 时，若 executing 仍不归零，多为泄漏或已 pack 前释放 executing
 * 的 backend 异常退出；核销以免 logical 永久卡住。
 */
static void EpochMaybeWriteOffStaleExecuting(uint64_t epoch, uint32_t node_id)
{
    if (E_NodeManager::GetExecutingTxnCounters(epoch, node_id) == 0) {
        return;
    }
    /*
     * change_set==0 时尚无 COMMIT pack，executing 表示在途 SQL/尚未 pack 的事务。
     * 若此时核销 executing，会空 seal 并推进 logical，随后旧 epoch 又补 pack → 双端卡死。
     */
    if (E_NodeManager::GetChangeSetTxnCounters(epoch, node_id) == 0) {
        /*
         * Async：physical 已顶满 gap 时，泄漏的 executing 会挡住空 epoch seal（epoch=830
         * exec_pending）；核销以便先跑通 TPCC，在途 SQL 后续 pack 触发 re-seal。
         */
        if (EpochIsAsyncMultimasterMode()) {
            uint64_t physical = E_NodeManager::GetPhysicalEpoch();
            uint64_t logical = E_NodeManager::GetLogicalEpoch();
            uint64_t gap = (physical > logical) ? (physical - logical) : 0;
            if (gap >= EpochGetPhysLogicalGapLimit()) {
                while (E_NodeManager::GetExecutingTxnCounters(epoch, node_id) > 0) {
                    E_NodeManager::DecExecutingTxnCounters(epoch, node_id);
                }
            }
        }
        return;
    }
    if (EpochComputeShouldSend(epoch, node_id) > 0) {
        return;
    }
    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t gap = (physical > logical) ? (physical - logical) : 0;
    if (gap < EpochGetPhysLogicalGapLimit()) {
        return;
    }
    while (E_NodeManager::GetExecutingTxnCounters(epoch, node_id) > 0) {
        E_NodeManager::DecExecutingTxnCounters(epoch, node_id);
    }
}

static bool EpochSealBlockedByExecuting(uint64_t epoch, uint32_t node_id)
{
    if (E_NodeManager::GetExecutingTxnCounters(epoch, node_id) == 0) {
        return false;
    }
    uint64_t should_send = EpochComputeShouldSend(epoch, node_id);
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    /*
     * 本 epoch 写集已 pack 齐 should_send 时，剩余 executing 为在途 SQL；
     * 继续等待会与 WaitForRemoteExeced 互锁，后续 pack 由 re-seal 补上。
     */
    if (should_send > 0 && packed >= should_send) {
        return false;
    }
    /*
     * Async：无 backend 等 remote_exec，executing 仅表示在途 SQL，不应阻塞空 epoch 关窗。
     */
    if (EpochIsAsyncMultimasterMode() && should_send == 0 && packed == 0) {
        return false;
    }
    return true;
}

static bool EpochShouldForceSealDespiteExecuting(uint64_t epoch, uint32_t node_id)
{
    if (EpochSealBlockedByExecuting(epoch, node_id)) {
        return EpochShouldForceEmptySealForPeerHandshake(epoch, node_id);
    }
    return false;
}

bool EpochLogicalEpochHasPendingPack(uint64_t epoch)
{
    if (EpochGetTotalNodes() <= 1) {
        return false;
    }
    uint32_t node_id = EpochGetNodeId();
    uint64_t should_send = EpochComputeShouldSend(epoch, node_id);
    if (should_send == 0) {
        return false;
    }
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    return packed < should_send;
}

bool EpochTrySealEpoch(uint64_t epoch)
{
    if (EpochGetTotalNodes() <= 1) {
        return false;
    }

    E_NodeManager::EnsureEpochSlot(epoch);

    uint32_t node_id = EpochGetNodeId();
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    uint64_t should_send = EpochComputeShouldSend(epoch, node_id);

    /*
     * change_set 仅在写 COMMIT pack 时 +1；若已 seal 但 pack 未齐或 pack 数变化，撤销 send 并重 seal。
     */
    if (E_NodeManager::IsNodeSendCompleted(epoch, node_id)) {
        uint64_t sealed = E_NodeManager::GetSealedPackTxnCounters(epoch, node_id);
        should_send = EpochComputeShouldSend(epoch, node_id);
        if (packed >= should_send && packed == sealed && should_send == sealed) {
            return true;
        }
        E_NodeManager::SetNodeSendCompleted(epoch, node_id, false);
        E_NodeManager::SetNodeReceiveCompleted(epoch, node_id, false);
    }

    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    uint32_t delay = static_cast<uint32_t>(g_epoch_delay_slots);
    if (delay == 0) {
        delay = EPOCH_DEFAULT_DELAY;
    }
    /*
     * 关窗前等 physical 窗口；若有事务仍在执行且读写集未 pack，则暂不 seal。
     */
    if (physical <= epoch + static_cast<uint64_t>(delay) + 1) {
        EpochRecvDiagLogSealBlock(epoch, "physical_delay");
        return false;
    }

    if (EpochSealBlockedByPeerLag(epoch, node_id)) {
        EpochRecvDiagLogSealBlock(epoch, "peer_lag");
        return false;
    }

    EpochMaybeWriteOffStaleExecuting(epoch, node_id);

    bool force_despite_executing = EpochShouldForceSealDespiteExecuting(epoch, node_id);
    if (EpochSealBlockedByExecuting(epoch, node_id) && !force_despite_executing) {
        EpochRecvDiagLogSealBlock(epoch, "exec_pending");
        EpochTraceMaybeLogSealBlocked(epoch, "exec_pending",
            E_NodeManager::GetExecutingTxnCounters(epoch, node_id),
            E_NodeManager::GetChangeSetTxnCounters(epoch, node_id),
            E_NodeManager::GetLocalPackTxnCounters(epoch, node_id));
        return false;
    }

    /*
     * GeoGauss IsCurrentEpochFinished: packed >= should_send
     */
    should_send = EpochComputeShouldSend(epoch, node_id);
    packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);

    if (packed < should_send) {
        EpochRecvDiagLogSealBlock(epoch, "change_set_pending");
        return false;
    }

    if (should_send == 0 && packed == 0 && EpochShouldDeferEmptySeal(epoch, node_id) &&
        !force_despite_executing) {
        EpochRecvDiagLogSealBlock(epoch, "epoch_active");
        return false;
    }

    /*
     * 对端 epoch-end 已到但 txn 未收齐时暂不 seal，避免本端先关窗而对端永远 recv 不齐。
     * 空 epoch 无 epoch-end 前 IsNodeSendCompleted(peer) 为 false，不会误伤启动握手。
     * 本端已空 seal 但 should_send 已抬高时须优先 re-seal，不可被 peer_txn_pending 挡住。
     */
    uint64_t sealed_now = E_NodeManager::GetSealedPackTxnCounters(epoch, node_id);
    bool self_reseal_needed = (should_send > sealed_now || packed > sealed_now);
    uint32_t total = EpochGetTotalNodes();
    if (!self_reseal_needed) {
        for (uint32_t peer = 0; peer < total; ++peer) {
            if (peer == node_id || !EpochPeerIsConnected(peer)) {
                continue;
            }
            if (E_NodeManager::IsNodeSendCompleted(epoch, peer) &&
                !E_NodeManager::IsNodeReceiveCompleted(epoch, peer)) {
                EpochRecvDiagLogSealBlock(epoch, "peer_txn_pending");
                return false;
            }
        }
    }

    EpochPipelineRebroadcastCachedTxns(epoch);
    pg_usleep(500L);
    EpochPipelineRebroadcastCachedTxns(epoch);

    /* usleep 期间可能有新 change_set / pack，关窗前再核对一次 */
    should_send = EpochComputeShouldSend(epoch, node_id);
    packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    if (packed < should_send) {
        EpochRecvDiagLogSealBlock(epoch, "change_set_pending");
        return false;
    }
    force_despite_executing = EpochShouldForceSealDespiteExecuting(epoch, node_id);
    if (EpochSealBlockedByExecuting(epoch, node_id) && !force_despite_executing) {
        EpochRecvDiagLogSealBlock(epoch, "exec_pending");
        EpochTraceMaybeLogSealBlocked(epoch, "exec_pending",
            E_NodeManager::GetExecutingTxnCounters(epoch, node_id),
            E_NodeManager::GetChangeSetTxnCounters(epoch, node_id), packed);
        return false;
    }

    EpochTraceLogSealPrepare(epoch, packed);

    if (!EpochSealEpoch(epoch)) {
        return false;
    }

    E_NodeManager::SetSealedPackTxnCounters(epoch, node_id, packed);
    E_NodeManager::SetNodeSendCompleted(epoch, EpochGetNodeId(), true);
    E_NodeManager::SetNodeReceiveCompleted(epoch, EpochGetNodeId(), true);
    EpochRecvDiagLogSent(epoch, E_NodeManager::GetLocalPackTxnCounters(epoch, EpochGetNodeId()));
    EpochPerfNoteSealOk();
    return true;
}

void EpochMaybeRequestMissingTxns(void)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 || !EpochAllPeersConnected()) {
        return;
    }
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint32_t self = EpochGetNodeId();
    uint32_t total = EpochGetTotalNodes();
    uint64_t lookback = EpochGetPhysLogicalGapLimit() + 64;
    uint64_t start = (logical > lookback) ? (logical - lookback) : 1;
    uint64_t request_epoch = 0;
    uint32_t request_peer = self;
    for (uint32_t peer = 0; peer < total; ++peer) {
        if (peer == self) {
            continue;
        }
        uint64_t peer_max = EpochGetPeerMaxSealedEpoch(peer);
        for (uint64_t epoch = start; epoch <= logical; ++epoch) {
            if (E_NodeManager::IsNodeReceiveCompleted(epoch, peer)) {
                continue;
            }
            if (epoch < peer_max) {
                EpochRestorePastEpochPeerFlags(epoch);
                continue;
            }
            if (request_epoch == 0 || epoch < request_epoch) {
                request_epoch = epoch;
                request_peer = peer;
            }
            break;
        }
    }
    if (request_epoch == 0) {
        return;
    }
    static int tick = 0;
    if (++tick % 50 != 0) {
        return;
    }
    /*
     * GeoGauss mot_internal.cpp: cache_txn_request / server_txn_request：
     * 对端 pack 未收齐时主动请求，由 EpochHandleTxnRequest 重放 cache + epoch-end。
     */
    EpochPublishTxnRequest(request_epoch);
    char buf[96];
    int rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1,
        "TXN_REQ_SENT epoch=%lu peer=%u", request_epoch, request_peer);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochOnPeerConnected(uint32_t peerId)
{
    if (peerId == EpochGetNodeId() || EpochGetTotalNodes() <= 1) {
        return;
    }
    /*
     * GeoGauss 不在上线时主动重播 epoch；缺包由对端发 cache_txn_request 拉取。
     */
}

void EpochDiagReset(void)
{
    g_last_diag_physical = 0;
}

void EpochRecvDiagReset(void)
{
    g_recvdiag_applied_rows.clear();
    g_recvdiag_applied_txns.clear();
    g_recvdiag_last_stall_epoch = 0;
    g_recvdiag_stall_count = 0;
    g_txn_request_retry_tick.clear();
    for (uint32_t i = 0; i < EPOCH_MAX_NODES; ++i) {
        g_peer_max_sealed_epoch[i].store(0, std::memory_order_relaxed);
    }
}

void EpochRecvDiagLogSealBlock(uint64_t epoch, const char* reason)
{
    EpochPerfNoteSealBlock(reason);
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 || reason == nullptr) {
        return;
    }
    uint32_t node_id = EpochGetNodeId();
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epoch, node_id);
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    uint64_t read_committed = E_NodeManager::GetReadCommittedTxnCounters(epoch, node_id);
    uint64_t aborts = E_NodeManager::GetAbortTxnCounters(epoch, node_id);
    uint64_t write_abort_after = E_NodeManager::GetWriteAbortAfterSendTxnCounters(epoch, node_id);
    uint64_t abort_net = (aborts > write_abort_after) ? (aborts - write_abort_after) : 0;
    uint64_t should_send = (change_set > abort_net) ? (change_set - abort_net) : 0;
    uint64_t executing = E_NodeManager::GetExecutingTxnCounters(epoch, node_id);

    char buf[384];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "SEAL_BLOCK epoch=%lu reason=%s chg=%lu pack=%lu executing=%lu read=%lu abort=%lu w_abort=%lu should_send=%lu",
        epoch,
        reason,
        change_set,
        packed,
        executing,
        read_committed,
        aborts,
        write_abort_after,
        should_send);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogSent(uint64_t epoch, uint64_t sent_count)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    char buf[256];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "SENT epoch=%lu pack=%lu",
        epoch,
        sent_count);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogEpochEnd(uint64_t epoch, uint32_t from_node, uint64_t expected_pack)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    uint64_t received = E_NodeManager::GetStoreReceivedPackCounters(epoch, from_node);
    uint64_t merged = E_NodeManager::GetMergedTxnCounters(epoch, EpochGetNodeId());
    uint64_t applied_rows = 0;
    uint64_t applied_txns = 0;
    auto it_rows = g_recvdiag_applied_rows.find(epoch);
    if (it_rows != g_recvdiag_applied_rows.end()) {
        applied_rows = it_rows->second;
    }
    auto it_txns = g_recvdiag_applied_txns.find(epoch);
    if (it_txns != g_recvdiag_applied_txns.end()) {
        applied_txns = it_txns->second;
    }
    bool recv_done = E_NodeManager::IsNodeReceiveCompleted(epoch, from_node);
    const char* status = (expected_pack == received) ? "OK" : "MISMATCH";

    char buf[512];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "RECV_%s epoch=%lu peer=%u expected=%lu received=%lu merged=%lu "
        "applied_txns=%lu applied_rows=%lu recv_done=%d",
        status,
        epoch,
        from_node,
        expected_pack,
        received,
        merged,
        applied_txns,
        applied_rows,
        recv_done ? 1 : 0);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);

    if (expected_pack == received && recv_done) {
        g_txn_request_retry_tick.erase(epoch);
        return;
    }
    if (expected_pack != received) {
        int& tick = g_txn_request_retry_tick[epoch];
        if (++tick % 50 != 1) {
            return;
        }
        EpochPublishTxnRequest(epoch);
        char req[128];
        int req_rc;
        if (expected_pack > received) {
            req_rc = snprintf_s(req, sizeof(req), sizeof(req) - 1,
                "TXN_REQ epoch=%lu peer=%u missing=%lu",
                epoch, from_node, expected_pack - received);
        } else {
            req_rc = snprintf_s(req, sizeof(req), sizeof(req) - 1,
                "TXN_REQ epoch=%lu peer=%u excess=%lu",
                epoch, from_node, received - expected_pack);
        }
        securec_check_ss(req_rc, "\0", "\0");
        EpochRecvDiagAppendLine(req);
    }
}

void EpochPublishTxnRequest(uint64_t epoch)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 || epoch == 0) {
        return;
    }
    E_WireTxn req;
    req.csn = EPOCH_WIRE_TXN_REQUEST_CSN;
    req.start_epoch = 0;
    req.commit_epoch = epoch;
    req.server_id = EpochGetNodeId();
    req.txn_state = EPOCH_TXN_EMPTY;
    req.rows.clear();
    std::string payload;
    if (E_WireSerializeTxn(req, &payload)) {
        (void)EpochNetPublish(payload);
        EpochPerfNoteTxnRequestSend();
    }
}

void EpochHandleTxnRequest(const E_WireTxn& txn)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    uint64_t epoch = txn.commit_epoch;
    if (epoch == 0) {
        return;
    }
    uint32_t self = EpochGetNodeId();
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    bool sealed_now = E_NodeManager::IsNodeSendCompleted(epoch, self);
    /*
     * logical 推进后 ClearEpochSlot 会清掉 send_completed，但 worker 内 wire cache 仍保留。
     * epoch < logical 表示本节点已关窗，允许重播（含 pack=0 的空 epoch）。
     */
    bool can_replay = sealed_now || epoch < logical;
    if (!can_replay && epoch == logical) {
        EpochMaybeInvalidateStaleSeal(epoch);
        (void)EpochTrySealEpoch(epoch);
        sealed_now = E_NodeManager::IsNodeSendCompleted(epoch, self);
        can_replay = sealed_now || epoch < logical;
    }
    if (!can_replay) {
        char buf[160];
        int rc = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            "TXN_REQ_IGNORE epoch=%lu from_peer=%u reason=not_sealed",
            epoch,
            txn.server_id);
        securec_check_ss(rc, "\0", "\0");
        EpochRecvDiagAppendLine(buf);
        return;
    }
    EpochPipelineRebroadcastCachedTxns(epoch);
    E_WireTxn end_wire;
    end_wire.csn = sealed_now ? E_NodeManager::GetLocalPackTxnCounters(epoch, self)
                              : EpochPipelineCachedPackCount(epoch);
    end_wire.start_epoch = 0;
    end_wire.commit_epoch = epoch;
    end_wire.server_id = self;
    end_wire.txn_state = EPOCH_TXN_EMPTY;
    end_wire.rows.clear();
    {
        std::string payload;
        if (E_WireSerializeTxn(end_wire, &payload)) {
            (void)EpochNetPublish(payload);
        }
    }
    char buf[128];
    int rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1,
        "TXN_REQ_REPLY epoch=%lu from_peer=%u", epoch, txn.server_id);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagOnApply(uint64_t epoch, uint64_t row_count, bool ok)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    const char* tag = ok ? "APPLY" : "APPLY_FAIL";
    if (ok) {
        g_recvdiag_applied_rows[epoch] += row_count;
        g_recvdiag_applied_txns[epoch] += 1;
    }

    char buf[256];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "%s epoch=%lu rows=%lu total_applied_txns=%lu total_applied_rows=%lu",
        tag,
        epoch,
        row_count,
        ok ? g_recvdiag_applied_txns[epoch] : 0UL,
        ok ? g_recvdiag_applied_rows[epoch] : 0UL);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogApplyIssue(uint64_t epoch, const char* issue, uint64_t merged, uint64_t build_rows)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 || issue == nullptr) {
        return;
    }
    char buf[192];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "APPLY_ISSUE epoch=%lu issue=%s merged=%lu build_rows=%lu",
        epoch,
        issue,
        merged,
        build_rows);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogApplySkip(uint64_t epoch, const char* reason, uint64_t merged, uint64_t committed,
    uint64_t peer_should, uint64_t peer_rcv, int has_remote_rows)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 || reason == nullptr) {
        return;
    }
    char buf[256];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "APPLY_SKIP epoch=%lu reason=%s merged=%lu committed=%lu peer_should=%lu peer_rcv=%lu remote_rows=%d",
        epoch,
        reason,
        merged,
        committed,
        peer_should,
        peer_rcv,
        has_remote_rows);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogMergePkFail(uint32_t relid, uint32_t local_relid, const char* relname, EpochOpType op)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    char buf[192];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "MERGE_PK_FAIL relid=%u local_relid=%u relname=%s op=%u",
        relid,
        local_relid,
        (relname != nullptr && relname[0] != '\0') ? relname : "?",
        static_cast<unsigned>(op));
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogStall(uint64_t epoch, const char* reason)
{
    EpochPerfNoteLogicalStall(reason);
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 || reason == nullptr) {
        return;
    }
    g_recvdiag_stall_count++;
    if (epoch == g_recvdiag_last_stall_epoch && g_recvdiag_stall_count % 100 != 1) {
        return;
    }
    g_recvdiag_last_stall_epoch = epoch;

    uint32_t self = EpochGetNodeId();
    char buf[512];
    uint32_t node_id = EpochGetNodeId();
    if (strcmp(reason, "commit_path_incomplete") == 0 || strcmp(reason, "record_incomplete") == 0 ||
        strcmp(reason, "local_commit_pending") == 0 || strcmp(reason, "merge_incomplete") == 0) {
        uint64_t expect = 0;
        uint64_t received = 0;
        for (uint32_t n = 0; n < EpochGetTotalNodes(); ++n) {
            if (n == self) {
                continue;
            }
            expect += E_NodeManager::GetStoreShouldReceivedPackCounters(epoch, n);
            received += E_NodeManager::GetStoreReceivedPackCounters(epoch, n);
        }
        int rc = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            "STALL epoch=%lu reason=%s pack=%lu prod=%lu rec=%lu rcd=%lu chg=%lu read=%lu abort=%lu merged=%lu committed=%lu peer_expect=%lu peer_rcv=%lu remote_exec=%d",
            epoch,
            reason,
            E_NodeManager::GetLocalPackTxnCounters(epoch, node_id),
            E_NodeManager::GetLocalProduceTxnCounters(epoch, node_id),
            E_NodeManager::GetLocalRecordCommitCounters(epoch, node_id),
            E_NodeManager::GetLocalRecordCommittedCounters(epoch, node_id),
            E_NodeManager::GetChangeSetTxnCounters(epoch, node_id),
            E_NodeManager::GetReadCommittedTxnCounters(epoch, node_id),
            E_NodeManager::GetAbortTxnCounters(epoch, node_id),
            E_NodeManager::GetMergedTxnCounters(epoch, node_id),
            E_NodeManager::GetCommittedTxnCounters(epoch, node_id),
            expect,
            received,
            E_NodeManager::IsRemoteExeced() ? 1 : 0);
        securec_check_ss(rc, "\0", "\0");
    } else {
        int offset = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            "STALL epoch=%lu reason=%s self_send=%d self_recv=%d",
            epoch,
            reason,
            E_NodeManager::IsNodeSendCompleted(epoch, self) ? 1 : 0,
            E_NodeManager::IsNodeReceiveCompleted(epoch, self) ? 1 : 0);
        securec_check_ss(offset, "\0", "\0");
        for (uint32_t peer = 0; peer < EpochGetTotalNodes(); ++peer) {
            if (peer == self) {
                continue;
            }
            int rc = snprintf_s(buf + offset,
                sizeof(buf) - static_cast<size_t>(offset),
                sizeof(buf) - static_cast<size_t>(offset) - 1,
                " p%u_s=%d p%u_r=%d",
                peer,
                E_NodeManager::IsNodeSendCompleted(epoch, peer) ? 1 : 0,
                peer,
                E_NodeManager::IsNodeReceiveCompleted(epoch, peer) ? 1 : 0);
            securec_check_ss(rc, "\0", "\0");
            offset += rc;
            if (offset >= static_cast<int>(sizeof(buf)) - 1) {
                break;
            }
        }
    }
    EpochRecvDiagAppendLine(buf);

    if (strcmp(reason, "recv_incomplete") == 0 && g_recvdiag_stall_count % 200 == 1) {
        uint32_t incomplete_peer = self;
        for (uint32_t peer = 0; peer < EpochGetTotalNodes(); ++peer) {
            if (peer != self && !E_NodeManager::IsNodeReceiveCompleted(epoch, peer)) {
                incomplete_peer = peer;
                break;
            }
        }
        if (incomplete_peer != self) {
            EpochPublishTxnRequest(epoch);
            char req[96];
            int req_rc = snprintf_s(req, sizeof(req), sizeof(req) - 1,
                "TXN_REQ_SENT epoch=%lu peer=%u", epoch, incomplete_peer);
            securec_check_ss(req_rc, "\0", "\0");
            EpochRecvDiagAppendLine(req);
        }
    }
}

void EpochDiagMaybeReport(void)
{
    if (!g_enable_epoch_multimaster || !g_epoch_diag) {
        return;
    }

    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    if (g_last_diag_physical != 0 && physical <= g_last_diag_physical + 99) {
        return;
    }
    g_last_diag_physical = physical;

    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t gap_pl = (physical >= logical) ? (physical - logical) : 0;

    char path[MAXPGPATH];
    if (!EpochFormatLogPath(path, sizeof(path), "epoch_diag")) {
        return;
    }

    FILE* fp = fopen(path, "a");
    if (fp == nullptr) {
        return;
    }

    time_t now = time(nullptr);
    struct tm tm_buf;
    (void)localtime_r(&now, &tm_buf);
    char ts[32];
    (void)strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_buf);

    fprintf(fp,
            "[%s] physical=%lu logical=%lu gap_phys_logical=%lu\n",
            ts,
            physical,
            logical,
            gap_pl);

    uint32_t total = EpochGetTotalNodes();
    uint64_t start = (logical > 5) ? (logical - 5) : 1;
    uint64_t end = logical + 1;
    if (end > start + 24) {
        start = end - 24;
    }

    uint32_t self = EpochGetNodeId();
    for (uint64_t epoch = start; epoch < end; ++epoch) {
        fprintf(fp, "  epoch=%lu", epoch);
        for (uint32_t node = 0; node < total; ++node) {
            fprintf(fp,
                    " n%u:chg=%lu pack=%lu prod=%lu rec=%lu rcd=%lu merged=%lu cmt=%lu should=%lu rcv=%lu",
                    node,
                    E_NodeManager::GetChangeSetTxnCounters(epoch, node),
                    E_NodeManager::GetLocalPackTxnCounters(epoch, node),
                    E_NodeManager::GetLocalProduceTxnCounters(epoch, node),
                    E_NodeManager::GetLocalRecordCommitCounters(epoch, node),
                    E_NodeManager::GetLocalRecordCommittedCounters(epoch, node),
                    E_NodeManager::GetMergedTxnCounters(epoch, node),
                    E_NodeManager::GetCommittedTxnCounters(epoch, node),
                    E_NodeManager::GetStoreShouldReceivedPackCounters(epoch, node),
                    E_NodeManager::GetStoreReceivedPackCounters(epoch, node));
        }
        fprintf(fp,
                " send_done=%d recv_done=%d\n",
                E_NodeManager::IsNodeSendCompleted(epoch, self) ? 1 : 0,
                E_NodeManager::IsNodeReceiveCompleted(epoch, self) ? 1 : 0);

        uint64_t self_pack = E_NodeManager::GetLocalPackTxnCounters(epoch, self);
        uint64_t self_merged = E_NodeManager::GetMergedTxnCounters(epoch, self);
        uint64_t self_committed = E_NodeManager::GetCommittedTxnCounters(epoch, self);
        uint64_t applied_txns = 0;
        uint64_t applied_rows = 0;
        auto it_txns = g_recvdiag_applied_txns.find(epoch);
        if (it_txns != g_recvdiag_applied_txns.end()) {
            applied_txns = it_txns->second;
        }
        auto it_rows = g_recvdiag_applied_rows.find(epoch);
        if (it_rows != g_recvdiag_applied_rows.end()) {
            applied_rows = it_rows->second;
        }
        fprintf(fp,
                "  SYNC epoch=%lu self=%u self_pack=%lu self_merged=%lu self_cmt=%lu applied_txns=%lu applied_rows=%lu",
                epoch,
                self,
                self_pack,
                self_merged,
                self_committed,
                applied_txns,
                applied_rows);
        for (uint32_t peer = 0; peer < total; ++peer) {
            if (peer == self) {
                continue;
            }
            uint64_t peer_pack = E_NodeManager::GetLocalPackTxnCounters(epoch, peer);
            uint64_t should = E_NodeManager::GetStoreShouldReceivedPackCounters(epoch, peer);
            uint64_t rcv = E_NodeManager::GetStoreReceivedPackCounters(epoch, peer);
            int pack_eq_rcv = (peer_pack == rcv) ? 1 : 0;
            int should_eq_rcv = (should == rcv) ? 1 : 0;
            fprintf(fp,
                    " peer%u:pack=%lu should=%lu rcv=%lu send=%d recv=%d pack_eq_rcv=%d should_eq_rcv=%d",
                    peer,
                    peer_pack,
                    should,
                    rcv,
                    E_NodeManager::IsNodeSendCompleted(epoch, peer) ? 1 : 0,
                    E_NodeManager::IsNodeReceiveCompleted(epoch, peer) ? 1 : 0,
                    pack_eq_rcv,
                    should_eq_rcv);
        }
        int merged_eq_cmt = (self_merged == self_committed) ? 1 : 0;
        fprintf(fp, " merged_eq_cmt=%d\n", merged_eq_cmt);
    }
    fprintf(fp, "\n");
    (void)fclose(fp);
}
