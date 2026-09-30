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
#include "epoch/e_epoch.h"
#include "epoch/e_epoch_lifecycle.h"
#include "epoch/e_concurrent_map.h"
#include "epoch/e_apply_shmem.h"
#include "epoch/e_merge_shmem.h"

#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "knl/knl_variable.h"
#include "knl/knl_thread.h"
#include "portability/instr_time.h"

#include <cstdio>
#include <cstring>
#include <ctime>
#include <unordered_map>
#include <unordered_set>

static uint64_t g_last_diag_physical = 0;
// HCJ ADDBY: GeoGauss aum::concurrent_unordered_map 风格分片 map，供多 apply worker 并发计数。
static EpochConcurrent::U64Map g_recvdiag_applied_rows;
static EpochConcurrent::U64Map g_recvdiag_applied_txns;
static uint64_t g_recvdiag_last_stall_epoch = 0;
static uint64_t g_recvdiag_stall_count = 0;
static uint64_t g_recvdiag_logical_advance_ok_count = 0;

/* recv_diag / epoch_diag 采样间隔（verbose 模式仍使用） */
static const int EPOCH_RECV_DIAG_STALL_SAMPLE = 500;
static const int EPOCH_RECV_DIAG_LOGICAL_ADVANCE_SAMPLE = 100;
static const uint64_t EPOCH_RECV_DIAG_LOGICAL_ADVANCE_SLOW_US = 100000;
static const int EPOCH_DIAG_PHYSICAL_INTERVAL = 500;
static const int EPOCH_DIAG_STALL_SNAPSHOT_MIN_INTERVAL_SEC = 1;
static uint64_t g_diag_stall_snapshot_key_epoch = 0;
static char g_diag_stall_snapshot_key_reason[32] = {0};
static time_t g_diag_stall_snapshot_last_ts = 0;

static bool EpochDiagShouldLogStallSnapshot(uint64_t epoch, const char* reason, uint64_t pack, uint64_t executing,
    uint64_t expected_merge)
{
    if (reason == nullptr) {
        return false;
    }
    /* 空 epoch 空转：无 pack/executing/peer_should，跳过（避免追 logical 时刷爆 epoch_diag） */
    if (pack == 0 && executing == 0 && expected_merge == 0) {
        return false;
    }

    time_t now = time(nullptr);
    bool same_key = (epoch == g_diag_stall_snapshot_key_epoch &&
                     strncmp(reason, g_diag_stall_snapshot_key_reason, sizeof(g_diag_stall_snapshot_key_reason)) == 0);
    if (same_key && g_diag_stall_snapshot_last_ts != 0 &&
        (now - g_diag_stall_snapshot_last_ts) < EPOCH_DIAG_STALL_SNAPSHOT_MIN_INTERVAL_SEC) {
        return false;
    }

    g_diag_stall_snapshot_key_epoch = epoch;
    errno_t rc = strncpy_s(g_diag_stall_snapshot_key_reason,
        sizeof(g_diag_stall_snapshot_key_reason),
        reason,
        sizeof(g_diag_stall_snapshot_key_reason) - 1);
    securec_check(rc, "\0", "\0");
    g_diag_stall_snapshot_last_ts = now;
    return true;
}
static std::atomic<uint64_t> g_peer_max_sealed_epoch[EPOCH_MAX_NODES] = {};

static uint64_t EpochComputeShouldSend(uint64_t epoch, uint32_t node_id);
static void EpochRecvDiagAppendLine(const char* line);

static inline bool EpochRecvDiagVerboseEnabled(void)
{
    return g_epoch_recv_diag && g_epoch_recv_diag_verbose;
}

static inline bool EpochRecvDiagRoutineTag(const char* tag)
{
    return tag != nullptr &&
           (strcmp(tag, "ENTER") == 0 || strcmp(tag, "PROCESSED") == 0 || strcmp(tag, "ARRIVE") == 0);
}

struct EpochLogicalAdvanceTraceState {
    uint64_t epoch;
    instr_time started;
    bool active;
    uint64_t stall_recv;
    uint64_t stall_send;
    uint64_t stall_other;
};

static EpochLogicalAdvanceTraceState g_logical_advance_trace = {0, {}, false, 0, 0, 0};

void EpochLogicalAdvanceTraceReset(uint64_t epoch)
{
    if (g_logical_advance_trace.active && g_logical_advance_trace.epoch == epoch) {
        return;
    }
    g_logical_advance_trace.epoch = epoch;
    INSTR_TIME_SET_CURRENT(g_logical_advance_trace.started);
    g_logical_advance_trace.active = true;
    g_logical_advance_trace.stall_recv = 0;
    g_logical_advance_trace.stall_send = 0;
    g_logical_advance_trace.stall_other = 0;
}

void EpochLogicalAdvanceTraceNoteStall(const char* reason)
{
    if (!g_logical_advance_trace.active || reason == nullptr) {
        return;
    }
    if (strcmp(reason, "send_incomplete") == 0) {
        g_logical_advance_trace.stall_send++;
    } else if (strcmp(reason, "peer_not_connected") == 0 || strcmp(reason, "peer_lag") == 0) {
        g_logical_advance_trace.stall_recv++;
    } else {
        g_logical_advance_trace.stall_other++;
    }
}

void EpochLogicalAdvanceTraceOnOk(uint64_t epoch)
{
    if (!g_logical_advance_trace.active || g_logical_advance_trace.epoch != epoch) {
        return;
    }
    instr_time end;
    INSTR_TIME_SET_CURRENT(end);
    INSTR_TIME_SUBTRACT(end, g_logical_advance_trace.started);
    uint64_t elapsed_us = static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(end));
    if (EpochRecvDiagVerboseEnabled()) {
        EpochRecvDiagLogLogicalAdvanceOk(epoch,
            elapsed_us,
            g_logical_advance_trace.stall_recv,
            g_logical_advance_trace.stall_send,
            g_logical_advance_trace.stall_other);
    }
    g_logical_advance_trace.active = false;
}

const char* EpochHandshakeSendSubReason(uint64_t epoch)
{
    uint32_t total = EpochGetTotalNodes();
    for (uint32_t node = 0; node < total; ++node) {
        if (!E_NodeManager::IsNodeSendCompleted(epoch, node)) {
            if (node == EpochGetNodeId()) {
                return "self_not_sealed";
            }
            return "peer_not_sealed";
        }
    }
    return "unknown";
}

void EpochRecvDiagLogHandshakeStall(uint64_t epoch, const char* reason, const char* sub_reason)
{
    if (!g_epoch_recv_diag || reason == nullptr || sub_reason == nullptr) {
        return;
    }
    uint32_t self = EpochGetNodeId();
    uint32_t total = EpochGetTotalNodes();
    char buf[640];
    int offset = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "HANDSHAKE_STALL epoch=%lu reason=%s sub=%s physical=%lu logical=%lu self_send=%d self_recv=%d exec=%lu pack=%lu should=%lu",
        epoch,
        reason,
        sub_reason,
        E_NodeManager::GetPhysicalEpoch(),
        E_NodeManager::GetLogicalEpoch(),
        E_NodeManager::IsNodeSendCompleted(epoch, self) ? 1 : 0,
        E_NodeManager::IsNodeReceiveCompleted(epoch, self) ? 1 : 0,
        E_NodeManager::GetExecutingTxnCounters(epoch, self),
        E_NodeManager::GetLocalPackTxnCounters(epoch, self),
        EpochComputeShouldSend(epoch, self));
    securec_check_ss(offset, "\0", "\0");
    for (uint32_t peer = 0; peer < total; ++peer) {
        if (peer == self) {
            continue;
        }
        int rc = snprintf_s(buf + offset,
            sizeof(buf) - static_cast<size_t>(offset),
            sizeof(buf) - static_cast<size_t>(offset) - 1,
            " p%u_send=%d p%u_rcv_done=%d p%u_should=%lu p%u_got=%lu p%u_max_sealed=%lu",
            peer,
            E_NodeManager::IsNodeSendCompleted(epoch, peer) ? 1 : 0,
            peer,
            E_NodeManager::IsNodeReceiveCompleted(epoch, peer) ? 1 : 0,
            peer,
            E_NodeManager::GetStoreShouldReceivedPackCounters(epoch, peer),
            peer,
            E_NodeManager::GetStoreReceivedPackCounters(epoch, peer),
            peer,
            EpochGetPeerMaxSealedEpoch(peer));
        securec_check_ss(rc, "\0", "\0");
        offset += rc;
        if (offset >= static_cast<int>(sizeof(buf)) - 1) {
            break;
        }
    }
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogLogicalAdvanceOk(uint64_t epoch, uint64_t elapsed_us, uint64_t stall_recv,
    uint64_t stall_send, uint64_t stall_other)
{
    if (!g_epoch_recv_diag) {
        return;
    }
    g_recvdiag_logical_advance_ok_count++;
    if (elapsed_us < EPOCH_RECV_DIAG_LOGICAL_ADVANCE_SLOW_US &&
        g_recvdiag_logical_advance_ok_count % EPOCH_RECV_DIAG_LOGICAL_ADVANCE_SAMPLE != 1) {
        return;
    }
    char buf[256];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "LOGICAL_ADVANCE_OK epoch=%lu elapsed_us=%lu stall_recv=%lu stall_send=%lu stall_other=%lu physical=%lu",
        epoch,
        elapsed_us,
        stall_recv,
        stall_send,
        stall_other,
        E_NodeManager::GetPhysicalEpoch());
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

bool EpochIsSyncBackgroundWorkerCommit(void)
{
    return g_enable_epoch_multimaster && EpochGetTotalNodes() > 1;
}

bool EpochShouldSkipBackendWal(void)
{
    /*
     * sync 多主 TPCC 客户端：跳过 COMMIT/ABORT WAL 与 XLogWaitFlush；heap 持久化由 apply worker 完成。
     * 不阻止 AssignTransactionId（SELECT FOR UPDATE / heap_lock_tuple 仍需 XID）。
     * RecordTransactionCommit/Abort 仍推进内存 CLOG/CSN（无 WAL），供 heap xmax 可见性判断。
     */
    if (!EpochIsSyncBackgroundWorkerCommit()) {
        return false;
    }
    if (IsAutoVacuumWorkerProcess() || AmEpochWorkerProcess()) {
        return false;
    }
    if (t_thrd.role == EPOCHWORKER || t_thrd.role == EPOCHAPPLYWORKER || t_thrd.role == EPOCHMERGEWORKER ||
        t_thrd.role == EPOCHPACKWORKER || t_thrd.role == EPOCHSENDWORKER ||
        t_thrd.role == EPOCHTIPFLUSHER) {
        return false;
    }
    return true;
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
 * 较快节点不可比任一 peer 已 seal 的 epoch 超前太多就关窗，否则对端
 * FUTURE_DROP / 来不及处理会导致 peer should 永为 0。
 * 允许超前至多 gap（与 seal-ahead 上界、FUTURE 接收窗口 2*gap+1 对齐）。
 */
static bool EpochSealBlockedByPeerLag(uint64_t epoch, uint32_t node_id)
{
    if (EpochGetTotalNodes() <= 1) {
        return false;
    }
    uint64_t slack = 1;
    if (!EpochIsPhysLogicalGapDisabled()) {
        slack = EpochGetPhysLogicalGapLimit();
        if (slack < 1) {
            slack = 1;
        }
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
        if (peer_max == 0) {
            /* 尚未收到任何对端 seal：只允许 seal 到 logical（由调用方保证） */
            uint64_t logical = E_NodeManager::GetLogicalEpoch();
            if (epoch > logical) {
                return true;
            }
            continue;
        }
        if (epoch > peer_max + slack) {
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
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    if (txn.rows.empty()) {
        return;
    }
    char buf[192];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "SEND_TXN epoch=%lu csn=%lu from=%u rows=%zu",
        txn.epoch,
        txn.csn,
        txn.server_id,
        txn.rows.size());
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochWireDiagLogTxnRecv(const E_WireTxn& txn)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    if (txn.rows.empty()) {
        return;
    }
    char buf[192];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "RECV_TXN epoch=%lu csn=%lu from=%u rows=%zu",
        txn.epoch,
        txn.csn,
        txn.server_id,
        txn.rows.size());
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogHello(const char* tag, uint32_t from_node)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 ||
        tag == nullptr) {
        return;
    }
    char buf[64];
    int rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "%s from=%u", tag, from_node);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogEpochEndSend(const char* result, uint64_t epoch, uint64_t wire_bytes)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 || result == nullptr) {
        return;
    }
    if (!EpochRecvDiagVerboseEnabled() && strcmp(result, "FAIL") != 0) {
        return;
    }
    char buf[128];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "EPOCH_END_SEND %s epoch=%lu bytes=%lu",
        result,
        epoch,
        wire_bytes);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

bool EpochBuildEpochEndWire(uint64_t epoch, E_WireTxn* out)
{
    if (out == nullptr) {
        return false;
    }
    /*
     * 本地 epoch-end：仅反映本节点 seal 时刻的 pack 总数（对端据此得 should_receive）。
     * 不依赖 peer 状态；关窗条件见 EpochTrySealEpoch（executing/pack/physical 等本地项）。
     */
    uint32_t self = EpochGetNodeId();
    uint64_t sealed = E_NodeManager::GetSealedPackTxnCounters(epoch, self);
    out->csn = (sealed > 0) ? sealed : E_NodeManager::GetLocalPackTxnCounters(epoch, self);
    out->epoch = epoch;
    out->server_id = self;
    out->txn_state = EPOCH_TXN_EMPTY;
    out->rows.clear();
    return true;
}

void EpochOnEpochEndWireReceived(const E_WireTxn& txn)
{
    if (!E_WireTxnIsEpochEnd(txn)) {
        return;
    }
    uint64_t epoch = txn.epoch;
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint32_t from_node = txn.server_id;
    uint64_t pack_count = txn.csn;
    uint64_t prev_should = E_NodeManager::GetStoreShouldReceivedPackCounters(epoch, from_node);
    bool already_send = E_NodeManager::IsNodeSendCompleted(epoch, from_node);
    int recv_done = E_NodeManager::IsNodeReceiveCompleted(epoch, from_node) ? 1 : 0;

    EpochRecvDiagLogEpochEndEvent("ENTER",
        epoch,
        from_node,
        pack_count,
        logical,
        already_send ? 1 : 0,
        prev_should,
        recv_done);

    /*
     * 双节点 ZMQ 到达顺序不确定，较快节点可能先把 epoch-end 发到较慢节点尚未关窗的 epoch
     * （epoch > logical）或已关窗的 epoch（epoch < logical）。
     * seal-ahead 下发送方最多 seal 到 sender_logical+gap；若接收方 logical 落后最多约 gap，
     * 则到达 epoch 可达 receiver_logical + 2*gap。旧窗口 logical+gap+1 会 FUTURE_DROP
     * 合法超前 seal → peer should 永为 0 → 双方 merge_incomplete 死锁。
     */
    if (epoch + 1 < logical) {
        EpochRecvDiagLogEpochEndEvent("STALE",
            epoch,
            from_node,
            pack_count,
            logical,
            already_send ? 1 : 0,
            prev_should,
            recv_done);
    } else if (!EpochIsPhysLogicalGapDisabled()) {
        const uint64_t gap = EpochGetPhysLogicalGapLimit();
        const uint64_t accept_ahead = (gap * 2) + 1;
        if (epoch > logical + accept_ahead) {
            EpochRecvDiagLogEpochEndEvent("FUTURE_DROP",
                epoch,
                from_node,
                pack_count,
                logical,
                already_send ? 1 : 0,
                prev_should,
                recv_done);
            return;
        }
    }

    E_NodeManager::EnsureEpochSlot(epoch);
    prev_should = E_NodeManager::GetStoreShouldReceivedPackCounters(epoch, from_node);
    already_send = E_NodeManager::IsNodeSendCompleted(epoch, from_node);
    /*
     * 对端 epoch-end 重传时 send_completed 可能已为 1；若仍 recv 不齐须更新 should_receive
     * 并重新判定 recv，不能因 early return 丢弃。
     * 本端 reopen seal 后 pack 数增加时，也须用更大的 pack_count 刷新对端 should_receive。
     */
    if (already_send && pack_count == prev_should) {
        EpochPipelineResyncPeerReceived(epoch, from_node);
        EpochNotePeerSealedEpoch(from_node, epoch);
        EpochPipelineNotePeerEpochEndReceived(epoch, from_node);
        recv_done = EpochCheckAndSetNodeReceiveCompleted(epoch, from_node) ? 1 : 0;
        EpochPipelineFinishPeerRecvAfterEpochEnd(epoch, from_node);
        EpochRecvDiagLogEpochEndEvent("DUP",
            epoch,
            from_node,
            pack_count,
            logical,
            1,
            prev_should,
            recv_done);
        EpochRecvDiagLogEpochEnd(epoch, from_node, pack_count);
        return;
    }
    if (already_send && pack_count < prev_should) {
        EpochPipelineResyncPeerReceived(epoch, from_node);
        EpochNotePeerSealedEpoch(from_node, epoch);
        EpochPipelineNotePeerEpochEndReceived(epoch, from_node);
        recv_done = EpochCheckAndSetNodeReceiveCompleted(epoch, from_node) ? 1 : 0;
        EpochPipelineFinishPeerRecvAfterEpochEnd(epoch, from_node);
        EpochRecvDiagLogEpochEndEvent("RESYNC",
            epoch,
            from_node,
            pack_count,
            logical,
            1,
            prev_should,
            recv_done);
        EpochRecvDiagLogEpochEnd(epoch, from_node, pack_count);
        return;
    }

    E_NodeManager::SetStoreShouldReceivedPackCounters(epoch, from_node, pack_count);
    /* 对端 epoch-end：写入 should；与本地 seal/send 无关 */
    E_NodeManager::SetLocalPackTxnCounters(epoch, from_node, pack_count);
    E_NodeManager::SetNodeSendCompleted(epoch, from_node, true);
    /*
     * 重 seal 抬高 should 时，slot 可能已被 Clear 导致 received 归零，
     * 但 dedupe 会丢弃重传 txn；用 g_peer_received_counts 恢复 received。
     */
    EpochPipelineResyncPeerReceived(epoch, from_node);
    EpochNotePeerSealedEpoch(from_node, epoch);
    EpochPipelineNotePeerEpochEndReceived(epoch, from_node);
    recv_done = EpochCheckAndSetNodeReceiveCompleted(epoch, from_node) ? 1 : 0;
    EpochPipelineFinishPeerRecvAfterEpochEnd(epoch, from_node);
    EpochRecvDiagLogEpochEndEvent("PROCESSED",
        epoch,
        from_node,
        pack_count,
        logical,
        0,
        prev_should,
        recv_done);
    EpochRecvDiagLogEpochEnd(epoch, from_node, pack_count);
    EpochTraceMaybeLogPeerReceived(epoch);
}

bool EpochCheckAndSetNodeReceiveCompleted(uint64_t epochId, uint64_t nodeId)
{
    uint32_t self = EpochGetNodeId();
    uint64_t should_receive = E_NodeManager::GetStoreShouldReceivedPackCounters(epochId, nodeId);
    uint64_t received = E_NodeManager::GetStoreReceivedPackCounters(epochId, nodeId);

    /*
     * 对端 recv：须已知 should（对端 epoch-end 写入）再判 received==should。
     * 仅用于 wire txn / 收包路径；本地 send/seal 不走此函数。
     */
    if (nodeId != self && !EpochPipelinePeerEpochEndReceived(epochId, static_cast<uint32_t>(nodeId))) {
        E_NodeManager::SetNodeReceiveCompleted(epochId, nodeId, false);
        return false;
    }

    if (should_receive > 0 && received > should_receive) {
        ereport(WARNING,
            (errmsg("epoch recv: received %lu > should %lu epoch %lu peer %lu (duplicate wire?)",
                received,
                should_receive,
                epochId,
                nodeId)));
        E_NodeManager::SetNodeReceiveCompleted(epochId, nodeId, false);
        return false;
    }

    bool done = (should_receive == received);
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
    /* epoch-end 与 txn 同队列 FIFO drain，禁止旁路直发 ZMQ */
    if (!EpochTxnQueuePush(epoch, payload.data(), static_cast<uint32_t>(payload.size()))) {
        ereport(WARNING, (errmsg("epoch: failed to enqueue epoch-end marker for epoch %lu", epoch)));
        return false;
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

void EpochMaybeInvalidateStaleSeal(uint64_t epoch)
{
    /*
     * 单 seal 策略：seal 完成后不再因 packed>sealed 撤销 send_completed。
     * assign 后立即 executing++（含 START gate 自旋），seal 前须 executing==0。
     */
    (void)epoch;
}

static bool EpochSealBlockedByExecuting(uint64_t epoch, uint32_t node_id)
{
    if (E_NodeManager::GetExecutingTxnCounters(epoch, node_id) == 0) {
        return false;
    }
    /*
     * sync 多主：epoch 上仍有在途 backend（executing>0，含 gate 等待）时不 seal。
     */
    return true;
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
    uint64_t sealed = E_NodeManager::GetSealedPackTxnCounters(epoch, node_id);

    /*
     * 已 seal（含 epoch-end 已入队、wire 尚未发出）：等待主 worker drain 发包。
     */
    if (sealed > 0 && packed == sealed) {
        return true;
    }

    /*
     * 已 seal 且 send 完成：单 seal 策略下 packed 与 sealed 一致即完成。
     */
    if (E_NodeManager::IsNodeSendCompleted(epoch, node_id)) {
        if (packed == sealed) {
            return true;
        }
        EpochRecvDiagLogSealBlock(epoch, "sealed_pack_mismatch");
        return false;
    }

    /*
     * 关窗 physical 前提与 logical 推进一致：physical > epoch + delay（delay=0 时 physical>=epoch+1）。
     */
    if (!EpochIsPhysLogicalGapDisabled() && !EpochIsLogicalReady(epoch)) {
        EpochRecvDiagLogSealBlock(epoch, "physical_delay");
        return false;
    }

    if (EpochSealBlockedByPeerLag(epoch, node_id)) {
        EpochRecvDiagLogSealBlock(epoch, "peer_lag");
        return false;
    }

    if (EpochSealBlockedByExecuting(epoch, node_id)) {
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
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epoch, node_id);

    if (packed < should_send ||
        (should_send == 0 && packed == 0 && change_set > 0)) {
        EpochRecvDiagLogSealBlock(epoch, "change_set_pending");
        return false;
    }

    if (EpochSealBlockedByExecuting(epoch, node_id)) {
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
    EpochLifecycleNoteSeal(epoch);
    EpochPerfNoteSealOk();
    return true;
}

static bool EpochLocalEpochEndAlreadySent(uint64_t epoch)
{
    return E_NodeManager::IsNodeSendCompleted(epoch, EpochGetNodeId());
}

/*
 * 重播 pack 数：优先读 sealed；seal-ahead 下 epoch>=logical 的 slot 仍有效。
 * 仅当 epoch 远低于 logical、slot 可能已回收时退回 0。
 */
static uint64_t EpochReplayEpochEndPackCount(uint64_t epoch)
{
    uint32_t self = EpochGetNodeId();
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    if (epoch + 1 < logical) {
        return 0;
    }
    E_NodeManager::EnsureEpochSlot(epoch);
    uint64_t sealed = E_NodeManager::GetSealedPackTxnCounters(epoch, self);
    if (sealed > 0) {
        return sealed;
    }
    return E_NodeManager::GetLocalPackTxnCounters(epoch, self);
}

static bool EpochReplayEpochEndWireToPeer(uint32_t peerId, uint64_t epoch)
{
    E_WireTxn end_wire;
    end_wire.csn = EpochReplayEpochEndPackCount(epoch);
    end_wire.epoch = epoch;
    end_wire.server_id = EpochGetNodeId();
    end_wire.txn_state = EPOCH_TXN_EMPTY;
    end_wire.rows.clear();

    std::string payload;
    if (!E_WireSerializeTxn(end_wire, &payload)) {
        return false;
    }
    if (!EpochNetPublish(payload)) {
        return false;
    }
    EpochPerfNoteEpochEndSend(payload.size());
    if (g_epoch_recv_diag) {
        char buf[128];
        int rc = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            "REPLAY_EPOCH_END peer=%u epoch=%lu pack=%lu",
            peerId,
            epoch,
            end_wire.csn);
        securec_check_ss(rc, "\0", "\0");
        EpochRecvDiagAppendLine(buf);
    }
    return true;
}

/*
 * merge 卡在等 peer epoch-end 时补发本端已 seal 的 epoch-end（应对 FUTURE_DROP / 丢包）。
 * 对所有 peer 广播；pack 数取 sealed（含 seal-ahead 的 future epoch）。
 */
void EpochRetransmitSealedEpochEnd(uint64_t epoch)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 || epoch == 0) {
        return;
    }
    uint32_t self = EpochGetNodeId();
    E_NodeManager::EnsureEpochSlot(epoch);
    if (E_NodeManager::GetSealedPackTxnCounters(epoch, self) == 0 &&
        !E_NodeManager::IsNodeSendCompleted(epoch, self)) {
        return;
    }
    for (uint32_t peer = 0; peer < EpochGetTotalNodes(); ++peer) {
        if (peer == self || !EpochPeerIsConnected(peer)) {
            continue;
        }
        (void)EpochReplayEpochEndWireToPeer(peer, epoch);
    }
}

void EpochOnPeerConnected(uint32_t peerId)
{
    if (peerId == EpochGetNodeId() || EpochGetTotalNodes() <= 1) {
        return;
    }
    /*
     * 对端晚启动时补发 [1, replay_until] 已发出的 epoch-end（不重播 txn wire）。
     * 解决 stagger 后慢节点错过快节点一次性 PUB 的 seal 信号。
     * physical 常远超 logical（gap 顶满时 logical 卡住），仅 replay 到 logical 会漏掉
     * 已 SENT 的高 epoch seal 信号 → peer_should 永为 0 → merge 死锁。
     */
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    uint64_t replay_until = physical > logical ? physical : logical;
    uint32_t replayed = 0;
    for (uint64_t epoch = 1; epoch <= replay_until; ++epoch) {
        if (!EpochLocalEpochEndAlreadySent(epoch)) {
            continue;
        }
        if (EpochReplayEpochEndWireToPeer(peerId, epoch)) {
            replayed++;
        }
    }
    if (g_epoch_recv_diag) {
        char buf[160];
        int rc = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            "PEER_CONNECTED peer=%u logical=%lu physical=%lu replay_until=%lu replayed=%u",
            peerId,
            logical,
            physical,
            replay_until,
            replayed);
        securec_check_ss(rc, "\0", "\0");
        EpochRecvDiagAppendLine(buf);
    }
    if (g_epoch_recv_diag && replayed > 0) {
        char buf[96];
        int rc = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            "REPLAY_EPOCH_END_DONE peer=%u logical=%lu count=%u",
            peerId,
            logical,
            replayed);
        securec_check_ss(rc, "\0", "\0");
        EpochRecvDiagAppendLine(buf);
    }
}

void EpochDiagReset(void)
{
    g_last_diag_physical = 0;
    g_diag_stall_snapshot_key_epoch = 0;
    g_diag_stall_snapshot_key_reason[0] = '\0';
    g_diag_stall_snapshot_last_ts = 0;
}

void EpochRecvDiagReset(void)
{
    g_recvdiag_applied_rows.Clear();
    g_recvdiag_applied_txns.Clear();
    g_recvdiag_last_stall_epoch = 0;
    g_recvdiag_stall_count = 0;
    g_recvdiag_logical_advance_ok_count = 0;
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
    if (strcmp(reason, "physical_delay") == 0 || strcmp(reason, "exec_pending") == 0 ||
        strcmp(reason, "change_set_pending") == 0) {
        if (!EpochRecvDiagVerboseEnabled()) {
            return;
        }
    }
    if (!g_epoch_recv_diag) {
        return;
    }
    uint32_t node_id = EpochGetNodeId();
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epoch, node_id);
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    uint64_t aborts = E_NodeManager::GetAbortTxnCounters(epoch, node_id);
    uint64_t write_abort_after = E_NodeManager::GetWriteAbortAfterSendTxnCounters(epoch, node_id);
    uint64_t abort_net = (aborts > write_abort_after) ? (aborts - write_abort_after) : 0;
    uint64_t should_send = (change_set > abort_net) ? (change_set - abort_net) : 0;
    uint64_t executing = E_NodeManager::GetExecutingTxnCounters(epoch, node_id);

    char buf[384];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "SEAL_BLOCK epoch=%lu reason=%s chg=%lu pack=%lu executing=%lu abort=%lu w_abort=%lu should_send=%lu",
        epoch,
        reason,
        change_set,
        packed,
        executing,
        aborts,
        write_abort_after,
        should_send);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogBackendGate(const char* gate, uint64_t commit_epoch, uint64_t spins)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || gate == nullptr) {
        return;
    }
    uint32_t node_id = EpochGetNodeId();
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    uint64_t slot_exec = 0;
    if (commit_epoch > 0) {
        slot_exec = E_NodeManager::GetExecutingTxnCounters(commit_epoch, node_id);
    }
    char buf[512];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "BACKEND_GATE pid=%lu gate=%s commit_epoch=%lu logical=%lu physical=%lu "
        "assigned=%d exec_counted=%d deferred=%d slot_executing=%lu spins=%lu",
        static_cast<unsigned long>(t_thrd.proc_cxt.MyProcPid),
        gate,
        commit_epoch,
        logical,
        physical,
        u_sess->epoch_cxt.epoch_assigned ? 1 : 0,
        u_sess->epoch_cxt.epoch_exec_counted ? 1 : 0,
        u_sess->epoch_cxt.deferred_commit ? 1 : 0,
        slot_exec,
        spins);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogBackendGateDone(const char* gate, uint64_t commit_epoch, uint64_t spins,
    uint64_t logical_entry, uint64_t logical_exit, uint64_t wait_us)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || gate == nullptr || spins == 0) {
        return;
    }
    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    char buf[576];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "BACKEND_GATE_DONE pid=%lu gate=%s commit_epoch=%lu logical_entry=%lu logical_exit=%lu "
        "physical=%lu gap_at_assign=%lu spins=%lu wait_us=%lu assigned=%d exec_counted=%d",
        static_cast<unsigned long>(t_thrd.proc_cxt.MyProcPid),
        gate,
        commit_epoch,
        logical_entry,
        logical_exit,
        physical,
        (commit_epoch > logical_entry) ? (commit_epoch - logical_entry) : 0,
        spins,
        wait_us,
        u_sess->epoch_cxt.epoch_assigned ? 1 : 0,
        u_sess->epoch_cxt.epoch_exec_counted ? 1 : 0);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogBackendPhase(const char* phase, uint64_t commit_epoch)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || phase == nullptr) {
        return;
    }
    uint32_t node_id = EpochGetNodeId();
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t slot_exec = 0;
    uint64_t slot_pack = 0;
    if (commit_epoch > 0) {
        slot_exec = E_NodeManager::GetExecutingTxnCounters(commit_epoch, node_id);
        slot_pack = E_NodeManager::GetLocalPackTxnCounters(commit_epoch, node_id);
    }
    char buf[448];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "BACKEND_PHASE pid=%lu phase=%s commit_epoch=%lu logical=%lu "
        "assigned=%d exec_counted=%d deferred=%d slot_executing=%lu slot_pack=%lu",
        static_cast<unsigned long>(t_thrd.proc_cxt.MyProcPid),
        phase,
        commit_epoch,
        logical,
        u_sess->epoch_cxt.epoch_assigned ? 1 : 0,
        u_sess->epoch_cxt.epoch_exec_counted ? 1 : 0,
        u_sess->epoch_cxt.deferred_commit ? 1 : 0,
        slot_exec,
        slot_pack);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogPhysicalTxnLimit(uint64_t commit_epoch, uint64_t since, uint64_t limit, uint64_t spins)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || g_epoch_physical_txns_per_epoch <= 0) {
        return;
    }
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    char buf[512];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "BACKEND_GATE pid=%lu gate=PHYSICAL_TXN_LIMIT node=%u commit_epoch=%lu logical=%lu physical=%lu "
        "node_since=%lu limit=%lu spins=%lu",
        static_cast<unsigned long>(t_thrd.proc_cxt.MyProcPid),
        EpochGetNodeId(),
        commit_epoch,
        logical,
        physical,
        since,
        limit,
        spins);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogPhysicalTxnReserved(uint64_t commit_epoch, uint64_t slot_num, uint64_t limit)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || g_epoch_physical_txns_per_epoch <= 0) {
        return;
    }
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    char buf[512];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "BACKEND_PHASE pid=%lu phase=PHYSICAL_TXN_RESERVED node=%u commit_epoch=%lu logical=%lu physical=%lu "
        "node_slot=%lu/%lu",
        static_cast<unsigned long>(t_thrd.proc_cxt.MyProcPid),
        EpochGetNodeId(),
        commit_epoch,
        logical,
        physical,
        slot_num,
        limit);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogPhysicalAdvance(uint64_t from_epoch,
    uint64_t to_epoch,
    uint64_t since,
    uint64_t limit,
    int idle_force)
{
    if (!g_enable_epoch_multimaster || g_epoch_physical_txns_per_epoch <= 0 || !g_epoch_recv_diag) {
        return;
    }
    char buf[512];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "PHYSICAL_ADVANCE node=%u from=%lu to=%lu logical=%lu since=%lu limit=%lu idle_force=%d",
        EpochGetNodeId(),
        from_epoch,
        to_epoch,
        E_NodeManager::GetLogicalEpoch(),
        since,
        limit,
        idle_force);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogExecChange(int delta, const char* reason, uint64_t epoch)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || reason == nullptr) {
        return;
    }
    uint32_t node_id = EpochGetNodeId();
    uint64_t slot_exec = 0;
    if (epoch > 0) {
        slot_exec = E_NodeManager::GetExecutingTxnCounters(epoch, node_id);
    }
    char buf[512];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "BACKEND_EXEC pid=%lu delta=%+d reason=%s commit_epoch=%lu "
        "exec_counted=%d deferred=%d slot_executing=%lu",
        static_cast<unsigned long>(t_thrd.proc_cxt.MyProcPid),
        delta,
        reason,
        epoch,
        u_sess->epoch_cxt.epoch_exec_counted ? 1 : 0,
        u_sess->epoch_cxt.deferred_commit ? 1 : 0,
        slot_exec);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogXactHook(const char* event, const char* detail)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || event == nullptr) {
        return;
    }
    const char* detail_str = (detail != nullptr && detail[0] != '\0') ? detail : "-";
    uint64_t epoch = u_sess->epoch_cxt.txn_epoch;
    char buf[512];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "BACKEND_XACT pid=%lu event=%s detail=%s commit_epoch=%lu "
        "assigned=%d exec_counted=%d deferred=%d",
        static_cast<unsigned long>(t_thrd.proc_cxt.MyProcPid),
        event,
        detail_str,
        epoch,
        u_sess->epoch_cxt.epoch_assigned ? 1 : 0,
        u_sess->epoch_cxt.epoch_exec_counted ? 1 : 0,
        u_sess->epoch_cxt.deferred_commit ? 1 : 0);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogTxnProgress(const char* tag, const char* detail)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || tag == nullptr) {
        return;
    }
    const char* detail_str = (detail != nullptr && detail[0] != '\0') ? detail : "-";
    uint32_t node_id = EpochGetNodeId();
    uint64_t epoch = u_sess->epoch_cxt.txn_epoch;
    uint64_t slot_exec = (epoch > 0) ? E_NodeManager::GetExecutingTxnCounters(epoch, node_id) : 0;
    char buf[448];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "BACKEND_ACTIVE pid=%lu tag=%s detail=%s commit_epoch=%lu slot_executing=%lu",
        static_cast<unsigned long>(t_thrd.proc_cxt.MyProcPid),
        tag,
        detail_str,
        epoch,
        slot_exec);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogSent(uint64_t epoch, uint64_t sent_count)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
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

void EpochRecvDiagLogEpochEndEvent(const char* tag, uint64_t epoch, uint32_t from_node, uint64_t pack,
    uint64_t logical, int already_send, uint64_t prev_should, int recv_done)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 || tag == nullptr) {
        return;
    }
    if (EpochRecvDiagRoutineTag(tag) && !EpochRecvDiagVerboseEnabled()) {
        return;
    }
    char buf[384];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "EPOCH_END_%s epoch=%lu peer=%u pack=%lu logical=%lu already_send=%d prev_should=%lu recv_done=%d",
        tag,
        epoch,
        from_node,
        pack,
        logical,
        already_send,
        prev_should,
        recv_done);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogPeerEpochEndMark(uint64_t epoch, uint32_t from_node, uint64_t logical)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    bool peer_send = E_NodeManager::IsNodeSendCompleted(epoch, from_node);
    bool peer_recv = E_NodeManager::IsNodeReceiveCompleted(epoch, from_node);
    char buf[256];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "PEER_EPOCH_END_MARK epoch=%lu peer=%u logical=%lu peer_send=%d peer_recv=%d",
        epoch,
        from_node,
        logical,
        peer_send ? 1 : 0,
        peer_recv ? 1 : 0);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogPeerTxnWireDone(uint64_t epoch, uint32_t from_node, uint64_t should, uint64_t got)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    char buf[160];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "PEER_TXN_WIRE_DONE epoch=%lu peer=%u should=%lu got=%lu",
        epoch,
        from_node,
        should,
        got);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogMergePeerEndGate(uint64_t epoch, uint64_t logical, const char* peer_detail)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 ||
        peer_detail == nullptr) {
        return;
    }
    uint32_t self = EpochGetNodeId();
    bool self_send = E_NodeManager::IsNodeSendCompleted(epoch, self);
    bool self_recv = E_NodeManager::IsNodeReceiveCompleted(epoch, self);
    char buf[512];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "MERGE_PEER_END_GATE epoch=%lu logical=%lu self_send=%d self_recv=%d%s",
        epoch,
        logical,
        self_send ? 1 : 0,
        self_recv ? 1 : 0,
        peer_detail);
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
    uint64_t applied_rows = EpochConcurrent::U64Get(g_recvdiag_applied_rows, epoch, 0);
    uint64_t applied_txns = EpochConcurrent::U64Get(g_recvdiag_applied_txns, epoch, 0);
    bool recv_done = E_NodeManager::IsNodeReceiveCompleted(epoch, from_node);
    const char* status = (expected_pack == received) ? "OK" : "MISMATCH";
    if (strcmp(status, "OK") == 0 && !EpochRecvDiagVerboseEnabled()) {
        return;
    }

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

}

void EpochRecvDiagOnApply(uint64_t epoch, uint64_t row_count, bool ok)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    const char* tag = ok ? "APPLY" : "APPLY_FAIL";
    uint64_t total_applied_txns = 0;
    uint64_t total_applied_rows = 0;
    if (ok) {
        total_applied_rows = EpochConcurrent::U64FetchAdd(g_recvdiag_applied_rows, epoch, row_count);
        total_applied_txns = EpochConcurrent::U64FetchAdd(g_recvdiag_applied_txns, epoch, 1);
    }
    if (ok && !EpochRecvDiagVerboseEnabled()) {
        return;
    }

    char buf[256];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "%s epoch=%lu rows=%lu total_applied_txns=%lu total_applied_rows=%lu",
        tag,
        epoch,
        row_count,
        ok ? total_applied_txns : 0UL,
        ok ? total_applied_rows : 0UL);
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
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 ||
        reason == nullptr) {
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

void EpochRecvDiagLogApplyTrace(uint64_t epoch, const char* tag, uint64_t merged, uint64_t committed,
    uint64_t peer_should, uint64_t peer_rcv, int remote_work, int merger_found, int has_remote_rows,
    int has_rows, uint64_t build_rows)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 ||
        tag == nullptr) {
        return;
    }
    char buf[384];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "APPLY_TRACE epoch=%lu tag=%s merged=%lu committed=%lu peer_should=%lu peer_rcv=%lu "
        "remote_work=%d merger=%d has_remote_rows=%d has_rows=%d build_rows=%lu sync=%d",
        epoch,
        tag,
        merged,
        committed,
        peer_should,
        peer_rcv,
        remote_work,
        merger_found,
        has_remote_rows,
        has_rows,
        build_rows,
        1);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogApplyBegin(uint64_t epoch, uint64_t build_rows, uint64_t merged, uint64_t committed)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    char buf[192];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "APPLY_BEGIN epoch=%lu build_rows=%lu merged=%lu committed=%lu",
        epoch,
        build_rows,
        merged,
        committed);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

static uint64_t EpochWireRelCount(const std::vector<E_WireRow>& rows, const char* relname)
{
    uint64_t cnt = 0;
    for (const auto& row : rows) {
        if (!row.relname.empty() && row.relname == relname) {
            cnt++;
        }
    }
    return cnt;
}

void EpochRecvDiagLogWireRelSummary(const char* tag, uint64_t epoch, uint64_t csn, uint32_t from_node,
    const std::vector<E_WireRow>& rows)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 ||
        tag == nullptr) {
        return;
    }
    uint64_t oorder = EpochWireRelCount(rows, "bmsql_oorder");
    uint64_t new_order = EpochWireRelCount(rows, "bmsql_new_order");
    uint64_t order_line = EpochWireRelCount(rows, "bmsql_order_line");
    if (oorder == 0 && new_order == 0 && order_line == 0) {
        return;
    }
    char buf[256];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "WIRE_REL %s epoch=%lu csn=%lu from=%u rows=%zu oorder=%lu new_order=%lu order_line=%lu",
        tag,
        epoch,
        csn,
        from_node,
        rows.size(),
        oorder,
        new_order,
        order_line);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogLogicalApplyGate(uint64_t epoch, int apply_ok, int merger_pending, uint64_t merged,
    uint64_t committed)
{
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    char buf[256];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "LOGICAL_APPLY_GATE epoch=%lu apply_ok=%d merger_pending=%d merged=%lu committed=%lu",
        epoch,
        apply_ok,
        merger_pending,
        merged,
        committed);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogLogicalAdvanceFail(uint64_t epoch, const char* reason)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 || reason == nullptr) {
        return;
    }
    char buf[192];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "LOGICAL_ADVANCE_FAIL epoch=%lu reason=%s",
        epoch,
        reason);
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

void EpochRecvDiagLogMergeHeapAbort(uint64_t epoch, uint64_t txn_csn, uint32_t server_id, const char* relname,
    uint32_t relid, EpochOpType op, bool row_found, uint64_t heap_csn, uint64_t expected_csn)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    char buf[256];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "MERGE_HEAP_ABORT epoch=%lu csn=%lu sid=%u rel=%s(%u) op=%u found=%d heap_csn=%lu expected_csn=%lu",
        epoch,
        txn_csn,
        server_id,
        (relname != nullptr && relname[0] != '\0') ? relname : "?",
        static_cast<uint32_t>(relid),
        static_cast<unsigned>(op),
        row_found ? 1 : 0,
        heap_csn,
        expected_csn);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogStall(uint64_t epoch, const char* reason)
{
    EpochLogicalAdvanceTraceNoteStall(reason);
    if (!EpochRecvDiagVerboseEnabled() || !g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 ||
        reason == nullptr) {
        return;
    }
    g_recvdiag_stall_count++;
    if (epoch == g_recvdiag_last_stall_epoch && g_recvdiag_stall_count % EPOCH_RECV_DIAG_STALL_SAMPLE != 1) {
        return;
    }
    g_recvdiag_last_stall_epoch = epoch;

    uint32_t self = EpochGetNodeId();
    char buf[512];
    uint32_t node_id = EpochGetNodeId();
    if (strcmp(reason, "local_commit_pending") == 0 || strcmp(reason, "merge_incomplete") == 0) {
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
            "STALL epoch=%lu reason=%s pack=%lu chg=%lu abort=%lu merged=%lu committed=%lu peer_expect=%lu peer_rcv=%lu",
            epoch,
            reason,
            E_NodeManager::GetLocalPackTxnCounters(epoch, node_id),
            E_NodeManager::GetChangeSetTxnCounters(epoch, node_id),
            E_NodeManager::GetAbortTxnCounters(epoch, node_id),
            E_NodeManager::GetMergedTxnCounters(epoch, node_id),
            E_NodeManager::GetCommittedTxnCounters(epoch, node_id),
            expect,
            received);
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

    if (strcmp(reason, "send_incomplete") == 0 && g_recvdiag_stall_count % 50 == 1) {
        EpochRecvDiagLogHandshakeStall(epoch, reason, EpochHandshakeSendSubReason(epoch));
    }
}

void EpochDiagLogEpochStallSnapshot(uint64_t epoch, const char* reason)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 || reason == nullptr) {
        return;
    }

    uint32_t self = EpochGetNodeId();
    uint64_t self_pack = E_NodeManager::GetLocalPackTxnCounters(epoch, self);
    uint64_t self_executing = E_NodeManager::GetExecutingTxnCounters(epoch, self);
    uint64_t expected_merge = EpochPipelineExpectedMergeTxnCount(epoch);
    if (!EpochDiagShouldLogStallSnapshot(epoch, reason, self_pack, self_executing, expected_merge)) {
        return;
    }

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

    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    uint64_t gap_pl = (physical >= logical) ? (physical - logical) : 0;
    uint32_t total = EpochGetTotalNodes();

    uint64_t merge_won = EpochPipelineExpectedApplyWinnerCount(epoch); /* merge 关窗后 CRDT 净赢家数 */
    uint64_t wire_merge_done = E_NodeManager::GetWireMergeDoneCounters(epoch, self);
    uint64_t self_committed = E_NodeManager::GetCommittedTxnCounters(epoch, self);
    uint64_t applied_txns = EpochConcurrent::U64Get(g_recvdiag_applied_txns, epoch, 0);
    uint32_t apply_inflight = EpochApplyQueueInflight();
    uint32_t merge_inflight = EpochMergeQueueInflight();
    int merge_inflight_epoch = EpochPipelineEpochHasMergeInflight(epoch) ? 1 : 0;
    int send_done = E_NodeManager::IsNodeSendCompleted(epoch, self) ? 1 : 0;
    int recv_done = E_NodeManager::IsNodeReceiveCompleted(epoch, self) ? 1 : 0;
    int merged_eq_cmt = (merge_won == self_committed) ? 1 : 0;
    uint32_t pending_unapplied = EpochPipelineCountUnappliedWireTxns(epoch);

    uint64_t node_txn_since = EpochGetPhysicalTxnSinceAdvance();

    bool physical_ready = false;
    bool local_commit_ready = false;
    bool self_send_sent = false;
    bool merge_complete = false;
    bool apply_complete = false;
    EpochPipelineReadStageFlagsForDiag(epoch,
        &physical_ready,
        &local_commit_ready,
        &self_send_sent,
        &merge_complete,
        &apply_complete);

    fprintf(fp,
        "[%s] STALL_SNAPSHOT reason=%s epoch=%lu logical=%lu physical=%lu gap=%lu "
        "executing=%lu send_done=%d recv_done=%d "
        "stage_phys=%d stage_local_cmt=%d stage_seal_send=%d stage_merge=%d stage_apply=%d "
        "expected_merge=%lu wire_merge_done=%lu merge_won=%lu merge_inflight_epoch=%d merge_inflight_global=%u "
        "apply_inflight=%u merged_eq_cmt=%d pending_unapplied=%u node_txn_since=%lu/%d\n",
        ts,
        reason,
        epoch,
        logical,
        physical,
        gap_pl,
        self_executing,
        send_done,
        recv_done,
        physical_ready ? 1 : 0,
        local_commit_ready ? 1 : 0,
        self_send_sent ? 1 : 0,
        merge_complete ? 1 : 0,
        apply_complete ? 1 : 0,
        expected_merge,
        wire_merge_done,
        merge_won,
        merge_inflight_epoch,
        merge_inflight,
        apply_inflight,
        merged_eq_cmt,
        pending_unapplied,
        node_txn_since,
        g_epoch_physical_txns_per_epoch);

    fprintf(fp,
        "  SYNC epoch=%lu self=%u chg=%lu pack=%lu cmt=%lu abort=%lu applied_txns=%lu",
        epoch,
        self,
        E_NodeManager::GetChangeSetTxnCounters(epoch, self),
        self_pack,
        self_committed,
        E_NodeManager::GetAbortTxnCounters(epoch, self),
        applied_txns);

    for (uint32_t peer = 0; peer < total; ++peer) {
        if (peer == self) {
            continue;
        }
        uint64_t should = E_NodeManager::GetStoreShouldReceivedPackCounters(epoch, peer);
        uint64_t rcv = E_NodeManager::GetStoreReceivedPackCounters(epoch, peer);
        fprintf(fp,
            " peer%u:should=%lu rcv=%lu send=%d recv=%d",
            peer,
            should,
            rcv,
            E_NodeManager::IsNodeSendCompleted(epoch, peer) ? 1 : 0,
            E_NodeManager::IsNodeReceiveCompleted(epoch, peer) ? 1 : 0);
    }
    fprintf(fp, "\n\n");
    (void)fclose(fp);

    if (pending_unapplied > 0 &&
        (strcmp(reason, "apply_incomplete") == 0 || strcmp(reason, "unknown") == 0)) {
        EpochPipelineDiagLogFirstUnapplied(epoch);
    }
}

static void EpochDiagAppendLine(const char* line)
{
    if (line == nullptr) {
        return;
    }
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
    fprintf(fp, "[%s] node=%u %s\n", ts, EpochGetNodeId(), line);
    (void)fclose(fp);
}

void EpochDiagLogMergeLoserAbortCsn(uint64_t epoch, uint64_t csn, uint32_t server_id, bool resource_failure,
    bool in_abort_csn)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    char buf[256];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "MERGE_LOSER_ABORT_CSN epoch=%lu csn=%lu server_id=%u resource_failure=%d in_abort_csn=%d",
        epoch,
        csn,
        server_id,
        resource_failure ? 1 : 0,
        in_abort_csn ? 1 : 0);
    securec_check_ss(rc, "\0", "\0");
    EpochDiagAppendLine(buf);
}

void EpochDiagLogApplyPendingFirst(uint64_t epoch, uint64_t csn, uint32_t server_id, uint64_t wire_key,
    bool in_abort_csn, bool has_writable_rows, bool merge_inflight, bool apply_inflight)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    char buf[320];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "APPLY_PENDING_FIRST epoch=%lu csn=%lu server_id=%u wire_key=%lu in_abort_csn=%d "
        "has_writable=%d merge_inflight=%d apply_inflight=%d",
        epoch,
        csn,
        server_id,
        wire_key,
        in_abort_csn ? 1 : 0,
        has_writable_rows ? 1 : 0,
        merge_inflight ? 1 : 0,
        apply_inflight ? 1 : 0);
    securec_check_ss(rc, "\0", "\0");
    EpochDiagAppendLine(buf);
}

void EpochDiagMaybeReport(void)
{
    if (!g_enable_epoch_multimaster || !g_epoch_diag) {
        return;
    }

    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    if (g_last_diag_physical != 0 && physical <= g_last_diag_physical + EPOCH_DIAG_PHYSICAL_INTERVAL - 1) {
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
                    " n%u:chg=%lu pack=%lu merged=%lu cmt=%lu should=%lu rcv=%lu",
                    node,
                    E_NodeManager::GetChangeSetTxnCounters(epoch, node),
                    E_NodeManager::GetLocalPackTxnCounters(epoch, node),
                    node == self ? EpochPipelineExpectedApplyWinnerCount(epoch) :
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
        uint64_t self_merged = EpochPipelineExpectedApplyWinnerCount(epoch);
        uint64_t self_committed = E_NodeManager::GetCommittedTxnCounters(epoch, self);
        uint64_t applied_txns = EpochConcurrent::U64Get(g_recvdiag_applied_txns, epoch, 0);
        uint64_t applied_rows = EpochConcurrent::U64Get(g_recvdiag_applied_rows, epoch, 0);
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
