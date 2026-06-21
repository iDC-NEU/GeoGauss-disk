#include "postgres.h"

#include <atomic>

#include "epoch/e_statistician.h"
#include "epoch/e_config.h"
#include "epoch/e_epoch.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_types.h"
#include "epoch/e_net.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_txn_queue.h"
#include "epoch/e_txn_wire.h"

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
static std::unordered_set<uint64_t> g_txn_request_sent;

static void EpochRecvDiagAppendLine(const char* line)
{
    char path[MAXPGPATH];
    int rc = snprintf_s(path, sizeof(path), sizeof(path) - 1, "%s/epoch_recv_diag.log", t_thrd.proc_cxt.DataDir);
    securec_check_ss(rc, "\0", "\0");

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
    uint32_t from_node = txn.server_id;
    uint64_t pack_count = txn.csn;

    E_NodeManager::SetStoreShouldReceivedPackCounters(epoch, from_node, pack_count);
    /* 对端 epoch-end 的 pack 数同步到本节点视图，便于 seal / logical 推进 */
    E_NodeManager::SetLocalPackTxnCounters(epoch, from_node, pack_count);
    E_NodeManager::SetLocalProduceTxnCounters(epoch, from_node, pack_count);
    E_NodeManager::SetNodeSendCompleted(epoch, from_node, true);
    (void)EpochCheckAndSetNodeReceiveCompleted(epoch, from_node);
    EpochRecvDiagLogEpochEnd(epoch, from_node, pack_count);
}

bool EpochCheckAndSetNodeReceiveCompleted(uint64_t epochId, uint64_t nodeId)
{
    uint64_t should_receive = E_NodeManager::GetStoreShouldReceivedPackCounters(epochId, nodeId);
    uint64_t received = E_NodeManager::GetStoreReceivedPackCounters(epochId, nodeId);
    bool done = (should_receive == received);
    E_NodeManager::SetNodeReceiveCompleted(epochId, nodeId, done);
    return done;
}

bool EpochIsAllNodesReceiveCompleted(uint64_t epochId)
{
    uint32_t total = EpochGetTotalNodes();
    for (uint32_t node = 0; node < total; ++node) {
        if (node == EpochGetNodeId()) {
            if (!E_NodeManager::IsNodeReceiveCompleted(epochId, node)) {
                return false;
            }
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
    if (!EpochTxnQueuePush(payload.data(), static_cast<uint32_t>(payload.size()))) {
        ereport(WARNING, (errmsg("epoch: failed to enqueue epoch-end marker for epoch %lu", epoch)));
    }
    return true;
}

bool EpochLogicalEpochHasPendingPack(uint64_t epoch)
{
    if (EpochGetTotalNodes() <= 1) {
        return false;
    }
    uint32_t node_id = EpochGetNodeId();
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epoch, node_id);
    if (change_set == 0) {
        return false;
    }
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    uint64_t aborts = E_NodeManager::GetAbortTxnCounters(epoch, node_id);
    uint64_t read_committed = E_NodeManager::GetReadCommittedTxnCounters(epoch, node_id);
    uint64_t write_abort_after = E_NodeManager::GetWriteAbortAfterSendTxnCounters(epoch, node_id);
    uint64_t abort_net = (aborts > write_abort_after) ? (aborts - write_abort_after) : 0;
    uint64_t accounted = read_committed + abort_net;
    uint64_t should_send = (change_set > accounted) ? (change_set - accounted) : 0;
    return packed < should_send;
}

bool EpochTrySealEpoch(uint64_t epoch)
{
    if (EpochGetTotalNodes() <= 1) {
        return false;
    }

    E_NodeManager::EnsureEpochSlot(epoch);

    if (E_NodeManager::IsNodeSendCompleted(epoch, EpochGetNodeId())) {
        return true;
    }

    uint32_t node_id = EpochGetNodeId();
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epoch, node_id);
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    uint64_t aborts = E_NodeManager::GetAbortTxnCounters(epoch, node_id);
    uint64_t read_committed = E_NodeManager::GetReadCommittedTxnCounters(epoch, node_id);
    uint64_t write_abort_after = E_NodeManager::GetWriteAbortAfterSendTxnCounters(epoch, node_id);
    bool epoch_empty = (change_set == 0 && packed == 0 && aborts == 0);

    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    uint32_t delay = static_cast<uint32_t>(g_epoch_delay_slots);
    if (delay == 0) {
        delay = EPOCH_DEFAULT_DELAY;
    }
    if (!epoch_empty && physical <= epoch + static_cast<uint64_t>(delay) + 1) {
        EpochRecvDiagLogSealBlock(epoch, "physical_delay");
        return false;
    }

    /*
     * GeoGauss LoadShouldSendTxnNum (is_sync_exec)：
     * change_set - read_committed - (total_abort - write_abort_after_send)
     * IsCurrentEpochFinished: packed >= should_send && epoch < physical
     */
    uint64_t abort_net = (aborts > write_abort_after) ? (aborts - write_abort_after) : 0;
    uint64_t accounted = read_committed + abort_net;
    uint64_t should_send = (change_set > accounted) ? (change_set - accounted) : 0;
    uint64_t produced = E_NodeManager::GetLocalProduceTxnCounters(epoch, node_id);

    if (packed < should_send && produced == 0) {
        uint64_t logical = E_NodeManager::GetLogicalEpoch();
        uint64_t gap = (physical > logical) ? (physical - logical) : 0;
        uint64_t gap_limit = EpochGetPhysLogicalGapLimit();
        /*
         * 客户端超时/强杀：START 已计入 change_set 但 COMMIT/ABORT 未入账。
         * 原条件 physical > epoch+delay+8 在 gap_limit=5 时永远达不到（physical 最多
         * logical+5），会导致 seal 永久卡死。physical 顶到 gap 上限且无 produce 时核销。
         */
        if (gap >= gap_limit || physical > epoch + static_cast<uint64_t>(delay) + 1) {
            uint64_t orphan = should_send - packed;
            for (uint64_t i = 0; i < orphan; ++i) {
                E_NodeManager::IncAbortTxnCounters(epoch, node_id);
            }
            aborts = E_NodeManager::GetAbortTxnCounters(epoch, node_id);
            abort_net = (aborts > write_abort_after) ? (aborts - write_abort_after) : 0;
            accounted = read_committed + abort_net;
            should_send = (change_set > accounted) ? (change_set - accounted) : 0;
        }
    }

    if (packed < should_send) {
        EpochRecvDiagLogSealBlock(epoch, "change_set_pending");
        return false;
    }

    EpochPipelineRebroadcastCachedTxns(epoch);
    pg_usleep(500L);
    EpochPipelineRebroadcastCachedTxns(epoch);

    if (!EpochSealEpoch(epoch)) {
        return false;
    }

    E_NodeManager::SetNodeSendCompleted(epoch, EpochGetNodeId(), true);
    E_NodeManager::SetNodeReceiveCompleted(epoch, EpochGetNodeId(), true);
    EpochRecvDiagLogSent(epoch, E_NodeManager::GetLocalPackTxnCounters(epoch, EpochGetNodeId()));
    return true;
}

void EpochRebroadcastSealedEpochEnds(void)
{
    if (EpochGetTotalNodes() <= 1) {
        return;
    }

    /*
     * ZMQ slow-joiner / 双节点先后启动：重发已 seal 的历史 epoch-end 与缓存写集。
     */
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint32_t self = EpochGetNodeId();
    for (uint64_t epoch = 1; epoch < logical; ++epoch) {
        if (!E_NodeManager::IsNodeSendCompleted(epoch, self)) {
            continue;
        }
        EpochPipelineRebroadcastCachedTxns(epoch);
        E_WireTxn end_wire;
        if (!EpochBuildEpochEndWire(epoch, &end_wire)) {
            continue;
        }
        std::string payload;
        if (!E_WireSerializeTxn(end_wire, &payload)) {
            continue;
        }
        (void)EpochNetPublish(payload);
    }
}

void EpochOnPeerConnected(uint32_t peerId)
{
    if (peerId == EpochGetNodeId() || EpochGetTotalNodes() <= 1) {
        return;
    }
    /* GeoGauss 用 cache_txn_request 补发；磁盘表端口用 epoch-end 重广播 */
    EpochRebroadcastSealedEpochEnds();
    /*
     * 勿 burst 推进 logical：对端可能尚未 seal 同 epoch，会造成双节点 logical 分叉。
     * 正常由 epoch worker 每轮 TryAdvanceLogical 逐步对齐。
     */
    (void)EpochTryAdvanceLogical();
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
    g_txn_request_sent.clear();
}

void EpochRecvDiagLogSealBlock(uint64_t epoch, const char* reason)
{
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
    uint64_t accounted = read_committed + abort_net;
    uint64_t should_send = (change_set > accounted) ? (change_set - accounted) : 0;

    char buf[320];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "SEAL_BLOCK epoch=%lu reason=%s chg=%lu pack=%lu read=%lu abort=%lu w_abort=%lu should_send=%lu",
        epoch,
        reason,
        change_set,
        packed,
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

    if (expected_pack != received && g_txn_request_sent.insert(epoch).second) {
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
    EpochPipelineRebroadcastCachedTxns(epoch);
    E_WireTxn end_wire;
    if (EpochBuildEpochEndWire(epoch, &end_wire)) {
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
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 || !ok) {
        return;
    }
    g_recvdiag_applied_rows[epoch] += row_count;
    g_recvdiag_applied_txns[epoch] += 1;

    char buf[256];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "APPLY epoch=%lu rows=%lu total_applied_txns=%lu total_applied_rows=%lu",
        epoch,
        row_count,
        g_recvdiag_applied_txns[epoch],
        g_recvdiag_applied_rows[epoch]);
    securec_check_ss(rc, "\0", "\0");
    EpochRecvDiagAppendLine(buf);
}

void EpochRecvDiagLogStall(uint64_t epoch, const char* reason)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 || reason == nullptr) {
        return;
    }
    g_recvdiag_stall_count++;
    if (epoch == g_recvdiag_last_stall_epoch && g_recvdiag_stall_count % 100 != 1) {
        return;
    }
    g_recvdiag_last_stall_epoch = epoch;

    uint32_t self = EpochGetNodeId();
    uint32_t peer = (self == 0) ? 1 : 0;
    char buf[512];
    uint32_t node_id = EpochGetNodeId();
    if (strcmp(reason, "commit_path_incomplete") == 0 || strcmp(reason, "record_incomplete") == 0) {
        int rc = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            "STALL epoch=%lu reason=%s pack=%lu prod=%lu rec=%lu rcd=%lu merged=%lu committed=%lu remote_exec=%d",
            epoch,
            reason,
            E_NodeManager::GetLocalPackTxnCounters(epoch, node_id),
            E_NodeManager::GetLocalProduceTxnCounters(epoch, node_id),
            E_NodeManager::GetLocalRecordCommitCounters(epoch, node_id),
            E_NodeManager::GetLocalRecordCommittedCounters(epoch, node_id),
            E_NodeManager::GetMergedTxnCounters(epoch, node_id),
            E_NodeManager::GetCommittedTxnCounters(epoch, node_id),
            E_NodeManager::IsRemoteExeced() ? 1 : 0);
        securec_check_ss(rc, "\0", "\0");
    } else {
        int rc = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            "STALL epoch=%lu reason=%s self_send=%d self_recv=%d peer_send=%d peer_recv=%d",
            epoch,
            reason,
            E_NodeManager::IsNodeSendCompleted(epoch, self) ? 1 : 0,
            E_NodeManager::IsNodeReceiveCompleted(epoch, self) ? 1 : 0,
            E_NodeManager::IsNodeSendCompleted(epoch, peer) ? 1 : 0,
            E_NodeManager::IsNodeReceiveCompleted(epoch, peer) ? 1 : 0);
        securec_check_ss(rc, "\0", "\0");
    }
    EpochRecvDiagAppendLine(buf);

    if (strcmp(reason, "recv_incomplete") == 0 &&
        !E_NodeManager::IsNodeReceiveCompleted(epoch, peer) &&
        g_recvdiag_stall_count % 200 == 1) {
        EpochPublishTxnRequest(epoch);
    }
}

void EpochDiagMaybeReport(void)
{
    if (!g_enable_epoch_multimaster) {
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
    int rc = snprintf_s(path, sizeof(path), sizeof(path) - 1, "%s/epoch_diag.log", t_thrd.proc_cxt.DataDir);
    securec_check_ss(rc, "\0", "\0");

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

    for (uint64_t epoch = start; epoch < end; ++epoch) {
        fprintf(fp, "  epoch=%lu", epoch);
        for (uint32_t node = 0; node < total; ++node) {
            fprintf(fp,
                    " n%u:chg=%lu pack=%lu prod=%lu rec=%lu rcd=%lu merged=%lu rcv=%lu",
                    node,
                    E_NodeManager::GetChangeSetTxnCounters(epoch, node),
                    E_NodeManager::GetLocalPackTxnCounters(epoch, node),
                    E_NodeManager::GetLocalProduceTxnCounters(epoch, node),
                    E_NodeManager::GetLocalRecordCommitCounters(epoch, node),
                    E_NodeManager::GetLocalRecordCommittedCounters(epoch, node),
                    E_NodeManager::GetMergedTxnCounters(epoch, node),
                    E_NodeManager::GetStoreReceivedPackCounters(epoch, node));
        }
        fprintf(fp,
                " send_done=%d recv_done=%d\n",
                E_NodeManager::IsNodeSendCompleted(epoch, EpochGetNodeId()) ? 1 : 0,
                E_NodeManager::IsNodeReceiveCompleted(epoch, EpochGetNodeId()) ? 1 : 0);
    }
    fprintf(fp, "\n");
    (void)fclose(fp);
}
