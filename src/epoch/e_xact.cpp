#include "postgres.h"

#include "epoch/e_xact.h"
#include "epoch/e_config.h"
#include "epoch/e_epoch.h"
#include "epoch/e_exec_track.h"
#include "epoch/e_read_frontier.h"
#include "epoch/e_net.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_perf.h"
#include "epoch/e_shmem.h"
#include "epoch/e_statistician.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_txn_queue.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_row_key.h"
#include "epoch/e_types.h"

#include "occ/o_commit.h"
#include "occ/o_txn.h"
#include "occ/occ_internal.h"
#include "occ/occ_stats.h"

#include "miscadmin.h"
#include "knl/knl_variable.h"
#include "knl/knl_thread.h"
#include "knl/knl_thread.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "postmaster/autovacuum.h"
#include "access/xact.h"
#include "access/heapam.h"
#include "storage/procarray.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include <climits>
#include <cstdint>
#include <cstring>

#include "access/tupdesc.h"
#include "catalog/pg_type.h"

static bool E_TryExtractRouteWid(Relation rel, HeapTuple tup, uint16_t* out_wid)
{
    if (rel == nullptr || tup == nullptr || out_wid == nullptr) {
        return false;
    }
    TupleDesc desc = RelationGetDescr(rel);
    for (int i = 0; i < desc->natts; ++i) {
        Form_pg_attribute att = TupleDescAttr(desc, i);
        if (att->attisdropped) {
            continue;
        }
        const char* name = NameStr(att->attname);
        size_t nlen = strlen(name);
        /* w_id / s_w_id / c_w_id / o_w_id ... */
        if (nlen < 4 || strcmp(name + nlen - 4, "w_id") != 0) {
            continue;
        }
        if (att->atttypid != INT2OID && att->atttypid != INT4OID) {
            continue;
        }
        bool isnull = false;
        Datum value = heap_getattr(tup, att->attnum, desc, &isnull);
        if (isnull) {
            continue;
        }
        int32 wid = (att->atttypid == INT2OID) ? static_cast<int32>(DatumGetInt16(value))
                                               : DatumGetInt32(value);
        if (wid <= 0 || wid > 65535) {
            continue;
        }
        *out_wid = static_cast<uint16_t>(wid);
        return true;
    }
    return false;
}

/*
 * 从 write set 行中统计 warehouse：众数；并列取最小 wid。
 * 无任何 w_id 列时保持 0（路由 fallback 到 wire_key）。
 */
static uint16_t E_PickRouteWidMajority(const uint16_t* samples, int n_samples)
{
    if (samples == nullptr || n_samples <= 0) {
        return 0;
    }
    struct WidCount {
        uint16_t wid;
        int cnt;
    };
    WidCount hist[32];
    int nh = 0;
    for (int i = 0; i < n_samples; ++i) {
        uint16_t w = samples[i];
        if (w == 0) {
            continue;
        }
        int found = -1;
        for (int j = 0; j < nh; ++j) {
            if (hist[j].wid == w) {
                found = j;
                break;
            }
        }
        if (found >= 0) {
            hist[found].cnt++;
        } else if (nh < 32) {
            hist[nh].wid = w;
            hist[nh].cnt = 1;
            nh++;
        }
    }
    uint16_t best = 0;
    int best_cnt = 0;
    for (int j = 0; j < nh; ++j) {
        if (hist[j].cnt > best_cnt || (hist[j].cnt == best_cnt && (best == 0 || hist[j].wid < best))) {
            best = hist[j].wid;
            best_cnt = hist[j].cnt;
        }
    }
    return best;
}

static bool E_TxnHasWriteSet(O_TxnManager* txn)
{
    if (txn == nullptr || txn->m_accessMgr == nullptr) {
        return false;
    }
    return txn->m_accessMgr->m_Size() > 0;
}

static EpochOpType E_MapAccessTypeToWireOp(AccessType type)
{
    switch (type) {
        case INS:
            return EPOCH_OP_INSERT;
        case WR:
            return EPOCH_OP_UPDATE;
        case DEL:
            return EPOCH_OP_DELETE;
        default:
            return EPOCH_OP_READ;
    }
}

static bool E_BuildWireTxn(O_TxnManager* txn, uint64_t csn, E_WireTxn* wire)
{
    if (txn == nullptr || wire == nullptr) {
        return false;
    }
    wire->csn = csn;
    wire->epoch = u_sess->epoch_cxt.txn_epoch;
    wire->server_id = EpochGetNodeId();
    wire->txn_state = EPOCH_TXN_COMMIT;
    wire->route_wid = 0;
    wire->rows.clear();

    uint16_t wid_samples[64];
    int n_wid_samples = 0;

    txn->m_accessMgr->ForEachWriteEntry([&](const occ_sentinel& sent, O_Access* access) {
        if (access == nullptr) {
            return;
        }
        AccessType mapped = access->m_type;
        if (mapped != INS && mapped != WR && mapped != DEL) {
            return;
        }
        HeapTuple row = access->GetTxnRow();
        if (!OccDraftTupleLayoutOk(row)) {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("epoch: invalid OCC draft tuple while packing write set (relid %u, access type %d)",
                        OccResolveRelid(sent), static_cast<int>(access->m_type)),
                 errhidestmt(true),
                 errhideprefix(true)));
        }
        E_WireRow wire_row;
        wire_row.op_type = E_MapAccessTypeToWireOp(mapped);
        wire_row.relid = OccResolveRelid(sent);
        char* relname = get_rel_name(wire_row.relid);
        if (relname != nullptr) {
            wire_row.relname = relname;
        }
        wire_row.bi_hi = sent.tuple_id.ip_blkid.bi_hi;
        wire_row.bi_lo = sent.tuple_id.ip_blkid.bi_lo;
        wire_row.ip_posid = sent.tuple_id.ip_posid;
        wire_row.expected_csn = access->m_csn;
        wire_row.tuple_data.resize(row->t_len);
        errno_t rc = memcpy_s(wire_row.tuple_data.data(), wire_row.tuple_data.size(),
                              row->t_data, row->t_len);
        securec_check(rc, "\0", "\0");

        Relation rel = heap_open(wire_row.relid, AccessShareLock);
        char pk_buf[E_ROW_PK_MAX_LEN];
        uint16_t pk_len = 0;
        if (rel != nullptr && E_BuildPkKey(rel, row, pk_buf, sizeof(pk_buf), &pk_len)) {
            wire_row.pk_len = pk_len;
            wire_row.pk.assign(pk_buf, pk_len);
        }
        uint16_t row_wid = 0;
        if (rel != nullptr && E_TryExtractRouteWid(rel, row, &row_wid) && n_wid_samples < 64) {
            wid_samples[n_wid_samples++] = row_wid;
        }
        if (rel != nullptr) {
            heap_close(rel, AccessShareLock);
        }

        wire->rows.push_back(std::move(wire_row));
    });
    wire->route_wid = E_PickRouteWidMajority(wid_samples, n_wid_samples);
    if (!wire->rows.empty()) {
        EpochRecvDiagLogWireRelSummary("PACK", wire->epoch, wire->csn, wire->server_id, wire->rows);
    }
    return !wire->rows.empty();
}

static bool E_ShouldTrackTxnEpoch(void)
{
    if (IsAutoVacuumWorkerProcess() || AmEpochWorkerProcess()) {
        return false;
    }
    if (t_thrd.role == EPOCHWORKER || t_thrd.role == EPOCHAPPLYWORKER || t_thrd.role == EPOCHMERGEWORKER ||
        t_thrd.role == EPOCHPACKWORKER || t_thrd.role == EPOCHSENDWORKER ||
        t_thrd.role == EPOCHTIPFLUSHER) {
        return false;
    }
    /*
     * 仅普通客户端 backend 连到 epoch 跟踪库时参与 epoch gate / 计数。
     * openGauss 中 XACT START 触发面很广，此处过滤后再 assign epoch。
     */
    if (t_thrd.role != WORKER && t_thrd.role != THREADPOOL_WORKER) {
        return false;
    }
    if (!IsNormalProcessingMode()) {
        return false;
    }
    if (u_sess->proc_cxt.MyProcPort == nullptr || u_sess->proc_cxt.MyProcPort->database_name == nullptr) {
        return false;
    }
    const char* track_db = (g_epoch_apply_database != nullptr && g_epoch_apply_database[0] != '\0')
        ? g_epoch_apply_database
        : "test";
    if (strcmp(u_sess->proc_cxt.MyProcPort->database_name, track_db) != 0) {
        return false;
    }
    return true;
}

static bool E_AllRequiredPeersConnected(void)
{
    uint32_t total = EpochGetTotalNodes();
    uint32_t self = EpochGetNodeId();
    for (uint32_t node = 0; node < total; ++node) {
        if (node == self) {
            continue;
        }
        if (!EpochPeerIsConnected(node)) {
            return false;
        }
    }
    return true;
}

/*
 * 多主 sync：所有对端连通时 pack/defer，heap 由 worker apply。
 * total_nodes=2 但对端未连通时退化为单节点，避免 remote gate 死等。
 */
static bool E_IsSyncEpochMode(void)
{
    return g_enable_epoch_multimaster && EpochGetTotalNodes() > 1 &&
           E_AllRequiredPeersConnected();
}

static constexpr long EPOCH_PACK_QUEUE_PAUSE_US = 200;

static uint32_t EpochPackQueuePressureThreshold(void)
{
    return (EpochGetTxnQueueCapacity() * 3) / 4;
}

static bool EpochPackQueuePressure(void)
{
    return EpochTxnQueueCount() >= EpochPackQueuePressureThreshold();
}

static void EpochRecordGateWait(EpochPerfGateKind kind, instr_time* start, uint64_t spin_count)
{
    if (!EpochPerfIsEnabled() || start == nullptr) {
        return;
    }
    instr_time end;
    INSTR_TIME_SET_CURRENT(end);
    INSTR_TIME_SUBTRACT(end, *start);
    EpochPerfAddGateWait(kind, INSTR_TIME_GET_MICROSEC(end), spin_count);
}

/* pack 前仅在队列高压时等待 drain，不要求 logical 推进 */
static void EpochWaitForPackQueue(void)
{
    if (!EpochIsSyncBackgroundWorkerCommit() || !EpochPackQueuePressure()) {
        return;
    }

    uint32_t queue_threshold = EpochPackQueuePressureThreshold();
    instr_time wait_start;
    INSTR_TIME_SET_CURRENT(wait_start);
    uint64_t spins = 0;

    while (EpochTxnQueueCount() >= queue_threshold) {
        pg_usleep(EPOCH_PACK_QUEUE_PAUSE_US);
        spins++;
        CHECK_FOR_INTERRUPTS();
    }

    if (spins > 0) {
        EpochRecordGateWait(EPOCH_PERF_GATE_PACK_ADMIT, &wait_start, spins);
    }
}

static bool E_PackWriteTxnToQueue(O_TxnManager* txn, uint64_t commit_epoch, uint32_t node_id)
{
    if (E_NodeManager::IsNodeSendCompleted(commit_epoch, node_id)) {
        ereport(ERROR,
            (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
             errmsg("epoch: cannot pack into sealed epoch %lu", commit_epoch),
             errhidestmt(true),
             errhideprefix(true),
             onlyfrontmsg(true)));
    }

    uint64_t csn = EpochGetNextCommitCsn();
    txn->SetCommitSequenceNumber(csn);
    E_WireTxn wire;
    if (!E_BuildWireTxn(txn, csn, &wire)) {
        return false;
    }
    E_NodeManager::IncChangeSetTxnCounters(commit_epoch, node_id);
    std::string payload;
    if (!E_WireSerializeTxn(wire, &payload)) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("epoch: failed to serialize transaction")));
    }
    if (!EpochTxnQueuePush(commit_epoch, payload.data(), static_cast<uint32_t>(payload.size()))) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("epoch: pending txn queue is full")));
    }
    E_NodeManager::IncLocalPackTxnCounters(commit_epoch, node_id);
    return true;
}

static void E_ResetTxnEpochContext(void)
{
    u_sess->epoch_cxt.txn_epoch = 0;
    u_sess->epoch_cxt.epoch_assigned = false;
    u_sess->epoch_cxt.epoch_exec_counted = false;
    u_sess->epoch_cxt.epoch_sync_heap_recorded = false;
    u_sess->epoch_cxt.epoch_procarray_ended = false;
    EpochResetReadFrontier();
}

static void EpochWaitForStartGate(uint64_t commit_epoch);

static void EpochMaybeLogBackendGateWait(const char* gate, uint64_t commit_epoch, uint64_t spins)
{
    if (spins == 1 || spins == 1000 || spins == 10000 || spins % 10000 == 0) {
        EpochRecvDiagLogBackendGate(gate, commit_epoch, spins);
    }
}

static void EpochMaybeLogPhysicalTxnLimitWait(uint64_t commit_epoch, uint64_t spins)
{
    if (g_epoch_physical_txns_per_epoch <= 0) {
        return;
    }
    if (spins == 1 || spins == 1000 || spins == 10000 || spins % 10000 == 0) {
        EpochRecvDiagLogPhysicalTxnLimit(commit_epoch,
            EpochGetPhysicalTxnSinceAdvance(),
            static_cast<uint64_t>(g_epoch_physical_txns_per_epoch),
            spins);
    }
}

static bool EpochShouldSkipAssignOnPrepareParse(void)
{
    /*
     * JDBC prepareStatement → extended Parse：仅做 plan cache，不参与 epoch 同步。
     * sync 多主下始终跳过 Parse 的 assign/gate/executing（与 epoch_sync_start_gate 无关）。
     * Bind/Execute/Simple Query 仍走完整 START gate（若 gate 开启）。
     */
    return E_IsSyncEpochMode() && u_sess->pbe_message == PARSE_MESSAGE_QUERY;
}

/*
 * XACT START：Assign + executing++ + START gate。
 * assign 后立即 executing++（含 gate 自旋），防止 worker 将已 assign 的 epoch 误判为空并 seal。
 */
static void E_AssignEpochOnStart(void)
{
    if (!E_ShouldTrackTxnEpoch()) {
        return;
    }
    if (EpochShouldSkipAssignOnPrepareParse()) {
        EpochRecvDiagLogBackendPhase("ASSIGN_SKIP_PREPARE", 0);
        return;
    }
    if (u_sess->epoch_cxt.epoch_assigned) {
        uint32_t node_id = EpochGetNodeId();
        uint64_t epoch = u_sess->epoch_cxt.txn_epoch;
        if (E_NodeManager::IsNodeSendCompleted(epoch, node_id)) {
            EpochReleaseTxnExecuting("assign_sealed_reset");
            E_ResetTxnEpochContext();
        } else {
            return;
        }
    }

    uint32_t node_id = EpochGetNodeId();
    uint64_t epoch = 0;
    uint64_t assign_spins = 0;
    uint64_t physical_txn_spins = 0;
    uint64_t reserved_slot = 0;
    for (;;) {
        epoch = EpochAssignTxnEpoch();
        if (E_NodeManager::IsNodeSendCompleted(epoch, node_id)) {
            assign_spins++;
            EpochMaybeLogBackendGateWait("ASSIGN_SEALED", epoch, assign_spins);
            pg_usleep(200);
            CHECK_FOR_INTERRUPTS();
            continue;
        }
        if (!EpochTryReservePhysicalTxnSlot(&reserved_slot)) {
            physical_txn_spins++;
            EpochMaybeLogPhysicalTxnLimitWait(epoch, physical_txn_spins);
            pg_usleep(200);
            CHECK_FOR_INTERRUPTS();
            continue;
        }
        break;
    }

    u_sess->epoch_cxt.txn_epoch = epoch;
    u_sess->epoch_cxt.epoch_assigned = true;
    EpochPinReadFrontierOnStart();
    EpochRecvDiagLogBackendPhase("ASSIGN_START", epoch);
    if (g_epoch_physical_txns_per_epoch > 0) {
        EpochRecvDiagLogPhysicalTxnReserved(epoch,
            reserved_slot,
            static_cast<uint64_t>(g_epoch_physical_txns_per_epoch));
    }
    if (assign_spins > 0) {
        EpochRecvDiagLogBackendGate("ASSIGN_SEALED", epoch, assign_spins);
    }
    EpochTrackExecutingInc(epoch, "assign_on_start");
    EpochWaitForStartGate(epoch);
    EpochRecvDiagLogBackendPhase("START_GATE_DONE", epoch);
}

/*
 * 写路径懒 Assign：正常 TPCC 已在 XACT START Assign；此处兜底未 START 即写的情况。
 */
void EpochEnsureTxnEpochOnAccess(bool is_write)
{
    if (!E_ShouldTrackTxnEpoch() || !is_write) {
        return;
    }
    if (!u_sess->epoch_cxt.epoch_assigned) {
        E_AssignEpochOnStart();
    }
}

/*
 * GeoGauss is_sync_exec START gate (mot_fdw.cpp):
 * while (start_epoch > logical) usleep(200);
 *
 * sync 多主（含后台 defer pack）：默认阻塞 START gate，保证 epoch N+1 在 epoch N 关窗后再执行。
 * epoch_sync_start_gate=off 时也不在 START 阻塞。
 */
static void EpochWaitForStartGate(uint64_t commit_epoch)
{
    if (E_IsSyncEpochMode() && !g_epoch_sync_start_gate) {
        (void)commit_epoch;
        return;
    }

    const long waitUs = 200;
    if (E_IsSyncEpochMode()) {
        instr_time wait_start;
        INSTR_TIME_SET_CURRENT(wait_start);
        uint64_t spins = 0;
        uint64_t logical_entry = E_NodeManager::GetLogicalEpoch();
        while (commit_epoch > E_NodeManager::GetLogicalEpoch()) {
            EpochMaybeLogBackendGateWait("START", commit_epoch, spins + 1);
            pg_usleep(waitUs);
            spins++;
            CHECK_FOR_INTERRUPTS();
        }
        if (spins > 0) {
            instr_time wait_end;
            INSTR_TIME_SET_CURRENT(wait_end);
            INSTR_TIME_SUBTRACT(wait_end, wait_start);
            EpochRecvDiagLogBackendGateDone("START",
                commit_epoch,
                spins,
                logical_entry,
                E_NodeManager::GetLogicalEpoch(),
                INSTR_TIME_GET_MICROSEC(wait_end));
        }
        EpochRecordGateWait(EPOCH_PERF_GATE_START, &wait_start, spins);
    }
}

/*
 * 已在途事务 COMMIT/pack：只等 logical 关窗。
 */
static void EpochWaitForLogicalEpoch(uint64_t commit_epoch)
{
    if (!E_IsSyncEpochMode()) {
        return;
    }
    if (!g_epoch_sync_start_gate) {
        return;
    }
    const long waitUs = 200;
    instr_time wait_start;
    INSTR_TIME_SET_CURRENT(wait_start);
    uint64_t spins = 0;
    while (commit_epoch > E_NodeManager::GetLogicalEpoch()) {
        EpochMaybeLogBackendGateWait("LOGICAL_COMMIT", commit_epoch, spins + 1);
        pg_usleep(waitUs);
        spins++;
        CHECK_FOR_INTERRUPTS();
    }
    EpochRecordGateWait(EPOCH_PERF_GATE_LOGICAL_COMMIT, &wait_start, spins);
}

/*
 * sync 单节点退化：COMMIT hook 内 O_RecordCommit 落 heap。
 * 多主写路径在 PRE_COMMIT pack 后由 worker apply；不在 backend 查 abort_csn。
 */
static void EpochSyncRecordHeap(O_TxnManager* txn)
{
    if (!E_IsSyncEpochMode() || u_sess->epoch_cxt.epoch_sync_heap_recorded) {
        return;
    }

    uint64_t csn = txn->GetCommitSequenceNumber();
    if (csn == INVALID_CSN) {
        csn = EpochGetNextCommitCsn();
        txn->SetCommitSequenceNumber(csn);
    }

    if (!O_RevalidateWriteSet()) {
        uint64_t commit_epoch = u_sess->epoch_cxt.txn_epoch;
        uint32_t node_id = EpochGetNodeId();
        if (u_sess->epoch_cxt.epoch_assigned) {
            E_NodeManager::IncWriteAbortAfterSendTxnCounters(commit_epoch, node_id);
            E_NodeManager::IncAbortTxnCounters(commit_epoch, node_id);
        }
        O_OccStatsRecordAbort(OCC_ABORT_REVALIDATE);
        ereport(ERROR,
            (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
             errmsg("epoch: could not serialize access due to concurrent update (revalidate)"),
             errhidestmt(true),
             errhideprefix(true),
             onlyfrontmsg(true)));
    }

    O_RecordCommit(csn);
    txn->ReleaseCachedRelations();
    u_sess->epoch_cxt.epoch_sync_heap_recorded = true;
    EpochRecvDiagLogBackendPhase("COMMIT_HEAP_RECORDED", u_sess->epoch_cxt.txn_epoch);
}

/*
 * sync 多主写事务：XACT_EVENT_PRE_COMMIT 仅 pack 入队并立即释放 executing，
 * merge / abort_csn / heap 落盘 / logical 关窗由 epoch worker 后台完成。
 */
static void EpochSyncPreCommitWritePath(O_TxnManager* txn)
{
    if (!E_IsSyncEpochMode() || txn == nullptr || !E_TxnHasWriteSet(txn)) {
        return;
    }
    if (u_sess->epoch_cxt.deferred_commit) {
        return;
    }

    if (!u_sess->epoch_cxt.epoch_assigned) {
        E_AssignEpochOnStart();
    }

    if (EpochGetTotalNodes() > 1) {
        O_PrepareWriteSetForPack();
        EpochMaybeLogTxnProgress("PACK_PREP", "write_set");
    }

    uint64_t commit_epoch = u_sess->epoch_cxt.txn_epoch;
    uint32_t node_id = EpochGetNodeId();

    EpochRecvDiagLogBackendPhase("PRE_COMMIT_WRITE_BEGIN", commit_epoch);

    if (EpochGetTotalNodes() > 1) {
        if (EpochIsSyncBackgroundWorkerCommit()) {
            EpochWaitForPackQueue();
        }
        if (E_PackWriteTxnToQueue(txn, commit_epoch, node_id)) {
            /* pack 成功；worker 负责 seal/merge/apply */
        }
    }
    EpochRecvDiagLogBackendPhase("PRE_COMMIT_AFTER_PACK", commit_epoch);
    EpochReleaseTxnExecuting("sync_precommit_pack");
    EpochRecvDiagLogBackendPhase("PRE_COMMIT_EXEC_RELEASED", commit_epoch);
    txn->ReleaseOccLocks();
    EpochRecvDiagLogBackendPhase("PRE_COMMIT_WORKER_DEFER", commit_epoch);

    u_sess->epoch_cxt.deferred_commit = true;
}

void EpochXactCallback(XactEvent event)
{
    if (!g_enable_epoch_multimaster) {
        return;
    }

    if (event == XACT_EVENT_STMT_FINISH) {
        if (u_sess->epoch_cxt.epoch_exec_counted && !u_sess->epoch_cxt.deferred_commit) {
            EpochRecvDiagLogXactHook("STMT_FINISH", "in_flight_write");
        }
        return;
    }

    if (event == XACT_EVENT_PRE_COMMIT) {
        O_TxnManager* pre_txn = u_sess->occ_cxt.txn_manager;
        if (pre_txn != nullptr && E_TxnHasWriteSet(pre_txn) && E_IsSyncEpochMode()) {
            EpochRecvDiagLogXactHook("PRE_COMMIT", "sync_write");
            EpochSyncPreCommitWritePath(pre_txn);
            return;
        }
        if (u_sess->epoch_cxt.epoch_exec_counted && !u_sess->epoch_cxt.deferred_commit) {
            EpochRecvDiagLogXactHook("PRE_COMMIT", "in_flight_write");
        }
        return;
    }

    if (event == XACT_EVENT_START) {
        if (E_ShouldTrackTxnEpoch()) {
            E_AssignEpochOnStart();
            EpochRecvDiagLogXactHook("START", "assigned");
        }
        return;
    }

    O_TxnManager* txn = u_sess->occ_cxt.txn_manager;
    if (txn == nullptr) {
        if (event == XACT_EVENT_ABORT || event == XACT_EVENT_END_TRANSACTION) {
            EpochRecvDiagLogXactHook(event == XACT_EVENT_ABORT ? "ABORT" : "END",
                "no_occ_txn");
            EpochReleaseTxnExecuting("no_occ_txn");
            E_ResetTxnEpochContext();
        }
        return;
    }

    if (event == XACT_EVENT_COMMIT) {
        bool has_write = E_TxnHasWriteSet(txn);
        EpochRecvDiagLogXactHook("COMMIT", has_write ? "has_write_set" : "readonly");
        if (!has_write) {
            /* 只读：若曾通过 OCC 读路径分配 epoch，等待关窗 */
            EpochReleaseTxnExecuting("readonly_commit");
            if (u_sess->epoch_cxt.epoch_assigned && E_IsSyncEpochMode()) {
                EpochWaitForLogicalEpoch(u_sess->epoch_cxt.txn_epoch);
                O_OccStatsRecordCommit();
            }
            E_ResetTxnEpochContext();
            return;
        }

        /*
         * sync 多主写：PRE_COMMIT 已 pack 并 deferred，COMMIT 不再等 gate。
         */
        if (E_IsSyncEpochMode() && u_sess->epoch_cxt.deferred_commit && EpochGetTotalNodes() > 1) {
            EpochRecvDiagLogXactHook("COMMIT", "sync_worker_deferred");
            return;
        }
        if (E_IsSyncEpochMode() && u_sess->epoch_cxt.epoch_sync_heap_recorded) {
            EpochRecvDiagLogXactHook("COMMIT", "sync_precommit_done");
            return;
        }
        if (E_IsSyncEpochMode() && EpochGetTotalNodes() > 1) {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("epoch: sync multimaster write path must complete in PRE_COMMIT"),
                 errhidestmt(true),
                 errhideprefix(true)));
        }

        if (!u_sess->epoch_cxt.epoch_assigned) {
            E_AssignEpochOnStart();
        }

        /*
         * epoch 多主 pack 前不做 ValidateOcc；merge/apply 由 worker 完成。
         * 写-写冲突由 worker merge/apply 处理；多主 defer 时 heap 不在 backend 落盘。
         */
        if (EpochGetTotalNodes() > 1) {
            O_PrepareWriteSetForPack();
            EpochMaybeLogTxnProgress("PACK_PREP", "write_set");
        } else {
            EpochMaybeLogTxnProgress("VALIDATE_COMMIT", "begin");
            if (O_ValidateCommit() != RC_OK) {
                EpochMaybeLogTxnProgress("VALIDATE_COMMIT", "fail");
                if (u_sess->epoch_cxt.epoch_assigned) {
                    E_NodeManager::IncAbortTxnCounters(u_sess->epoch_cxt.txn_epoch, EpochGetNodeId());
                }
                EpochReleaseTxnExecuting("validate_fail");
                ereport(ERROR,
                        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                         errmsg("epoch: could not serialize access due to concurrent update"),
                         errhidestmt(true),
                         errhideprefix(true),
                         onlyfrontmsg(true)));
            }
            EpochMaybeLogTxnProgress("VALIDATE_COMMIT", "ok");
        }

        uint64_t commit_epoch = u_sess->epoch_cxt.txn_epoch;
        EpochRecvDiagLogBackendPhase("COMMIT_WRITE_BEGIN", commit_epoch);

        /*
         * 单节点 sync 退化：仍走 COMMIT hook（无 PRE_COMMIT 写路径）。
         */
        if (E_IsSyncEpochMode()) {
            EpochRecvDiagLogBackendPhase("COMMIT_AFTER_PACK", commit_epoch);
            EpochReleaseTxnExecuting("sync_commit_pack");
            txn->ReleaseOccLocks();
            EpochSyncRecordHeap(txn);
            O_OccStatsRecordCommit();
            u_sess->epoch_cxt.deferred_commit = true;
        }
        return;
    }

    if (event == XACT_EVENT_RECORD_COMMIT) {
        if (!E_TxnHasWriteSet(txn)) {
            return;
        }
        EpochRecvDiagLogXactHook("RECORD_COMMIT",
            u_sess->epoch_cxt.deferred_commit ? "deferred" : "immediate");
        if (E_IsSyncEpochMode() && !u_sess->epoch_cxt.deferred_commit) {
            return;
        }

        uint64_t csn = txn->GetCommitSequenceNumber();
        if (csn == INVALID_CSN) {
            csn = EpochGetNextCommitCsn();
            txn->SetCommitSequenceNumber(csn);
        }
        uint64_t commit_epoch = u_sess->epoch_cxt.txn_epoch;
        uint32_t node_id = EpochGetNodeId();

        if (u_sess->epoch_cxt.epoch_sync_heap_recorded) {
            O_OccStatsRecordCommit();
            return;
        }

        if (E_IsSyncEpochMode()) {
            /*
             * sync 多主 defer：heap 由 worker E_ApplyLocalWireTxn 落盘。
             * 单节点退化路径在 COMMIT hook 经 EpochSyncRecordHeap 落盘。
             */
            if (u_sess->epoch_cxt.deferred_commit && EpochGetTotalNodes() > 1) {
                return;
            }
            if (!O_RevalidateWriteSet()) {
                if (u_sess->epoch_cxt.epoch_assigned) {
                    E_NodeManager::IncWriteAbortAfterSendTxnCounters(commit_epoch, node_id);
                    E_NodeManager::IncAbortTxnCounters(commit_epoch, node_id);
                }
                O_OccStatsRecordAbort(OCC_ABORT_REVALIDATE);
                ereport(ERROR,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                     errmsg("epoch: could not serialize access due to concurrent update (revalidate)"),
                     errhidestmt(true),
                     errhideprefix(true),
                     onlyfrontmsg(true)));
            }
            O_RecordCommit(csn);
            O_OccStatsRecordCommit();
            return;
        }

        O_RecordCommit(csn);
        O_OccStatsRecordCommit();
        return;
    }

    if (event == XACT_EVENT_END_TRANSACTION) {
        EpochRecvDiagLogXactHook("END", u_sess->epoch_cxt.deferred_commit ? "deferred" : "done");
        u_sess->epoch_cxt.deferred_commit = false;
        EpochReleaseTxnExecuting("end_transaction");
        txn->EndTransaction();
        E_ResetTxnEpochContext();
        return;
    }

    if (event == XACT_EVENT_ABORT) {
        EpochRecvDiagLogXactHook("ABORT", u_sess->epoch_cxt.deferred_commit ? "deferred" : "rollback");
        bool had_deferred = u_sess->epoch_cxt.deferred_commit;
        if (had_deferred) {
            u_sess->epoch_cxt.deferred_commit = false;
        }
        if (u_sess->epoch_cxt.epoch_assigned && E_IsSyncEpochMode()) {
            if (had_deferred) {
                E_NodeManager::IncWriteAbortAfterSendTxnCounters(u_sess->epoch_cxt.txn_epoch, EpochGetNodeId());
            }
            E_NodeManager::IncAbortTxnCounters(u_sess->epoch_cxt.txn_epoch, EpochGetNodeId());
        }
        EpochReleaseTxnExecuting("abort");
        txn->Rollback();
        E_ResetTxnEpochContext();
    }
}
