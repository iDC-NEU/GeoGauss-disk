#include "postgres.h"
#include <mutex>

#include <sys/time.h>

#include "epoch/e_xact.h"
#include "epoch/e_config.h"
#include "epoch/e_epoch.h"
#include "epoch/e_exec_track.h"
#include "epoch/e_read_frontier.h"
#include "epoch/e_net.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_pacer.h"
#include "epoch/e_perf.h"
#include "epoch/e_shmem.h"
#include "epoch/e_apply_shmem.h"
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
    /* 事务起点随 wire 传到 apply 侧，供发起节点统计"事务开始→写回完成"延迟 */
    wire->origin_start_us = u_sess->epoch_cxt.txn_start_us;
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
    }
    return !wire->rows.empty();
}

static bool E_ShouldTrackTxnEpoch(void)
{
    if (IsAutoVacuumWorkerProcess() || AmEpochWorkerProcess()) {
        return false;
    }
    /*
     * apply 写回连接（由 apply worker 用 libpq 连本库、带 xc_maintenance_mode=on）
     * 不参与 epoch：它若走 assign/pack/gate，就要等自己这批写集的 apply 完成，
     * 而 logical 又依赖该写回 → 死锁闭环（实测 apply worker 连 localhost:32000
     * 超时、apply_inflight 只增不减、logical 卡死、所有新会话卡 START gate）。
     */
    if (u_sess->attr.attr_common.xc_maintenance_mode) {
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
    /*
     * 连接状态不再单独维护：ZMQ 的 PUSH 发不出去会阻塞重试，
     * 收不到包会在接收游标上体现，不需要额外的“是否连上”判据。
     */
    return EpochGetTotalNodes() > 1;
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

/* pack 前仅在队列高压时等待 drain，不要求 logical 推进 *//*
 * 本地写集入队。
 *
 * 每个时间片固定 24 个名额，backend 通过共享内存的 pack_slot_seq 抢号：
 *   · 抢到 1..23  → 写集入队这一片
 *   · 抢到 24     → 由它塞收尾包，并把 physical 推到下一片
 *   · 抢到 >24    → 这一片已经满了，本事务放弃入队（计入 abort）
 *
 * 好处：每片条数固定、队列不可能溢出；有事务才推进，不产生空片；
 * 不再需要每 10ms 的定时推进线程。
 */
/*
 * 环形复用的进度下界 = min(logical, 最慢 peer 的 send 游标)。
 *
 * physical 领先它不能到 N：否则 epoch 与 epoch+N 会落进同一格。
 *  - logical 管的是计数器/账本的复用
 *  - 最慢 peer 的 send 管的是它自己那份发送队列的复用
 * 队列按 peer 拆分后，某个 peer 可能远远落后（这正是解耦的目的），
 * 所以必须按"最慢的那个"来卡，否则它的旧数据会被新片覆盖。
 */
static uint64_t E_ProgressFloor(void)
{
    const uint64_t logical = E_NodeManager::GetLogicalEpoch();
    const uint64_t send = EpochGetSendEpoch();      /* 所有 peer 的最小值 */
    return (send < logical) ? send : logical;
}

static bool E_PackWriteTxnToQueue(O_TxnManager* txn, uint64_t commit_epoch, uint32_t node_id)
{
    /*
     * 环形硬约束：physical 领先 logical 不能到 N，否则 epoch 与 epoch+N 共用
     * 计数器槽，EnsureEpochSlot 会把对方那片的 wire_merge_done 清零，
     * 该片永远判不完、merge 卡死（实测 node2 phys=10115 / merge=115，正好差 10000）。
     *
     * 这里才是真正决定“哪个 epoch 被填满”的地方：提交时重新读 physical 抢号。
     * 事务开始处的检查挡不住它 —— 事务可能在 phys-log=9000 时开始，提交时
     * physical 已跑到 10114，于是给 10114 抢号并封口，绕过准入检查。
     *
     * 提前 100 片刹车：physical 最多到 logical + N - 99，不会再撞槽。
     */
    for (;;) {
        const uint64_t phys = E_NodeManager::GetPhysicalEpoch();
        const uint64_t floor_ep = E_ProgressFloor();
        if (phys <= floor_ep || (phys - floor_ep) < (EPOCH_TXN_EPOCH_COUNT - 100)) {
            break;
        }
        pg_usleep(200);
        CHECK_FOR_INTERRUPTS();
    }

    const uint64_t epoch = E_NodeManager::GetPhysicalEpoch();
    if (epoch == 0) {
        return false;
    }

    const uint32_t ticket = E_NodeManager::NextPackSlotSeq(epoch);
    if (ticket == 0) {
        /* 格子已被别的时间片接管（环形复用），本笔放弃 */
        E_NodeManager::IncAbortTxnCounters(epoch, node_id);
        return false;
    }

    /* 名额用尽：这一片已经封口，本事务不入队 */
    if (ticket > EPOCH_TXN_SLOTS_PER_EPOCH) {
        E_NodeManager::IncAbortTxnCounters(epoch, node_id);
        return false;
    }

    /* 抢到最后一个名额：塞收尾包并推进 physical，本事务本身也要入队 */
    const bool is_sealer = (ticket == EPOCH_TXN_SLOTS_PER_EPOCH);

    E_NodeManager::IncLocalPackTxnCounters(epoch, node_id);

    uint64_t csn = EpochGetNextCommitCsn();
    txn->SetCommitSequenceNumber(csn);
    E_WireTxn wire;
    if (!E_BuildWireTxn(txn, csn, &wire)) {
        return false;
    }
    wire.epoch = epoch;
    E_NodeManager::IncChangeSetTxnCounters(epoch, node_id);

    std::string payload;
    if (!E_WireSerializeTxn(wire, &payload)) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("epoch: failed to serialize transaction")));
    }
    /*
     * 发给每个对端各一份（每 peer 独立队列）：某个对端慢时只堵它自己那条发送线程。
     */
    const uint32_t self_id = EpochGetNodeId();
    const uint32_t total_nodes = EpochGetTotalNodes();
    /*
     * 注意：self 槽（peer == self_id）也要压一份——
     * 自环由独立的 self 线程处理，这样"本端入队"不再受任何网络方向的速度影响。
     */
    for (uint32_t peer = 0; peer < total_nodes; ++peer) {
        if (!EpochTxnQueuePush(peer, epoch, payload.data(), static_cast<uint32_t>(payload.size()))) {
            ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("epoch: pending txn queue is full")));
        }
    }

    if (is_sealer) {
        EpochSealEpoch(epoch);
    }
    return true;
}

static void E_ResetTxnEpochContext(void)
{
    u_sess->epoch_cxt.txn_epoch = 0;
    u_sess->epoch_cxt.txn_start_us = 0;
    u_sess->epoch_cxt.epoch_assigned = false;
    u_sess->epoch_cxt.epoch_exec_counted = false;
    u_sess->epoch_cxt.epoch_sync_heap_recorded = false;
    u_sess->epoch_cxt.epoch_procarray_ended = false;
    EpochResetReadFrontier();
}

/*
 * XACT START：assign + executing++。
 * assign 后立即 executing++，防止 worker 将已 assign 的 epoch 误判为空并 seal。
 */
static void E_AssignEpochOnStart(void)
{
    if (!E_ShouldTrackTxnEpoch()) {
        return;
    }

    /* 事务起点打点：延迟统计要用 */
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    u_sess->epoch_cxt.txn_start_us =
        static_cast<uint64_t>(tv.tv_sec) * 1000000ULL + static_cast<uint64_t>(tv.tv_usec);

    /*
     * 事务的 epoch 直接取当前的 physical epoch，但必须同时满足两个条件：
     *
     *   ① 环形保护：physical 领先"进度下界"不能到 N，否则 epoch 与 epoch+N
     *      会落进同一格、互相覆盖。
     *      进度下界 = min(logical, 最慢 peer 的 send)。提前 100 片刹车后，
     *      physical 最多到 下界 + N - 99，不会再撞槽（实测撞槽会让
     *      wire_merge_done 被清零，merge 永久卡死）。
     *
     *   ② 快照就绪（放宽版）：允许 physical 领先 apply 最多 EPOCH_SNAPSHOT_LAG_MAX 片。
     *      =0 时等价于原来的"apply_ep >= phys"；现取 100，用略微陈旧的一致性换吞吐。
     *
     *   ② 带超时（2×RTT + 2s）：收尾阶段可能出现"最后一片始终收不齐"，
     *      此时 phys 比 apply 大 1，若无限等待会把所有终端挂死
     *      （实测 phys=2944/apply=2943，jTPCC 收尾 28 秒无响应被看门狗杀掉）。
     *      超时后放行并计数，宁可读到略旧的快照，也不挂死客户端。
     */
    const uint64_t snap_rtt_us =
        static_cast<uint64_t>(g_epoch_dyn_net_rtt_ms > 0 ? g_epoch_dyn_net_rtt_ms : 100) * 1000ULL;
    const uint64_t snap_wait_timeout_us = 2ULL * snap_rtt_us + 2000000ULL;
    (void)snap_rtt_us;

    /*
     * 允许 physical 领先 apply 的片数上限。
     *
     * 0   = 严格：事务总是基于"上一片已写完"的快照（原行为）
     * 100 = 宽松：允许读到最多落后 100 片的快照，换取更高吞吐
     *
     * 牺牲的是一致性（读到略旧的数据），换来的是 backend 不必每片都等 apply。
     */
    constexpr uint64_t EPOCH_SNAPSHOT_LAG_MAX = 10;

    /* 只对"快照等待"这一段计时；单调时钟，避免系统时间回拨导致误超时 */
    uint64_t snap_wait_start_us = 0;
    uint64_t assign_phys = 0;

    for (;;) {
        const uint64_t phys = E_NodeManager::GetPhysicalEpoch();
        const uint64_t floor_ep = E_ProgressFloor();
        const uint64_t apply_ep = EpochApplyQueueGetApplyEpoch();

        /* ① 环形保护 */
        if (phys > floor_ep && (phys - floor_ep) >= (EPOCH_TXN_EPOCH_COUNT - 100)) {
            pg_usleep(200);
            CHECK_FOR_INTERRUPTS();
            continue;
        }

        /*
         * ② 快照就绪（放宽版）。
         *
         *    原判据：apply_ep >= phys（严格"上一片已写完"）
         *    现判据：phys - apply_ep <= EPOCH_SNAPSHOT_LAG_MAX
         *            即允许领先最多 100 片，事务可能读到落后的快照。
         *
         *    带超时（2×RTT + 2s）：收尾时最后一片可能永远收不齐，
         *    无限等待会把所有终端挂死；超时放行并计数。
         */
        if (phys > apply_ep + EPOCH_SNAPSHOT_LAG_MAX) {
            struct timespec ts;
            clock_gettime(CLOCK_MONOTONIC, &ts);
            const uint64_t now_us =
                static_cast<uint64_t>(ts.tv_sec) * 1000000ULL + static_cast<uint64_t>(ts.tv_nsec / 1000);

            if (snap_wait_start_us == 0) {
                snap_wait_start_us = now_us;
            } else if (now_us - snap_wait_start_us >= snap_wait_timeout_us) {
                EpochPerfNoteSnapTimeout();
                EPOCH_DBG_FPRINTF(
                    "EPOCH_DBG snap_wait_timeout node=%u phys=%lu apply=%lu waited_ms=%lu 放行\n",
                    EpochGetNodeId(), (unsigned long)phys, (unsigned long)apply_ep,
                    (unsigned long)((now_us - snap_wait_start_us) / 1000ULL));
                fflush(stderr);
                assign_phys = phys;
                break;
            }
            pg_usleep(200);
            CHECK_FOR_INTERRUPTS();
            continue;
        }

        snap_wait_start_us = 0;   /* 快照已就绪：计时器归零，下次重新计时 */
        assign_phys = phys;
        break;
    }

    u_sess->epoch_cxt.txn_epoch = assign_phys;
    u_sess->epoch_cxt.epoch_assigned = true;

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
 * 已在途事务 COMMIT/pack：只等 logical 关窗。
 */
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
    /* apply 写回连接（xc_maintenance_mode）不走 epoch pack：见 EpochAssignTxnEpoch */
    if (u_sess->attr.attr_common.xc_maintenance_mode) {
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


    if (EpochGetTotalNodes() > 1) {
        (void)E_PackWriteTxnToQueue(txn, commit_epoch, node_id);
    }
    EpochReleaseTxnExecuting("sync_precommit_pack");
    txn->ReleaseOccLocks();

    u_sess->epoch_cxt.deferred_commit = true;
}

void EpochXactCallback(XactEvent event)
{
    if (!g_enable_epoch_multimaster) {
        return;
    }

    if (event == XACT_EVENT_STMT_FINISH) {
        if (u_sess->epoch_cxt.epoch_exec_counted && !u_sess->epoch_cxt.deferred_commit) {
        }
        return;
    }

    if (event == XACT_EVENT_PRE_COMMIT) {
        O_TxnManager* pre_txn = u_sess->occ_cxt.txn_manager;
        if (pre_txn != nullptr && E_TxnHasWriteSet(pre_txn) && E_IsSyncEpochMode()) {
            EpochSyncPreCommitWritePath(pre_txn);
            return;
        }
        if (u_sess->epoch_cxt.epoch_exec_counted && !u_sess->epoch_cxt.deferred_commit) {
        }
        return;
    }

    if (event == XACT_EVENT_START) {
        if (E_ShouldTrackTxnEpoch()) {
            E_AssignEpochOnStart();
        }
        return;
    }

    O_TxnManager* txn = u_sess->occ_cxt.txn_manager;
    if (txn == nullptr) {
        if (event == XACT_EVENT_ABORT || event == XACT_EVENT_END_TRANSACTION) {
            EpochReleaseTxnExecuting("no_occ_txn");
            E_ResetTxnEpochContext();
        }
        return;
    }

    if (event == XACT_EVENT_COMMIT) {
        bool has_write = E_TxnHasWriteSet(txn);
        if (!has_write) {
            /* 只读：若曾通过 OCC 读路径分配 epoch，等待关窗 */
            EpochReleaseTxnExecuting("readonly_commit");
            if (u_sess->epoch_cxt.epoch_assigned && E_IsSyncEpochMode()) {
                O_OccStatsRecordCommit();
            }
            E_ResetTxnEpochContext();
            return;
        }

        /*
         * sync 多主写：PRE_COMMIT 已 pack 并 deferred，COMMIT 不再等 gate。
         */
        if (E_IsSyncEpochMode() && u_sess->epoch_cxt.deferred_commit && EpochGetTotalNodes() > 1) {
            return;
        }
        if (E_IsSyncEpochMode() && u_sess->epoch_cxt.epoch_sync_heap_recorded) {
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

        /*
         * 单节点 sync 退化：仍走 COMMIT hook（无 PRE_COMMIT 写路径）。
         */
        if (E_IsSyncEpochMode()) {
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
        u_sess->epoch_cxt.deferred_commit = false;
        EpochReleaseTxnExecuting("end_transaction");
        txn->EndTransaction();
        E_ResetTxnEpochContext();
        return;
    }

    if (event == XACT_EVENT_ABORT) {
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
        /*
         * backend 侧回滚也计入 abort 统计。
         * 这类事务没有进入发送队列、也不会被 merge 判输，
         * 是 merge abort 之外的另一半来源；两者合起来才是完整的 abort 量。
         */
        EpochPerfAddApplyAbortTxn(1);
        txn->Rollback();
        E_ResetTxnEpochContext();
    }
}
