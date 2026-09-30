//
// Created by hcj2003 on 9/16/25.
//

#include "occ/o_heapam.h"
#include "occ/o_executor.h"
#include "occ/o_txn.h"
#include "occ/occ_internal.h"
#include "epoch/e_read_frontier.h"
#include "epoch/e_heap_version.h"
#include "epoch/e_apply_buflock.h"
#include "epoch/e_types.h"

#include "portability/instr_time.h"

#include "access/tableam.h"
#include "access/sysattr.h"
#include "access/visibilitymap.h"
#include "catalog/index.h"
#include "utils/lsyscache.h"
#include "access/tuptoaster.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"

#include "occ/occ_mem.h"

/* 释放 buffer 锁之后机会性 prune，不阻塞其它 find/update */
static void EpochHeapOpportunisticPrune(Relation relation, BlockNumber blk, const ItemPointerData* tip_hint)
{
    if (!EpochHeapVersionChainEnabled(relation) || blk == InvalidBlockNumber) {
        return;
    }
    uint64_t f_safe = EpochGetPruneSafeFrontier();
    if (f_safe == 0) {
        return;
    }
    Buffer buf = ReadBuffer(relation, blk);
    if (!ConditionalLockBuffer(buf)) {
        ReleaseBuffer(buf);
        return;
    }
    (void)EpochHeapPrunePageLimited(relation, buf, f_safe, tip_hint, EPOCH_PRUNE_HOT_MAX_ITEMS);
    /* 大页也做限量全页回收（O(n)+max_items）；旧 FULL_MAXOFF 门槛会放过 stock 膨胀页 */
    (void)EpochHeapPrunePageLimited(relation, buf, f_safe, nullptr, EPOCH_PRUNE_PAGE_MAX_ITEMS);
    LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buf);
}

/* prune 后 Item 指针可能因 PageRepairFragmentation 失效，按 TID 重钉 */
static void EpochRefreshTupleFromTid(HeapTupleData* tup, Page page, ItemPointer tid)
{
    ItemId lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
    if (!ItemIdIsNormal(lp)) {
        return;
    }
    tup->t_data = (HeapTupleHeader)PageGetItem(page, lp);
    tup->t_len = ItemIdGetLength(lp);
    HeapTupleCopyBaseFromPage(tup, page);
}

static void HeapSatisfiesHOTUpdate(Relation relation, Bitmapset *hot_attrs, Bitmapset *key_attrs, Bitmapset *id_attrs,
                                   bool *satisfies_hot, bool *satisfies_key, bool *satisfies_id, HeapTuple oldtup, HeapTuple newtup, char *page);

static bool heap_tuple_attr_equals(TupleDesc tupdesc, int attrnum, HeapTuple tup1, HeapTuple tup2, char *page);

static XLogRecPtr log_heap_new_cid(Relation relation, HeapTuple tup);

static XLogRecPtr log_heap_new_cid_insert(xl_heap_new_cid *xlrec, int bucketid);

static HeapTuple ExtractReplicaIdentity(Relation relation, HeapTuple tp, bool key_changed, bool *copy,
                                        char *relreplident);

static uint8 ComputeInfobits(uint16 infomask, uint16 infomask2);

static XLogRecPtr log_heap_update(Relation reln, Buffer oldbuf, HeapTuple oldtup, Buffer newbuf, HeapTuple newtup,
                                  HeapTuple old_key_tuple, bool all_visible_cleared, bool new_all_visible_cleared,
                                  char relreplident);

static Oid occ_heap_insert(Relation relation, HeapTuple tup, CommandId cid, int options, BulkInsertState bistate, bool istoast);

static HeapTuple heap_prepare_insert(Relation relation, HeapTuple tup, CommandId cid, int options);

void O_FormIndexDatum(IndexInfo *indexInfo, TupleTableSlot *slot, Datum *values, bool *isnull);

static void O_FormIndexDatumFromHeap(IndexInfo *indexInfo, Relation heapRel, HeapTuple tup,
                                     Datum *values, bool *isnull);

/*
 * 并发 prune/update 导致 ctid 失效时，用唯一索引从 slot 重定位 tip TID。
 * 成功则写回 *tupleid 并返回 true。
 */
static bool O_RefindTipTidFromSlot(Relation relation, EState *estate, TupleTableSlot *slot, ItemPointer tupleid)
{
    if (relation == nullptr || estate == nullptr || slot == nullptr || tupleid == nullptr) {
        return false;
    }
    if (!EpochHeapVersionChainEnabled(relation)) {
        return false;
    }

    List *indexoidlist = RelationGetIndexList(relation);
    if (indexoidlist == NIL) {
        return false;
    }

    bool found = false;
    ListCell *lc = nullptr;
    foreach (lc, indexoidlist) {
        Oid idxoid = lfirst_oid(lc);
        Relation idxrel = index_open(idxoid, AccessShareLock);
        if (!idxrel->rd_index->indisunique || !idxrel->rd_index->indisvalid) {
            index_close(idxrel, AccessShareLock);
            continue;
        }

        TupleTableSlot *outslot = MakeSingleTupleTableSlot(RelationGetDescr(relation));
        FakeRelationPartition fake;
        errno_t rc = memset_s(&fake, sizeof(fake), 0, sizeof(fake));
        securec_check(rc, "\0", "\0");

        if (RelationFindReplTupleEpoch(estate, relation, idxoid, slot, outslot, &fake)) {
            HeapTuple htup = (HeapTuple)outslot->tts_tuple;
            if (htup != nullptr && ItemPointerIsValid(&htup->t_self)) {
                ItemPointerCopy(&htup->t_self, tupleid);
                found = true;
            }
        }

        ExecDropSingleTupleTableSlot(outslot);
        index_close(idxrel, AccessShareLock);
        if (found) {
            break;
        }
    }
    list_free(indexoidlist);
    return found;
}

List *O_ExecInsertIndexTuples(TupleTableSlot *slot, ItemPointer tupleid, EState *estate,
                              Bitmapset *modifiedIdxAttrs);

RC O_UpdateRow(O_TxnManager *txn, HeapTuple heap_tuple, TupleTableSlot *slot, EState *estate, bool update_index = false)
{
    RC rc;

    do
    {
        if (txn == nullptr || txn->m_accessMgr == nullptr || txn->m_accessMgr->GetLastAccess() == nullptr) {
            return RC_ERROR;
        }

        txn->m_accessMgr->SetLastAccessedEstate(estate);

        txn->m_accessMgr->SetLastAccessedSlot(slot, update_index);

        rc = txn->UpdateLastRowState(AccessType::WR, update_index);
        if (rc != RC_OK) {
            break;
        }

        O_Access *access = txn->m_accessMgr->GetLastAccess();
        if (access == nullptr || heap_tuple == nullptr || heap_tuple->t_data == nullptr) {
            rc = RC_ERROR;
            break;
        }

        /*
         * 必须用 O_heap_copytuple 按新 t_len 重新分配 draft。
         * EnsureRowAccessForModify / CreateMVCCDraft 按旧行长度分配缓冲区，
         * 原地 memcpy 会在新 tuple 更大时写穿 arena，污染相邻 O_Access/HeapTuple 指针
         * （E_BuildWireTxn pack 时 SIGSEGV）。
         */
        HeapTuple newDraft = O_heap_copytuple(heap_tuple);
        if (newDraft == nullptr) {
            rc = RC_ERROR;
            break;
        }
        if (!ItemPointerIsValid(&newDraft->t_self) &&
            OccTidIsPlausible(&access->m_origSentinel.tuple_id)) {
            newDraft->t_self = access->m_origSentinel.tuple_id;
        }

        if (access->m_type == INS) {
            access->m_localInsertRow = newDraft;
        } else {
            access->m_localRow = newDraft;
        }
    } while (0);

    return rc;
}

static void O_DeferRegisterUpdateWriteSet(O_TxnManager *txn, Relation relation, ItemPointer tupleid,
    HeapTupleData *oldtup, HeapTuple newtuple, TupleTableSlot *slot, EState *estate, Buffer buffer)
{
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    if (!txn->m_accessMgr->EnsureRowAccessForModify(relation, tupleid, oldtup)) {
        ReleaseBuffer(buffer);
        ereport(ERROR, (errmsg("OCC: failed to establish row access for deferred update")));
    }
    /*
     * sync 双主 defer：写集仅登记旧 tid + 新 tuple draft，不区分 HOT。
     * heap/index 维护（含跨页 UPDATE）由 apply 侧 tableam_tuple_update 完成。
     */
    O_UpdateRow(txn, newtuple, slot, estate, false);
    ReleaseBuffer(buffer);
}

// 接口 只判断索引列修改,目前tpcc不涉及
TupleTableSlot *O_ExecUpdate(ItemPointer tupleid,
                             Oid oldPartitionOid,
                             int2 bucketid, HeapTupleHeader oldtuple, TupleTableSlot *slot, TupleTableSlot *planSlot, EPQState *epqstate,
                             ModifyTableState *node, bool canSetTag, bool partKeyUpdate, TM_Result *uresultp, char *partExprKeyStr, TM_FailureData *tmfdp)
{
    EState *estate = node->ps.state;
    Tuple tuple = NULL;
    ResultRelInfo *result_rel_info = NULL;
    Relation result_relation_desc;
    TM_Result result;

    result_rel_info = estate->es_result_relation_info;
    result_relation_desc = result_rel_info->ri_RelationDesc;

    O_TxnManager *txn = GetTxnManager();
    tuple = tableam_tslot_get_tuple_from_slot(result_relation_desc, slot);

    // 判断是否更改索引
    Bitmapset *hot_attrs = NULL;
    Bitmapset *key_attrs = NULL;
    Bitmapset *id_attrs = NULL;

    bool satisfies_hot = false;
    bool satisfies_key = false;
    bool satisfies_id = false;

    Relation relation = result_relation_desc;

    hot_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_ALL);
    key_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_KEY);
    id_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_IDENTITY_KEY);

    HeapTupleData oldtup;

    Page page;
    BlockNumber block;
    Buffer buffer = InvalidBuffer;
    ItemId lp;
    ItemPointerData working_tid = *tupleid;
    ItemPointer tid = &working_tid;

    block = ItemPointerGetBlockNumber(tid);
    buffer = ReadBuffer(relation, block);
    page = BufferGetPage(buffer);

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
    if (!ItemIdIsNormal(lp)) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
        buffer = InvalidBuffer;
        if (!O_RefindTipTidFromSlot(relation, estate, slot, tid)) {
            bms_free(hot_attrs);
            bms_free(key_attrs);
            bms_free(id_attrs);
            if (uresultp != nullptr) {
                *uresultp = TM_Deleted;
            }
            return NULL;
        }
        block = ItemPointerGetBlockNumber(tid);
        buffer = ReadBuffer(relation, block);
        page = BufferGetPage(buffer);
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
        if (!ItemIdIsNormal(lp)) {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buffer);
            bms_free(hot_attrs);
            bms_free(key_attrs);
            bms_free(id_attrs);
            if (uresultp != nullptr) {
                *uresultp = TM_Deleted;
            }
            return NULL;
        }
    }

    oldtup.t_data = (HeapTupleHeader)PageGetItem(page, lp);
    oldtup.t_len = ItemIdGetLength(lp);
    oldtup.t_self = *tid;
    oldtup.t_tableOid = RelationGetRelid(relation);
    oldtup.t_bucketId = RelationGetBktid(relation);

    HeapSatisfiesHOTUpdate(relation, hot_attrs, key_attrs, id_attrs, &satisfies_hot,
                           &satisfies_key, &satisfies_id, &oldtup, (HeapTuple)tuple, page);

    if (OccSyncDeferHeapWrite()) {
        O_DeferRegisterUpdateWriteSet(txn, relation, tid, &oldtup, (HeapTuple)tuple, slot, estate, buffer);
        bms_free(hot_attrs);
        bms_free(key_attrs);
        bms_free(id_attrs);
        return NULL;
    }

    (void)GetCurrentTransactionId();

    if (!satisfies_hot)
    {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

        occ_heap_insert(relation, (HeapTuple)tuple, estate->es_output_cid, 0, NULL, false);

        if (!txn->m_accessMgr->EnsureRowAccessForModify(relation, tid, &oldtup)) {
            ereport(ERROR, (errmsg("OCC: failed to establish row access for non-HOT update")));
        }
        O_UpdateRow(txn, (HeapTuple)tuple, slot, estate, true); // 旧的版本需要无效掉

        if (result_rel_info->ri_NumIndices > 0)
        {
            Bitmapset *modifiedIdxAttrs = NULL;
            TupleTableSlot *oldslot = NULL;

            ExecIndexTuplesState exec_index_tuples_state;
            exec_index_tuples_state.estate = estate;
            exec_index_tuples_state.targetPartRel = NULL;
            exec_index_tuples_state.p = NULL;
            exec_index_tuples_state.conflict = NULL;
            exec_index_tuples_state.rollbackIndex = false;
            // 这个tupleid是旧的还是新的?
            tableam_tops_exec_update_index_tuples(slot, oldslot, relation, node, tuple, tid,
                                                  exec_index_tuples_state, bucketid, modifiedIdxAttrs);
        }

        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

        ItemPointer pTSelf = NULL;
        pTSelf = tableam_tops_get_t_self(result_relation_desc, tuple);
        O_InsertRow(pTSelf, estate, (HeapTuple)tuple); // 注册读写集，用于写回rowcsn
    }
    else
    {
        /*
         * HOT：oldtup 已拷入栈变量，释放 EXCLUSIVE 后再 O_UpdateRow（内含 START gate），
         * 避免 gate 自旋期间持页锁阻塞 epoch worker apply 的 index fetch。
         */
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        if (!txn->m_accessMgr->EnsureRowAccessForModify(relation, tid, &oldtup)) {
            ReleaseBuffer(buffer);
            ereport(ERROR, (errmsg("OCC: failed to establish row access for HOT update")));
        }
        O_UpdateRow(txn, (HeapTuple)tuple, slot, estate);
        ReleaseBuffer(buffer);
        return NULL;
    }

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    ReleaseBuffer(buffer);

    return NULL;
}

bool O_OpFusionExecUpdate(Relation relation, ItemPointer tupleid, HeapTuple newtuple, TupleTableSlot *slot,
    EState *estate, int2 bucketid, bool *needsIndexUpdate, bool *indexUpdateDone)
{
    O_TxnManager *txn = GetTxnManager();
    ResultRelInfo *result_rel_info = estate->es_result_relation_info;
    Tuple tuple = (Tuple)newtuple;

    Bitmapset *hot_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_ALL);
    Bitmapset *key_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_KEY);
    Bitmapset *id_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_IDENTITY_KEY);

    bool satisfies_hot = false;
    bool satisfies_key = false;
    bool satisfies_id = false;

    BlockNumber block = ItemPointerGetBlockNumber(tupleid);
    Buffer buffer = ReadBuffer(relation, block);
    Page page = BufferGetPage(buffer);

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    ItemId lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tupleid));
    if (!ItemIdIsNormal(lp)) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
        ItemPointerData fresh_tid = *tupleid;
        if (!O_RefindTipTidFromSlot(relation, estate, slot, &fresh_tid)) {
            bms_free(hot_attrs);
            bms_free(key_attrs);
            bms_free(id_attrs);
            return false;
        }
        *tupleid = fresh_tid;
        block = ItemPointerGetBlockNumber(tupleid);
        buffer = ReadBuffer(relation, block);
        page = BufferGetPage(buffer);
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tupleid));
        if (!ItemIdIsNormal(lp)) {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buffer);
            bms_free(hot_attrs);
            bms_free(key_attrs);
            bms_free(id_attrs);
            return false;
        }
    }

    HeapTupleData oldtup;
    oldtup.t_data = (HeapTupleHeader)PageGetItem(page, lp);
    oldtup.t_len = ItemIdGetLength(lp);
    oldtup.t_self = *tupleid;
    oldtup.t_tableOid = RelationGetRelid(relation);
    oldtup.t_bucketId = RelationGetBktid(relation);

    HeapSatisfiesHOTUpdate(relation, hot_attrs, key_attrs, id_attrs, &satisfies_hot,
                           &satisfies_key, &satisfies_id, &oldtup, newtuple, page);

    if (OccSyncDeferHeapWrite()) {
        *needsIndexUpdate = false;
        *indexUpdateDone = false;
        O_DeferRegisterUpdateWriteSet(txn, relation, tupleid, &oldtup, newtuple, slot, estate, buffer);
        bms_free(hot_attrs);
        bms_free(key_attrs);
        bms_free(id_attrs);
        return true;
    }

    *needsIndexUpdate = !satisfies_hot || satisfies_key;
    *indexUpdateDone = false;

    if (!satisfies_hot) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

        occ_heap_insert(relation, newtuple, estate->es_output_cid, 0, NULL, false);

        if (!txn->m_accessMgr->EnsureRowAccessForModify(relation, tupleid, &oldtup)) {
            ReleaseBuffer(buffer);
            ereport(ERROR, (errmsg("OCC: failed to establish row access for non-HOT update")));
        }
        O_UpdateRow(txn, newtuple, slot, estate, true);

        if (result_rel_info->ri_NumIndices > 0) {
            Bitmapset *modifiedIdxAttrs = NULL;
            TupleTableSlot *oldslot = NULL;

            ExecIndexTuplesState exec_index_tuples_state;
            exec_index_tuples_state.estate = estate;
            exec_index_tuples_state.targetPartRel = NULL;
            exec_index_tuples_state.p = NULL;
            exec_index_tuples_state.conflict = NULL;
            exec_index_tuples_state.rollbackIndex = false;
            tableam_tops_exec_update_index_tuples(slot, oldslot, relation, NULL, tuple, tupleid,
                                                  exec_index_tuples_state, bucketid, modifiedIdxAttrs);
        }

        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

        ItemPointer pTSelf = tableam_tops_get_t_self(relation, tuple);
        O_InsertRow(pTSelf, estate, newtuple);
        *indexUpdateDone = true;
    } else {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        if (!txn->m_accessMgr->EnsureRowAccessForModify(relation, tupleid, &oldtup)) {
            ReleaseBuffer(buffer);
            ereport(ERROR, (errmsg("OCC: failed to establish row access for HOT update")));
        }
        O_UpdateRow(txn, newtuple, slot, estate, false);
        *needsIndexUpdate = satisfies_key;

        ReleaseBuffer(buffer);
        bms_free(hot_attrs);
        bms_free(key_attrs);
        bms_free(id_attrs);

        (void)GetCurrentTransactionId();
        return true;
    }

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);

    bms_free(hot_attrs);
    bms_free(key_attrs);
    bms_free(id_attrs);

    (void)GetCurrentTransactionId();
    return true;
}

void O_heap_commit_insert_csn(Relation relation, ItemPointer otid, uint64_t csn)
{
    Buffer buffer;
    Page page;
    ItemId lp;
    HeapTupleHeader hdr;

    if (relation == nullptr || otid == nullptr) {
        return;
    }

    buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(otid));
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buffer);
    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(otid));
    if (ItemIdIsNormal(lp)) {
        hdr = (HeapTupleHeader)PageGetItem(page, lp);
        hdr->m_rowCSN = csn;
        hdr->is_latest = true;
        MarkBufferDirty(buffer);
    }
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);
}

//=======================================================
// writeback
// 判断能否放在同一页面中
TM_Result O_heap_update(Relation relation, ItemPointer otid, HeapTuple newtup,
                        CommandId cid, uint64_t csn, TupleTableSlot *slot, EState *estate)
{
    Page page, newpage;
    BlockNumber block;
    Buffer buffer = InvalidBuffer;
    ItemId lp;
    HeapTupleData oldtup;
    TM_Result result;
    TransactionId xid = GetCurrentTransactionId();
    Bitmapset* hot_attrs = NULL;
    Bitmapset* key_attrs = NULL;
    Bitmapset* id_attrs = NULL;
    TransactionId xmax_new_tuple;
    TransactionId xmax_old_tuple;
    uint16 infomask_old_tuple;
    uint16 infomask2_old_tuple;
    uint16 infomask_new_tuple;
    uint16 infomask2_new_tuple;
    Buffer newbuf = InvalidBuffer;
    HeapTuple heaptup;
    bool already_marked = false;
    bool is_combo = false;

    bool satisfies_key = false;
    bool satisfies_id = false;
    bool satisfies_hot = true;
    bool use_hot_update = false;
    bool csn_cross_page_heap_only = false; /* HEAP_ONLY without HOT_UPDATED */

    Size new_tup_size, pagefree;
    int options = 0;

    bool all_visible_cleared = false;
    bool all_visible_cleared_new = false;

    HeapTuple old_key_tuple = NULL;
    bool old_key_copied = false;
    char relreplident;

    ItemPointerData prune_hint;
    ItemPointerSetInvalid(&prune_hint);

    block = ItemPointerGetBlockNumber(otid);
    buffer = ReadBuffer(relation, block);
    page = BufferGetPage(buffer);

    Buffer vmbuffer = InvalidBuffer;
    Buffer vmbuffer_new = InvalidBuffer;
    BlockNumber rel_end_block = InvalidBlockNumber;

    if (PageIsAllVisible(page))
    {
        visibilitymap_pin(relation, block, &vmbuffer);
    }

    EpochApplyBufLockExclusive(buffer);

    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(otid));
    if (!ItemIdIsNormal(lp)) {
        EpochApplyBufLockUnlock(buffer);
        ReleaseBuffer(buffer);
        if (BufferIsValid(vmbuffer)) {
            ReleaseBuffer(vmbuffer);
        }
        return TM_Deleted;
    }
    oldtup.t_data = (HeapTupleHeader)PageGetItem(page, lp);
    oldtup.t_len = ItemIdGetLength(lp);
    oldtup.t_self = *otid;
    oldtup.t_tableOid = RelationGetRelid(relation);
    oldtup.t_bucketId = RelationGetBktid(relation);

    /*
     * CSN 版本链：apply find 与 update 之间 tip 可能已前进；必须在 EXCLUSIVE 下再追到
     * 当前 is_latest，否则会分叉出多个 tip（warehouse 曾出现每行近百个 is_latest）。
     */
    if (EpochHeapVersionChainEnabled(relation)) {
        Buffer tip_buf = buffer;
        instr_time tip_t0;
        INSTR_TIME_SET_CURRENT(tip_t0);
        if (!EpochHeapLockFollowToTip(relation, otid, &tip_buf, &oldtup)) {
            EpochApplyBufLockUnlock(tip_buf);
            ReleaseBuffer(tip_buf);
            if (BufferIsValid(vmbuffer)) {
                ReleaseBuffer(vmbuffer);
            }
            return TM_Deleted;
        }
        {
            instr_time tip_t1;
            INSTR_TIME_SET_CURRENT(tip_t1);
            INSTR_TIME_SUBTRACT(tip_t1, tip_t0);
            EpochApplyBufLockAddFollowTipUs(static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(tip_t1)));
        }
        if (tip_buf != buffer) {
            /* tip 在其它页：释放旧页相关 vm pin，按 tip 页重钉 */
            if (BufferIsValid(vmbuffer)) {
                ReleaseBuffer(vmbuffer);
                vmbuffer = InvalidBuffer;
            }
            buffer = tip_buf;
            block = BufferGetBlockNumber(buffer);
            page = BufferGetPage(buffer);
            if (PageIsAllVisible(page)) {
                visibilitymap_pin(relation, block, &vmbuffer);
            }
        } else {
            page = BufferGetPage(buffer);
        }
    }

    hot_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_ALL);
    key_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_KEY);
    id_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_IDENTITY_KEY);

    HeapSatisfiesHOTUpdate(relation, hot_attrs, key_attrs, id_attrs, &satisfies_hot, &satisfies_key, &satisfies_id,
                           &oldtup, newtup, (char*)page);

    HeapTupleCopyBaseFromPage(&oldtup, BufferGetPage(buffer));

    if (O_HeapUseCsnEpochSemantics()) {
        /*
         * 索引列未变：旧索引项 + t_ctid 链仍有效，无需 insert 新索引项。
         * 索引列已变：旧行须 xid xmax，否则 HEAP_XMAX_INVALID 下 _bt_check_unique
         * 仍视旧索引项 alive，跨页 UPDATE 会 duplicate PK（Payment 写 warehouse 触发）。
         */
        if (!satisfies_hot) {
            xmax_old_tuple = xid;
            infomask_old_tuple = 0;
        } else {
            xmax_old_tuple = InvalidTransactionId;
            infomask_old_tuple = HEAP_XMAX_INVALID;
        }
    } else {
        xmax_old_tuple = xid;
        infomask_old_tuple = 0;
    }
    infomask2_old_tuple = 0;
    infomask_new_tuple = HEAP_XMAX_INVALID;
    infomask2_new_tuple = 0;
    xmax_new_tuple = 0;

    newtup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
    newtup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);

    heap_page_prepare_for_xid(relation, buffer, xid, false);
    if (TransactionIdIsNormal(xmax_old_tuple))
    {
        heap_page_prepare_for_xid(relation, buffer, xmax_old_tuple,
                                  (infomask_old_tuple & HEAP_XMAX_IS_MULTI) ? true : false);
    }
    HeapTupleCopyBaseFromPage(newtup, page);

    HeapTupleSetXmin(newtup, xid);
    HeapTupleHeaderSetCmin(newtup->t_data, cid);
    newtup->t_data->t_infomask |= (HEAP_UPDATED | infomask_new_tuple);
    newtup->t_data->t_infomask2 |= infomask2_new_tuple;
    HeapTupleHeaderSetXmax(page, newtup->t_data, xmax_new_tuple); /* for cleanliness */
    newtup->t_tableOid = RelationGetRelid(relation);
    newtup->t_bucketId = RelationGetBktid(relation);
    newtup->t_data->m_rowCSN = csn;
    newtup->t_data->is_latest = true;
    if (O_HeapUseCsnEpochSemantics()) {
        newtup->t_data->m_status = EpochHeapCommitEpochForWrite();
    }

    HeapTupleHeaderAdjustCmax(oldtup.t_data, &cid, &is_combo, buffer);

    pagefree = PageGetHeapFreeSpace(page);
    new_tup_size = MAXALIGN(newtup->t_len);

    /*
     * 空间不足时先在已持有的 EXCLUSIVE 下做限量 prune（对齐 heap_update），尽量同页 HOT。
     * 完全跳过会导致跨页链爆炸：异页后继旧逻辑不回收 → heap_insert/find 随时间恶化。
     * 限量 + O(n) 前驱表，避免旧 O(n²) 长持锁。
     */
    if (EpochHeapVersionChainEnabled(relation) && new_tup_size > pagefree) {
        uint64_t f_safe = EpochGetPruneSafeFrontier();
        if (f_safe != 0) {
            instr_time prune_t0;
            INSTR_TIME_SET_CURRENT(prune_t0);
            (void)EpochHeapPrunePageLimited(relation, buffer, f_safe, &oldtup.t_self, EPOCH_PRUNE_PAGE_MAX_ITEMS);
            EpochRefreshTupleFromTid(&oldtup, page, &oldtup.t_self);
            pagefree = PageGetHeapFreeSpace(page);
            if (new_tup_size > pagefree) {
                (void)EpochHeapPrunePageLimited(relation, buffer, f_safe, nullptr, EPOCH_PRUNE_PAGE_MAX_ITEMS);
                EpochRefreshTupleFromTid(&oldtup, page, &oldtup.t_self);
                pagefree = PageGetHeapFreeSpace(page);
            }
            instr_time prune_t1;
            INSTR_TIME_SET_CURRENT(prune_t1);
            INSTR_TIME_SUBTRACT(prune_t1, prune_t0);
            EpochApplyBufLockAddPruneUs(static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(prune_t1)));
        }
    }

    if (new_tup_size > pagefree)
    {
        /* Clear obsolete visibility flags ... */
        oldtup.t_data->t_infomask &= ~HEAP_XMAX_BITS;
        oldtup.t_data->t_infomask2 &= ~(HEAP_XMAX_LOCK_ONLY | HEAP_KEYS_UPDATED);
        oldtup.t_data->t_infomask |= infomask_old_tuple;
        oldtup.t_data->t_infomask2 |= infomask2_old_tuple;
        HeapTupleClearHotUpdated(&oldtup);
        HeapTupleHeaderSetXmax(page, oldtup.t_data, xmax_old_tuple);
        if (TransactionIdIsValid(xmax_old_tuple)) {
            HeapTupleHeaderSetCmax(oldtup.t_data, cid, is_combo);
        }
        if (!O_HeapUseCsnEpochSemantics()) {
            oldtup.t_data->m_rowCSN = csn;
            oldtup.t_data->is_latest = false;
            /* temporarily make it look not-updated */
            oldtup.t_data->t_ctid = oldtup.t_self;
        }
        /*
         * CSN 路径：勿在换页窗口把 tip 标成 is_latest=false 且 t_ctid=self
         * （既非 tip 也非 superseded，易残留多 tip）。最终 CRIT 段再链接。
         */

        HeapTupleCopyBaseFromPage(&oldtup, page);

        already_marked = true;
        /* 换页前暂放 tip 锁：暂停 excl_hold 计时 */
        EpochApplyBufLockMarkExclReleased();
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

        heaptup = newtup;

        if (new_tup_size > pagefree)
        {
            /*
             * RelationGetBufferForTuple 返回时已对 newbuf 与 otherBuffer(本 tip 页)
             * 都加好 EXCLUSIVE；切勿再 LockBuffer(buffer)，否则对本进程自死锁。
             */
            instr_time gb_t0;
            INSTR_TIME_SET_CURRENT(gb_t0);
            newbuf = RelationGetBufferForTuple(relation, heaptup->t_len, buffer, options, NULL, &vmbuffer_new,
                                               &vmbuffer, rel_end_block);
            instr_time gb_t1;
            INSTR_TIME_SET_CURRENT(gb_t1);
            INSTR_TIME_SUBTRACT(gb_t1, gb_t0);
            EpochApplyBufLockAddGetBufUs(static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(gb_t1)));
            EpochApplyBufLockMarkExclHeld();
        }

        /*
         * 解锁窗口内并发 apply 可能已推进 tip。同页内再追到 tip；若 tip 已跨页，
         * 放弃本次换页更新（避免释放 otherBuffer 破坏 RelationGetBufferForTuple 约定）。
         */
        if (EpochHeapVersionChainEnabled(relation) && BufferIsValid(buffer)) {
            page = BufferGetPage(buffer);
            BlockNumber tip_blk = BufferGetBlockNumber(buffer);
            lp = PageGetItemId(page, ItemPointerGetOffsetNumber(&oldtup.t_self));
            if (ItemIdIsNormal(lp)) {
                oldtup.t_data = (HeapTupleHeader)PageGetItem(page, lp);
                oldtup.t_len = ItemIdGetLength(lp);
                HeapTupleCopyBaseFromPage(&oldtup, page);
            }
            int hop = 0;
            while (oldtup.t_data != nullptr && !oldtup.t_data->is_latest &&
                   !ItemPointerEquals(&oldtup.t_self, &oldtup.t_data->t_ctid) &&
                   ItemPointerGetBlockNumber(&oldtup.t_data->t_ctid) == tip_blk &&
                   hop++ < EPOCH_HEAP_VERSION_CHAIN_MAX) {
                OffsetNumber next_off = ItemPointerGetOffsetNumber(&oldtup.t_data->t_ctid);
                ItemId next_lp = PageGetItemId(page, next_off);
                if (!ItemIdIsNormal(next_lp)) {
                    break;
                }
                oldtup.t_self = oldtup.t_data->t_ctid;
                oldtup.t_data = (HeapTupleHeader)PageGetItem(page, next_lp);
                oldtup.t_len = ItemIdGetLength(next_lp);
                HeapTupleCopyBaseFromPage(&oldtup, page);
            }
            if (oldtup.t_data != nullptr && !oldtup.t_data->is_latest &&
                !ItemPointerEquals(&oldtup.t_self, &oldtup.t_data->t_ctid) &&
                ItemPointerGetBlockNumber(&oldtup.t_data->t_ctid) != tip_blk) {
                /* tip 已跨页：丢掉新页，整链重追后再决定同页/换页 */
                if (BufferIsValid(newbuf) && newbuf != buffer) {
                    UnlockReleaseBuffer(newbuf);
                    newbuf = InvalidBuffer;
                }
                if (BufferIsValid(vmbuffer_new)) {
                    ReleaseBuffer(vmbuffer_new);
                    vmbuffer_new = InvalidBuffer;
                }
                Buffer tip_buf = buffer;
                if (!EpochHeapLockFollowToTip(relation, &oldtup.t_self, &tip_buf, &oldtup)) {
                    EpochApplyBufLockUnlock(tip_buf);
                    ReleaseBuffer(tip_buf);
                    if (BufferIsValid(vmbuffer)) {
                        ReleaseBuffer(vmbuffer);
                    }
                    bms_free(hot_attrs);
                    bms_free(key_attrs);
                    bms_free(id_attrs);
                    return TM_Deleted;
                }
                if (tip_buf != buffer) {
                    if (BufferIsValid(vmbuffer)) {
                        ReleaseBuffer(vmbuffer);
                        vmbuffer = InvalidBuffer;
                    }
                    buffer = tip_buf;
                    block = BufferGetBlockNumber(buffer);
                    page = BufferGetPage(buffer);
                    if (PageIsAllVisible(page)) {
                        visibilitymap_pin(relation, block, &vmbuffer);
                    }
                } else {
                    page = BufferGetPage(buffer);
                }
                pagefree = PageGetHeapFreeSpace(page);
                if (new_tup_size <= pagefree) {
                    already_marked = false;
                    newbuf = buffer;
                    heaptup = newtup;
                } else {
                    already_marked = true;
                    EpochApplyBufLockMarkExclReleased();
                    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                    /* GetBufferForTuple 已锁住 buffer，勿再 EXCLUSIVE */
                    instr_time gb2_t0;
                    INSTR_TIME_SET_CURRENT(gb2_t0);
                    newbuf = RelationGetBufferForTuple(relation, heaptup->t_len, buffer, options, NULL,
                        &vmbuffer_new, &vmbuffer, rel_end_block);
                    instr_time gb2_t1;
                    INSTR_TIME_SET_CURRENT(gb2_t1);
                    INSTR_TIME_SUBTRACT(gb2_t1, gb2_t0);
                    EpochApplyBufLockAddGetBufUs(static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(gb2_t1)));
                    EpochApplyBufLockMarkExclHeld();
                    page = BufferGetPage(buffer);
                }
            }
        }
    }
    else
    {
        already_marked = false;
        newbuf = buffer;
        heaptup = newtup;
    }

    if (newbuf == buffer)
    {
        if (satisfies_hot)
        {
            use_hot_update = true;
        }
    }
    else
    {
        PageSetFull(page);
        /*
         * CSN + index cols unchanged: skip unique-index insert (see below:
         * !use_hot_update && !satisfies_hot) and mark new version HEAP_ONLY
         * for prune. Must NOT set HOT_UPDATED — that requires same-page t_ctid;
         * heap_hot_search_buffer Asserts on cross-page HOT (postmaster TRAP).
         */
        if (O_HeapUseCsnEpochSemantics() && satisfies_hot) {
            csn_cross_page_heap_only = true;
        }
    }

    //    bool keyChanged = XLogLogicalInfoActive() ? true : !satisfies_id;
    //    old_key_tuple = ExtractReplicaIdentity(relation, &oldtup, keyChanged, &old_key_copied, &relreplident);

    newpage = BufferGetPage(newbuf);

    if (newbuf != buffer)
    {
        (void)heap_page_prepare_for_xid(relation, newbuf, xid, false);
    }

    HeapTupleCopyBaseFromPage(heaptup, newpage);
    HeapTupleHeaderSetXmax(newpage, heaptup->t_data, xmax_new_tuple);

    instr_time crit_t0;
    INSTR_TIME_SET_CURRENT(crit_t0);
    START_CRIT_SECTION();

    if (!(options & HEAP_INSERT_SKIP_FSM))
    {
        PageSetPrunable(page, xid);
    }

    if (use_hot_update)
    {
        /* Same-page only — guaranteed by newbuf == buffer above. */
        HeapTupleSetHotUpdated(&oldtup);
        HeapTupleSetHeapOnly(heaptup);
        HeapTupleSetHeapOnly(newtup);
    }
    else if (csn_cross_page_heap_only)
    {
        HeapTupleClearHotUpdated(&oldtup);
        HeapTupleSetHeapOnly(heaptup);
        HeapTupleSetHeapOnly(newtup);
    }
    else
    {
        HeapTupleClearHotUpdated(&oldtup);
        HeapTupleClearHeapOnly(heaptup);
        HeapTupleClearHeapOnly(newtup);
    }

    RelationPutHeapTuple(relation, newbuf, heaptup, xid); /* insert new tuple */

    if (!already_marked)
    {
        oldtup.t_data->t_infomask &= ~HEAP_XMAX_BITS;
        oldtup.t_data->t_infomask2 &= ~(HEAP_XMAX_LOCK_ONLY | HEAP_KEYS_UPDATED);
        oldtup.t_data->t_infomask |= infomask_old_tuple;
        oldtup.t_data->t_infomask2 |= infomask2_old_tuple;
        HeapTupleHeaderSetXmax(page, oldtup.t_data, xmax_old_tuple);
        HeapTupleHeaderSetCmax(oldtup.t_data, cid, is_combo);
    }

    if (O_HeapUseCsnEpochSemantics()) {
        /*
         * 旧版本保留原 m_rowCSN（MVCC 链），仅标记非 latest 并链接新版本（正向）。
         * backend 按哨兵 TID + pinned CSN / read_frontier 仍可读旧版本。
         * 索引列未变时不更新索引 TID（HOT），find 从链头正向追到 is_latest。
         */
        oldtup.t_data->is_latest = false;
        oldtup.t_data->t_ctid = heaptup->t_self;
    } else {
        oldtup.t_data->m_rowCSN = csn;
        oldtup.t_data->is_latest = false;
        oldtup.t_data->t_ctid = heaptup->t_self;
    }

    if (PageIsAllVisible(BufferGetPage(buffer)))
    {
        all_visible_cleared = true;
        PageClearAllVisible(BufferGetPage(buffer));
        visibilitymap_clear(relation, BufferGetBlockNumber(buffer), vmbuffer);
    }
    if (newbuf != buffer && PageIsAllVisible(BufferGetPage(newbuf)))
    {
        all_visible_cleared_new = true;
        PageClearAllVisible(BufferGetPage(newbuf));
        visibilitymap_clear(relation, BufferGetBlockNumber(newbuf), vmbuffer_new);
    }

    if (newbuf != buffer)
    {
        MarkBufferDirty(newbuf);
    }
    MarkBufferDirty(buffer);

    /* XLOG stuff */
    if (RelationNeedsWAL(relation))
    {
        XLogRecPtr recptr;
        /*
         * For logical decoding we need combocids to properly decode the
         * catalog.
         */
        if (RelationIsAccessibleInLogicalDecoding(relation))
        {
            (void)log_heap_new_cid(relation, &oldtup);
            (void)log_heap_new_cid(relation, heaptup);
        }

        recptr = log_heap_update(relation, buffer, &oldtup, newbuf, heaptup, old_key_tuple, all_visible_cleared,
                                 all_visible_cleared_new, relreplident);

        if (newbuf != buffer)
        {
            PageSetLSN(BufferGetPage(newbuf), recptr);
        }
        PageSetLSN(BufferGetPage(buffer), recptr);
    }

    END_CRIT_SECTION();
    {
        instr_time crit_t1;
        INSTR_TIME_SET_CURRENT(crit_t1);
        INSTR_TIME_SUBTRACT(crit_t1, crit_t0);
        EpochApplyBufLockAddCritUs(static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(crit_t1)));
    }

    prune_hint = heaptup->t_self;

    /* 先放新页锁（若跨页），hold 计时仍挂在 tip/旧页上；最后 Unlock tip 结束 hold */
    if (newbuf != buffer)
    {
        LockBuffer(newbuf, BUFFER_LOCK_UNLOCK);
    }
    EpochApplyBufLockUnlock(buffer);

    if (newbuf != buffer)
    {
        ReleaseBuffer(newbuf);
    }
    ReleaseBuffer(buffer);
    if (BufferIsValid(vmbuffer_new))
    {
        ReleaseBuffer(vmbuffer_new);
    }
    if (BufferIsValid(vmbuffer))
    {
        ReleaseBuffer(vmbuffer);
    }

    /* buffer 锁外机会性回收 */
    if (EpochHeapVersionChainEnabled(relation) && ItemPointerIsValid(&prune_hint)) {
        EpochHeapOpportunisticPrune(relation, ItemPointerGetBlockNumber(&prune_hint), &prune_hint);
        if (block != ItemPointerGetBlockNumber(&prune_hint)) {
            EpochHeapOpportunisticPrune(relation, block, nullptr);
        }
    }

    /*
     * 索引列未变时旧索引项仍指向根 TID，经 t_ctid 正向链可达新版本（同 PG HOT 语义），
     * 跨页 UPDATE 也不应 insert 新索引项。
     */
    if (!use_hot_update && !satisfies_hot)
    {
        if (estate != nullptr && slot != nullptr)
        {
            ResultRelInfo *result_rel_info = estate->es_result_relation_info;
            if (result_rel_info != nullptr && result_rel_info->ri_NumIndices > 0)
            {
                O_ExecInsertIndexTuples(slot, &(heaptup->t_self), estate, NULL);
            }
        }
        else
        {
            O_InsertIndexTuplesForHeap(relation, heaptup, &(heaptup->t_self));
        }
    }

    if (heaptup != newtup)
    {
        newtup->t_self = heaptup->t_self;
        heap_freetuple(heaptup);
    }

    bms_free(hot_attrs);
    bms_free(key_attrs);
    bms_free(id_attrs);
    //
    //    if (old_key_tuple != NULL && old_key_copied) {
    //        heap_freetuple(old_key_tuple);
    //    }

    return TM_Ok;
}

// 暂时可以忽略
TM_Result O_heap_update2(Relation relation, ItemPointer otid, HeapTuple newtup,
                         CommandId cid, uint64_t csn)
{
    Page page, newpage;
    BlockNumber block;
    Buffer buffer = InvalidBuffer;
    ItemId lp;
    HeapTupleData oldtup;
    TM_Result result;
    TransactionId xid = GetCurrentTransactionId();

    TransactionId xmax_new_tuple;
    TransactionId xmax_old_tuple;
    uint16 infomask_old_tuple;
    uint16 infomask2_old_tuple;
    uint16 infomask_new_tuple;
    uint16 infomask2_new_tuple;
    Buffer newbuf = InvalidBuffer;

    bool already_marked = false;
    bool is_combo = false;

    Size new_tup_size, pagefree;
    int options = 0;

    bool all_visible_cleared = false;
    bool all_visible_cleared_new = false;

    HeapTuple heaptup = newtup;

    HeapTuple old_key_tuple = NULL;
    bool old_key_copied = false;
    char relreplident;

    bool satisfies_id = false;

    block = ItemPointerGetBlockNumber(otid);
    buffer = ReadBuffer(relation, block);
    page = BufferGetPage(buffer);

    Buffer vmbuffer = InvalidBuffer;
    Buffer vmbuffer_new = InvalidBuffer;
    BlockNumber rel_end_block = InvalidBlockNumber;

    if (PageIsAllVisible(page))
    {
        visibilitymap_pin(relation, block, &vmbuffer);
    }

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(otid));
    oldtup.t_data = (HeapTupleHeader)PageGetItem(page, lp);
    oldtup.t_len = ItemIdGetLength(lp);
    oldtup.t_self = *otid;
    oldtup.t_tableOid = RelationGetRelid(relation);
    oldtup.t_bucketId = RelationGetBktid(relation);

    HeapTupleCopyBaseFromPage(&oldtup, BufferGetPage(buffer));

    if (O_HeapUseCsnEpochSemantics()) {
        xmax_old_tuple = InvalidTransactionId;
        infomask_old_tuple = HEAP_XMAX_INVALID;
    } else {
        xmax_old_tuple = xid;
        infomask_old_tuple = 0;
    }
    infomask2_old_tuple = 0;

    heap_page_prepare_for_xid(relation, buffer, xid, false);
    if (TransactionIdIsNormal(xmax_old_tuple))
    {
        heap_page_prepare_for_xid(relation, buffer, xmax_old_tuple,
                                  (infomask_old_tuple & HEAP_XMAX_IS_MULTI) ? true : false);
    }

    HeapTupleHeaderAdjustCmax(oldtup.t_data, &cid, &is_combo, buffer);

    already_marked = false;
    newbuf = buffer;

    bool keyChanged = XLogLogicalInfoActive() ? true : !satisfies_id;
    old_key_tuple = ExtractReplicaIdentity(relation, &oldtup, keyChanged, &old_key_copied, &relreplident);

    newpage = BufferGetPage(newbuf);

    if (newbuf != buffer)
    {
        (void)heap_page_prepare_for_xid(relation, newbuf, xid, false);
    }

    PageSetPrunable(page, xid);

    HeapTupleClearHotUpdated(&oldtup);

    if (!already_marked)
    {
        oldtup.t_data->t_infomask &= ~HEAP_XMAX_BITS;
        oldtup.t_data->t_infomask2 &= ~(HEAP_XMAX_LOCK_ONLY | HEAP_KEYS_UPDATED);
        oldtup.t_data->t_infomask |= infomask_old_tuple;
        oldtup.t_data->t_infomask2 |= infomask2_old_tuple;
        HeapTupleHeaderSetXmax(page, oldtup.t_data, xmax_old_tuple);
        HeapTupleHeaderSetCmax(oldtup.t_data, cid, is_combo);

        oldtup.t_data->m_rowCSN = csn;
    }

    if (PageIsAllVisible(BufferGetPage(buffer)))
    {
        all_visible_cleared = true;
        PageClearAllVisible(BufferGetPage(buffer));
        visibilitymap_clear(relation, BufferGetBlockNumber(buffer), vmbuffer);
    }
    if (newbuf != buffer && PageIsAllVisible(BufferGetPage(newbuf)))
    {
        all_visible_cleared_new = true;
        PageClearAllVisible(BufferGetPage(newbuf));
        visibilitymap_clear(relation, BufferGetBlockNumber(newbuf), vmbuffer_new);
    }

    if (newbuf != buffer)
    {
        MarkBufferDirty(newbuf);
    }
    MarkBufferDirty(buffer);

    //    /* XLOG stuff */
    //    if (RelationNeedsWAL(relation)) {
    //        XLogRecPtr recptr;
    //        /*
    //         * For logical decoding we need combocids to properly decode the
    //         * catalog.
    //         */
    //        if (RelationIsAccessibleInLogicalDecoding(relation)) {
    //            (void)log_heap_new_cid(relation, &oldtup);
    //            (void)log_heap_new_cid(relation, heaptup);
    //        }
    //
    //        recptr = log_heap_update(relation, buffer, &oldtup, newbuf, heaptup, old_key_tuple, all_visible_cleared,
    //                                 all_visible_cleared_new, relreplident);
    //
    //        if (newbuf != buffer) {
    //            PageSetLSN(BufferGetPage(newbuf), recptr);
    //        }
    //        PageSetLSN(BufferGetPage(buffer), recptr);
    //    }

    if (newbuf != buffer)
    {
        LockBuffer(newbuf, BUFFER_LOCK_UNLOCK);
    }
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    if (newbuf != buffer)
    {
        ReleaseBuffer(newbuf);
    }
    ReleaseBuffer(buffer);
    if (BufferIsValid(vmbuffer_new))
    {
        ReleaseBuffer(vmbuffer_new);
    }
    if (BufferIsValid(vmbuffer))
    {
        ReleaseBuffer(vmbuffer);
    }

    if (old_key_tuple != NULL && old_key_copied)
    {
        heap_freetuple(old_key_tuple);
    }

    return TM_Ok;
}

//======================================================================

static bool heap_tuple_attr_equals(TupleDesc tupdesc, int attrnum, HeapTuple tup1, HeapTuple tup2, char *page)
{
    Datum value1, value2;
    bool isnull1 = false;
    bool isnull2 = false;
    Form_pg_attribute att;

    /*
     * If it's a whole-tuple reference, say "not equal".  It's not really
     * worth supporting this case, since it could only succeed after a no-op
     * update, which is hardly a case worth optimizing for.
     */
    if (attrnum == 0)
    {
        return false;
    }

    /*
     * Likewise, automatically say "not equal" for any system attribute other
     * than OID and tableOID; we cannot expect these to be consistent in a HOT
     * chain, or even to be set correctly yet in the new tuple.
     */
    if (attrnum < 0)
    {
        if (attrnum != ObjectIdAttributeNumber &&
#ifdef PGXC
            attrnum != XC_NodeIdAttributeNumber && attrnum != BucketIdAttributeNumber &&
            attrnum != UidAttributeNumber && /* may should remove this attr */
#endif
            attrnum != TableOidAttributeNumber)
        {
            return false;
        }
    }

    /*
     * Extract the corresponding values.  XXX this is pretty inefficient if
     * there are many indexed columns.	Should HeapSatisfiesHOTUpdate do a
     * single heap_deform_tuple call on each tuple, instead?  But that doesn't
     * work for system columns ...
     */
    if (HEAP_TUPLE_IS_COMPRESSED(tup1->t_data))
    {
        value1 = heap_getattr_with_dict(tup1, attrnum, tupdesc, &isnull1, (char *)getPageDict(page));
    }
    else
    {
        value1 = heap_getattr(tup1, attrnum, tupdesc, &isnull1);
    }
    Assert(!HEAP_TUPLE_IS_COMPRESSED(tup2->t_data));
    value2 = heap_getattr(tup2, attrnum, tupdesc, &isnull2);

    /*
     * If one value is NULL and other is not, then they are certainly not
     * equal
     */
    if (isnull1 != isnull2)
    {
        return false;
    }

    /*
     * If both are NULL, they can be considered equal.
     */
    if (isnull1)
    {
        return true;
    }

    /*
     * We do simple binary comparison of the two datums.  This may be overly
     * strict because there can be multiple binary representations for the
     * same logical value.	But we should be OK as long as there are no false
     * positives.  Using a type-specific equality operator is messy because
     * there could be multiple notions of equality in different operator
     * classes; furthermore, we cannot safely invoke user-defined functions
     * while holding exclusive buffer lock.
     */
    if (attrnum <= 0)
    {
        /* The only allowed system columns are OIDs, so do this */
        return (DatumGetObjectId(value1) == DatumGetObjectId(value2));
    }
    else
    {
        Assert(attrnum <= tupdesc->natts);
        att = &tupdesc->attrs[attrnum - 1];
        return datumIsEqual(value1, value2, att->attbyval, att->attlen);
    }
}

static void HeapSatisfiesHOTUpdate(Relation relation, Bitmapset *hot_attrs, Bitmapset *key_attrs, Bitmapset *id_attrs,
                                   bool *satisfies_hot, bool *satisfies_key, bool *satisfies_id, HeapTuple oldtup, HeapTuple newtup, char *page)
{
    int next_hot_attnum;
    int next_key_attum;
    int next_id_attnum;
    bool hot_result = true;
    bool key_result = true;
    bool id_result = true;

    /*
     * If one of these sets contains no remaining bits, bms_first_member will
     * return -1, and after adding FirstLowInvalidHeapAttributeNumber (which
     * is negative!)  we'll get an attribute number that can't possibly be
     * real, and thus won't match any actual attribute number.
     */
    next_hot_attnum = bms_first_member(hot_attrs);
    next_hot_attnum += FirstLowInvalidHeapAttributeNumber;
    next_key_attum = bms_first_member(key_attrs);
    next_key_attum += FirstLowInvalidHeapAttributeNumber;
    next_id_attnum = bms_first_member(id_attrs);
    next_id_attnum += FirstLowInvalidHeapAttributeNumber;

    for (;;)
    {
        bool changed = false;
        int check_now;
        /*
         * Since the HOT attributes are a superset of the key attributes and
         * the key attributes are a superset of the id attributes, this logic
         * is guaranteed to identify the next column that needs to be
         * checked.
         */
        if (hot_result && next_hot_attnum > FirstLowInvalidHeapAttributeNumber)
        {
            check_now = next_hot_attnum;
        }
        else if (key_result && next_key_attum > FirstLowInvalidHeapAttributeNumber)
        {
            check_now = next_key_attum;
        }
        else if (id_result && next_id_attnum > FirstLowInvalidHeapAttributeNumber)
        {
            check_now = next_id_attnum;
        }
        else
        {
            break;
        }

        /* See whether it changed. */
        changed = !heap_tuple_attr_equals(RelationGetDescr(relation), check_now, oldtup, newtup, page);
        if (changed)
        {
            if (check_now == next_hot_attnum)
            {
                hot_result = false;
            }
            if (check_now == next_key_attum)
            {
                key_result = false;
            }
            if (check_now == next_id_attnum)
            {
                id_result = false;
            }

            /* if all are false now, we can stop checking */
            if (!hot_result && !key_result && !id_result)
            {
                break;
            }
        }

        /*
         * Advance the next attribute numbers for the sets that contain
         * the attribute we just checked.  As we work our way through the
         * columns, the next_attnum values will rise; but when each set
         * becomes empty, bms_first_member() will return -1 and the attribute
         * number will end up with a value less than
         * FirstLowInvalidHeapAttributeNumber.
         */
        if (hot_result && check_now == next_hot_attnum)
        {
            next_hot_attnum = bms_first_member(hot_attrs);
            next_hot_attnum += FirstLowInvalidHeapAttributeNumber;
        }
        if (key_result && check_now == next_key_attum)
        {
            next_key_attum = bms_first_member(key_attrs);
            next_key_attum += FirstLowInvalidHeapAttributeNumber;
        }
        if (id_result && check_now == next_id_attnum)
        {
            next_id_attnum = bms_first_member(id_attrs);
            next_id_attnum += FirstLowInvalidHeapAttributeNumber;
        }
    }
    *satisfies_hot = hot_result;
    *satisfies_key = key_result;
    *satisfies_id = id_result;
}

static XLogRecPtr log_heap_new_cid(Relation relation, HeapTuple tup)
{
    xl_heap_new_cid xlrec;

    XLogRecPtr recptr;
    HeapTupleHeader hdr = tup->t_data;

    Assert(ItemPointerIsValid(&tup->t_self));
    Assert(tup->t_tableOid != InvalidOid);

    xlrec.top_xid = GetTopTransactionId();
    xlrec.target_node.dbNode = relation->rd_node.dbNode;
    xlrec.target_node.relNode = relation->rd_node.relNode;
    xlrec.target_node.spcNode = relation->rd_node.spcNode;
    xlrec.target_tid = tup->t_self;

    /*
     * If the tuple got inserted & deleted in the same TX we definitely have a
     * combocid, set cmin and cmax.
     */
    if (hdr->t_infomask & HEAP_COMBOCID)
    {
        Assert(!(hdr->t_infomask & HEAP_XMAX_INVALID));
        Assert(!(hdr->t_infomask & HEAP_XMIN_INVALID));
        xlrec.cmin = HeapTupleGetCmin(tup);
        xlrec.cmax = HeapTupleGetCmax(tup);
        xlrec.combocid = HeapTupleHeaderGetRawCommandId(hdr);
    }
    /* No combocid, so only cmin or cmax can be set by this TX */
    else
    {
        /*
         * Tuple inserted.
         *
         * We need to check for LOCK ONLY because multixacts might be
         * transferred to the new tuple in case of FOR KEY SHARE updates in
         * which case there will be an xmax, although the tuple just got
         * inserted.
         */
        if ((hdr->t_infomask & HEAP_XMAX_INVALID) || HEAP_XMAX_IS_LOCKED_ONLY(hdr->t_infomask, hdr->t_infomask2))
        {
            xlrec.cmin = HeapTupleHeaderGetRawCommandId(hdr);
            xlrec.cmax = InvalidCommandId;
        }
        else
        {
            /* Tuple from a different tx updated or deleted. */
            xlrec.cmin = InvalidCommandId;
            xlrec.cmax = HeapTupleHeaderGetRawCommandId(hdr);
        }
        xlrec.combocid = InvalidCommandId;
    }
    recptr = log_heap_new_cid_insert(&xlrec, relation->rd_node.bucketNode);
    return recptr;
}

static XLogRecPtr log_heap_new_cid_insert(xl_heap_new_cid *xlrec, int bucketid)
{
    XLogRecPtr recptr;
    /*
     * Note that we don't need to register the buffer here, because this
     * operation does not modify the page. The insert/update/delete that
     * called us certainly did, but that's WAL-logged separately.
     */
    XLogBeginInsert();
    XLogRegisterData((char *)xlrec, SizeOfHeapNewCid);
    /* will be looked at irrespective of origin */
    recptr = XLogInsert(RM_HEAP3_ID, XLOG_HEAP3_NEW_CID, bucketid);
    return recptr;
}

static HeapTuple ExtractReplicaIdentity(Relation relation, HeapTuple tp, bool key_changed, bool *copy,
                                        char *relreplident)
{
    TupleDesc desc = RelationGetDescr(relation);
    Oid replidindex;
    Relation idx_rel;
    HeapTuple key_tuple = NULL;
    bool nulls[MaxHeapAttributeNumber];
    Datum values[MaxHeapAttributeNumber];
    int natt;
    errno_t rc = 0;
    *copy = false;
    *relreplident = REPLICA_IDENTITY_NOTHING;

    if (!RelationIsLogicallyLogged(relation))
    {
        return NULL;
    }

    bool is_null = true;
    Relation rel = heap_open(RelationRelationId, AccessShareLock);
    Oid relid = RelationIsPartition(relation) ? relation->parentId : relation->rd_id;
    Oid tmpRelid = partid_get_parentid(relid);
    if (OidIsValid(tmpRelid))
    {
        relid = tmpRelid;
    }
    HeapTuple tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
    {
        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                        errmsg("pg_class entry for relid %u vanished during ExtractReplicaIdentity", relid)));
    }
    Datum replident = heap_getattr(tuple, Anum_pg_class_relreplident, RelationGetDescr(rel), &is_null);
    heap_close(rel, AccessShareLock);
    heap_freetuple(tuple);

    if (is_null)
    {
        *relreplident = REPLICA_IDENTITY_NOTHING;
    }
    else
    {
        *relreplident = CharGetDatum(replident);
    }

    if (*relreplident == REPLICA_IDENTITY_NOTHING)
    {
        return NULL;
    }

    if (*relreplident == REPLICA_IDENTITY_FULL)
    {
        /*
         * When logging the entire old tuple, it very well could contain
         * toasted columns. If so, force them to be inlined.
         */
        if (HeapTupleHasExternal(tp))
        {
            *copy = true;
            tp = toast_flatten_tuple(tp, RelationGetDescr(relation));
        }
        return tp;
    }

    /* if the key hasn't changed and we're only logging the key, we're done */
    if (!key_changed)
    {
        return NULL;
    }

    /* find the replica identity index */
    replidindex = RelationGetReplicaIndex(relation);
    if (!OidIsValid(replidindex))
    {
        ereport(DEBUG4, (errmsg("could not find configured replica identity for table \"%s\"",
                                RelationGetRelationName(relation))));
        return NULL;
    }

    idx_rel = RelationIdGetRelation(replidindex);

    /* deform tuple, so we have fast access to columns */
    heap_deform_tuple(tp, desc, values, nulls);

    /* set all columns to NULL, regardless of whether they actually are */
    rc = memset_s(nulls, sizeof(nulls), 1, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    /*
     * Now set all columns contained in the index to NOT NULL, they cannot
     * currently be NULL.
     */
    for (natt = 0; natt < IndexRelationGetNumberOfKeyAttributes(idx_rel); natt++)
    {
        int attno = idx_rel->rd_index->indkey.values[natt];

        if (attno < 0)
        {
            /*
             * The OID column can appear in an index definition, but that's
             * OK, because we always copy the OID if present (see below).
             * Other system columns may not.
             */
            if (attno == ObjectIdAttributeNumber)
            {
                continue;
            }
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("system column in index")));
        }
        nulls[attno - 1] = false;
    }

    key_tuple = heap_form_tuple(desc, values, nulls);
    *copy = true;
    RelationClose(idx_rel);

    /*
     * Always copy oids if the table has them, even if not included in the
     * index. The space in the logged tuple is used anyway, so there's little
     * point in not including the information.
     */
    if (relation->rd_rel->relhasoids)
    {
        HeapTupleSetOid(key_tuple, HeapTupleGetOid(tp));
    }

    /*
     * If the tuple, which by here only contains indexed columns, still has
     * toasted columns, force them to be inlined. This is somewhat unlikely
     * since there's limits on the size of indexed columns, so we don't
     * duplicate toast_flatten_tuple()s functionality in the above loop over
     * the indexed columns, even if it would be more efficient.
     */
    if (HeapTupleHasExternal(key_tuple))
    {
        HeapTuple oldtup = key_tuple;

        key_tuple = toast_flatten_tuple(oldtup, RelationGetDescr(relation));
        heap_freetuple(oldtup);
    }

    return key_tuple;
}

static uint8 ComputeInfobits(uint16 infomask, uint16 infomask2)
{
    return ((infomask & HEAP_XMAX_IS_MULTI) != 0 ? XLHL_XMAX_IS_MULTI : 0) |
           (HEAP_XMAX_IS_LOCKED_ONLY(infomask, infomask2) ? XLHL_XMAX_LOCK_ONLY : 0) |
           (HEAP_XMAX_IS_EXCL_LOCKED(infomask) ? XLHL_XMAX_EXCL_LOCK : 0) |
           /* note we ignore HEAP_XMAX_SHR_LOCK here */
           (HEAP_XMAX_IS_KEYSHR_LOCKED(infomask) ? XLHL_XMAX_KEYSHR_LOCK : 0) |
           ((infomask2 & HEAP_KEYS_UPDATED) != 0 ? XLHL_KEYS_UPDATED : 0);
}

static XLogRecPtr log_heap_update(Relation reln, Buffer oldbuf, HeapTuple oldtup, Buffer newbuf, HeapTuple newtup,
                                  HeapTuple old_key_tuple, bool all_visible_cleared, bool new_all_visible_cleared,
                                  char relreplident)
{
    xl_heap_update xlrec;
    xl_heap_header xlhdr;
    xl_heap_header xlhdr_idx;
    uint8 info;
    XLogRecPtr recptr;
    Page page = BufferGetPage(newbuf);
    bool need_tuple_data = RelationIsLogicallyLogged(reln);
    int bufflags;
    TdeInfo tdeinfo = {0};
    OffsetNumber maxoff;
    bool useOldXlog;

    /* Caller should not call me on a non-WAL-logged relation */
    Assert(RelationNeedsWAL(reln));

    if (HeapTupleIsHeapOnly(newtup))
    {
        info = XLOG_HEAP_HOT_UPDATE;
    }
    else
    {
        info = XLOG_HEAP_UPDATE;
    }

    XLogBeginInsert();
    maxoff = PageGetMaxOffsetNumber(page);
    /*
     * If new tuple is the single and first tuple on page...
     * If page is already compressed, should not init page,
     * or lead to inconsistency.
     */
    if (ItemPointerGetOffsetNumber(&(newtup->t_self)) == FirstOffsetNumber && maxoff == FirstOffsetNumber &&
        !PageIsCompressed(page))
    {
        info |= XLOG_HEAP_INIT_PAGE;
        bufflags = REGBUF_STANDARD | REGBUF_WILL_INIT;
    }
    else
        bufflags = REGBUF_STANDARD;

    /* Prepare WAL data */
    xlrec.old_offnum = ItemPointerGetOffsetNumber(&oldtup->t_self);
    xlrec.new_offnum = ItemPointerGetOffsetNumber(&newtup->t_self);
    xlrec.flags = 0;
    xlrec.old_xmax = HeapTupleGetRawXmax(oldtup);
    xlrec.new_xmax = HeapTupleGetRawXmax(newtup);
    xlrec.old_infobits_set = ComputeInfobits(oldtup->t_data->t_infomask, oldtup->t_data->t_infomask2);
    useOldXlog = t_thrd.proc->workingVersionNum < ENHANCED_TUPLE_LOCK_VERSION_NUM ||
                 !(xlrec.old_infobits_set & XLHL_XMAX_IS_MULTI);
#ifdef ENABLE_MULTIPLE_NODES
    useOldXlog = true;
#endif

    if (all_visible_cleared)
    {
        xlrec.flags |= XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED;
    }
    if (new_all_visible_cleared)
    {
        xlrec.flags |= XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED;
    }
    if (need_tuple_data)
    {
        xlrec.flags |= XLH_UPDATE_CONTAINS_NEW_TUPLE;
        if (old_key_tuple)
        {
            if (relreplident == REPLICA_IDENTITY_FULL)
                xlrec.flags |= XLH_UPDATE_CONTAINS_OLD_TUPLE;
            else
                xlrec.flags |= XLH_UPDATE_CONTAINS_OLD_KEY;
        }
    }
    if (need_tuple_data)
    {
        bufflags |= REGBUF_KEEP_DATA;
    }

    xlhdr.t_infomask2 = newtup->t_data->t_infomask2;
    xlhdr.t_infomask = newtup->t_data->t_infomask;
    xlhdr.t_hoff = newtup->t_data->t_hoff;

    /*
     * For TDE relation, we intend to put the TdeoInfo(relevant to cipher, cmkid, etc.) after the pd_xid_base.
     */
    if (RelationisEncryptEnable(reln))
    {
        GetTdeInfoFromRel(reln, &tdeinfo);
    }

    /*
     * As with insert records, we need not store the rdata[2] segment
     * if we decide to store the whole buffer instead unless we're
     * doing logical decoding.
     */
    XLogRegisterBuffer(0, newbuf, bufflags, &tdeinfo);
    XLogRegisterBufData(0, (char *)&xlhdr, SizeOfHeapHeader);

    /* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
    XLogRegisterBufData(0, (char *)newtup->t_data + offsetof(HeapTupleHeaderData, t_bits),
                        newtup->t_len - offsetof(HeapTupleHeaderData, t_bits));

    if (oldbuf != newbuf)
    {
        XLogRegisterBuffer(1, oldbuf, REGBUF_STANDARD, &tdeinfo);
    }

    if (info & XLOG_HEAP_INIT_PAGE)
    {
        XLogRegisterData((char *)&((HeapPageHeader)(page))->pd_xid_base, sizeof(TransactionId));
    }

    XLogRegisterData((char *)&xlrec, useOldXlog ? SizeOfOldHeapUpdate : SizeOfHeapUpdate);
    CommitSeqNo curCSN = InvalidCommitSeqNo;
    LogCSN(&curCSN);

    /* We need to log a tuple identity */
    if (need_tuple_data && old_key_tuple)
    {
        /* don't really need this, but its more comfy to decode */
        xlhdr_idx.t_infomask2 = old_key_tuple->t_data->t_infomask2;
        xlhdr_idx.t_infomask = old_key_tuple->t_data->t_infomask;
        xlhdr_idx.t_hoff = old_key_tuple->t_data->t_hoff;

        XLogRegisterData((char *)&xlhdr_idx, SizeOfHeapHeader);

        /* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
        XLogRegisterData((char *)old_key_tuple->t_data + offsetof(HeapTupleHeaderData, t_bits),
                         old_key_tuple->t_len - offsetof(HeapTupleHeaderData, t_bits));
    }

    /* filtering by origin on a row level is much more efficient */
    XLogIncludeOrigin();

    info |= useOldXlog ? 0 : XLOG_TUPLE_LOCK_UPGRADE_FLAG;

    recptr = XLogInsert(RM_HEAP_ID, info);
    ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                     errmsg("[REDO_LOG_TRACE]log_heap_update: fromBlkNum:%u,fromOffsetNum:%hu,"
                            "newBlkNum:%u,newOffsetNum:%hu,"
                            "t_infomask2:%hu,t_infomask:%hu,t_hoff:%hhu,flags:%hhu,bufflags:%d,newLen:%u",
                            ItemPointerGetBlockNumber(&oldtup->t_self), ItemPointerGetOffsetNumber(&oldtup->t_self),
                            ItemPointerGetBlockNumber(&newtup->t_self), ItemPointerGetOffsetNumber(&newtup->t_self),
                            xlhdr.t_infomask2, xlhdr.t_infomask, xlhdr.t_hoff, xlrec.flags, bufflags, newtup->t_len)));
    return recptr;
}

static Oid occ_heap_insert(Relation relation, HeapTuple tup, CommandId cid, int options, BulkInsertState bistate, bool istoast)
{
    TransactionId xid = GetCurrentTransactionId();
    HeapTuple heaptup;
    Buffer buffer;
    Buffer vmbuffer = InvalidBuffer;
    bool all_visible_cleared = false;
    BlockNumber rel_end_block = InvalidBlockNumber;
#ifdef USE_ASSERT_CHECKING
    if (tup != NULL)
    {
        Assert(TUPLE_IS_HEAP_TUPLE(tup));
    }
#endif
    /*
     * Fill in tuple header fields, assign an OID, and toast the tuple if
     * necessary.
     *
     * Note: below this point, heaptup is the data we actually intend to store
     * into the relation; tup is the caller's original untoasted data.
     */
    heaptup = heap_prepare_insert(relation, tup, cid, options);
    /*
     * We're about to do the actual insert -- but check for conflict first, to
     * avoid possibly having to roll back work we've just done.
     *
     * For a heap insert, we only need to check for table-level SSI locks. Our
     * new tuple can't possibly conflict with existing tuple locks, and heap
     * page locks are only consolidated versions of tuple locks; they do not
     * lock "gaps" as index page locks do.	So we don't need to identify a
     * buffer before making the call.
     */
//    CheckForSerializableConflictIn(relation, NULL, InvalidBuffer);
#ifdef ENABLE_MULTIPLE_NODES
    if (RelationInClusterResizing(relation) && !RelationInClusterResizingReadOnly(relation))
    {
        options |= HEAP_INSERT_SKIP_FSM;
        rel_end_block = RelationGetEndBlock(relation);
    }
#endif
    /*
     * Find buffer to insert this tuple into.  If the page is all visible,
     * this will also pin the requisite visibility map page.
     */
    buffer = RelationGetBufferForTuple(relation, heaptup->t_len, InvalidBuffer, options, bistate, &vmbuffer, NULL,
                                       rel_end_block);

    (void)heap_page_prepare_for_xid(relation, buffer, xid, false);
    HeapTupleCopyBaseFromPage(heaptup, BufferGetPage(buffer));

    /* NO EREPORT(ERROR) from here till changes are logged */
    START_CRIT_SECTION();

    RelationPutHeapTuple(relation, buffer, heaptup, xid);

    if (PageIsAllVisible(BufferGetPage(buffer)))
    {
        all_visible_cleared = true;
        PageClearAllVisible(BufferGetPage(buffer));
        visibilitymap_clear(relation, ItemPointerGetBlockNumber(&(heaptup->t_self)), vmbuffer);
    }

    /*
     * XXX Should we set PageSetPrunable on this page ?
     *
     * The inserting transaction may eventually abort thus making this tuple
     * DEAD and hence available for pruning. Though we don't want to optimize
     * for aborts, if no other tuple in this page is UPDATEd/DELETEd, the
     * aborted tuple will never be pruned until next vacuum is triggered.
     *
     * If you do add PageSetPrunable here, add it in heap_xlog_insert too.
     */
    MarkBufferDirty(buffer);

    END_CRIT_SECTION();

    UnlockReleaseBuffer(buffer);
    if (vmbuffer != InvalidBuffer)
    {
        ReleaseBuffer(vmbuffer);
    }

    /*
     * If tuple is cachable, mark it for invalidation from the caches in case
     * we abort.  Note it is OK to do this after releasing the buffer, because
     * the heaptup data structure is all in local memory, not in the shared
     * buffer.
     */
    CacheInvalidateHeapTuple(relation, heaptup, NULL);

    /* Note: speculative insertions are counted too, even if aborted later */
    pgstat_count_heap_insert(relation, 1);

    /*
     * If heaptup is a private copy, release it.  Don't forget to copy t_self
     * back to the caller's image, too.
     */
    if (heaptup != tup)
    {
        tup->t_self = heaptup->t_self;
        heap_freetuple(heaptup);
    }

    return HeapTupleGetOid(tup);
}

static HeapTuple heap_prepare_insert(Relation relation, HeapTuple tup, CommandId cid, int options)
{
    if (relation->rd_rel->relhasoids)
    {
#ifdef NOT_USED
        /* this is redundant with an Assert in HeapTupleSetOid */
        Assert(tup->t_data->t_infomask & HEAP_HASOID);
#endif

        /* For catalogs that do not support DDL, we set new object oids here. */
        if (u_sess->attr.attr_common.IsInplaceUpgrade && OidIsValid(u_sess->upg_cxt.Inplace_upgrade_next_general_oid))
        {
            HeapTupleSetOid(tup, u_sess->upg_cxt.Inplace_upgrade_next_general_oid);
            u_sess->upg_cxt.Inplace_upgrade_next_general_oid = InvalidOid;
        }

        /*
         * If the object id of this tuple has already been assigned, trust the
         * caller.	There are a couple of ways this can happen.  At initial db
         * creation, the backend program sets oids for tuples. When we define
         * an index, we set the oid.  Finally, in the future, we may allow
         * users to set their own object ids in order to support a persistent
         * object store (objects need to contain pointers to one another).
         */
        if (!OidIsValid(HeapTupleGetOid(tup)))
        {
            HeapTupleSetOid(tup, GetNewOid(relation));
        }
    }
#ifdef USE_ASSERT_CHECKING
    else
    {
        /* check there is not space for an OID */
        Assert(!(tup->t_data->t_infomask & HEAP_HASOID));
    }

    if (tup != NULL)
    {
        Assert(TUPLE_IS_HEAP_TUPLE(tup));
    }
#endif
    tup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
    tup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
    tup->t_data->t_infomask |= HEAP_XMAX_INVALID;
    tup->t_data->t_choice.t_heap.t_xmin = InvalidTransactionId;
    if (options & HEAP_INSERT_FROZEN)
    {
        HeapTupleHeaderSetXminFrozen(tup->t_data);
    }

    HeapTupleHeaderSetCmin(tup->t_data, cid);
    HeapTupleSetXmax(tup, 0); /* for cleanliness */
    tup->t_tableOid = RelationGetRelid(relation);
    tup->t_bucketId = RelationGetBktid(relation);
#ifdef ENABLE_MULTIPLE_NODES
    tup->t_xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
    if (RelationIsRedistributeDest(relation))
    {
        HeapTupleHeaderSetRedisColumns(tup->t_data);
    }
#endif
    /*
     * If the new tuple is too big for storage or contains already toasted
     * out-of-line attributes from some other relation, invoke the toaster.
     */
    if (relation->rd_rel->relkind != RELKIND_RELATION && relation->rd_rel->relkind != RELKIND_MATVIEW)
    {
        /* toast table entries should never be recursively toasted */
        Assert(!HeapTupleHasExternal(tup));
        return tup;
    }
    else if (HeapTupleHasExternal(tup) || tup->t_len > TOAST_TUPLE_THRESHOLD)
    {
        return toast_insert_or_update(relation, tup, NULL, options, NULL);
    }
    else
    {
        return tup;
    }
}

//=========================================================
static void O_FormIndexDatumFromHeap(IndexInfo *indexInfo, Relation heapRel, HeapTuple tup,
                                     Datum *values, bool *isnull)
{
    TupleDesc tupdesc = RelationGetDescr(heapRel);
    for (int i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
    {
        int keycol = indexInfo->ii_KeyAttrNumbers[i];
        if (keycol != 0)
        {
            values[i] = heap_getattr(tup, keycol, tupdesc, &isnull[i]);
        }
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("wrong number of index expressions")));
        }
    }
}

void O_InsertIndexTuplesForHeap(Relation heapRel, HeapTuple tup, ItemPointer tupleid)
{
    List *indexoidlist = RelationGetIndexList(heapRel);
    ListCell *l = NULL;

    foreach (l, indexoidlist)
    {
        Oid indexOid = lfirst_oid(l);
        Relation indexDesc = index_open(indexOid, RowExclusiveLock);
        IndexInfo *ii = BuildIndexInfo(indexDesc);

        if (!ii->ii_ReadyForInserts)
        {
            index_close(indexDesc, RowExclusiveLock);
            continue;
        }

        Datum values[INDEX_MAX_KEYS];
        bool isnull[INDEX_MAX_KEYS];
        IndexUniqueCheck checkUnique = UNIQUE_CHECK_NO;

        O_FormIndexDatumFromHeap(ii, heapRel, tup, values, isnull);

        if (u_sess->attr.attr_common.unique_checks && indexDesc->rd_index->indisunique)
        {
            checkUnique = indexDesc->rd_index->indimmediate ? UNIQUE_CHECK_YES : UNIQUE_CHECK_PARTIAL;
        }

        (void)index_insert(indexDesc, values, isnull, tupleid, heapRel, checkUnique);
        index_close(indexDesc, RowExclusiveLock);
    }

    list_free_ext(indexoidlist);
}

void O_FormIndexDatum(IndexInfo *indexInfo, TupleTableSlot *slot, Datum *values, bool *isnull)
{
    int i;
    for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
    {
        int keycol = indexInfo->ii_KeyAttrNumbers[i];
        Datum iDatum;
        bool isNull = false;

        if (keycol != 0)
        {
            /*
             * Plain index column; get the value we need directly from the
             * heap tuple.
             */
            /* Get the Table Accessor Method*/
            iDatum = tableam_tslot_getattr(slot, keycol, &isNull);
        }
        else
        {
            /*
             * Index expression --- need to evaluate it.
             */
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("wrong number of index expressions")));
        }
        values[i] = iDatum;
        isnull[i] = isNull;
    }
}

List *O_ExecInsertIndexTuples(TupleTableSlot *slot, ItemPointer tupleid, EState *estate,
                              Bitmapset *modifiedIdxAttrs)
{
    List *result = NIL;
    ResultRelInfo *resultRelInfo = NULL;
    int numIndices;
    int i;
    int numUnusedIndices;
    int totalIndices;
    Relation heapRelation;
    Relation actualheap;
    RelationPtr relationDescs;
    RelationPtr unusedRelationDescs;
    IndexInfo **indexInfoArray;
    IndexInfo **unusedindexInfoArray;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];

    resultRelInfo = estate->es_result_relation_info;
    numIndices = resultRelInfo->ri_NumIndices;
    relationDescs = resultRelInfo->ri_IndexRelationDescs;
    indexInfoArray = resultRelInfo->ri_IndexRelationInfo;

    numUnusedIndices = resultRelInfo->ri_NumUnusableIndices;
    unusedRelationDescs = resultRelInfo->ri_UnusableIndexRelationDescs;
    unusedindexInfoArray = resultRelInfo->ri_UnusableIndexRelationInfo;

    heapRelation = resultRelInfo->ri_RelationDesc;

    totalIndices = numIndices + numUnusedIndices;

    actualheap = heapRelation;

    for (i = 0; i < totalIndices; i++)
    {
        Relation indexRelation = i < numIndices ? relationDescs[i] : unusedRelationDescs[i - numIndices];
        IndexInfo *indexInfo = NULL;
        IndexUniqueCheck checkUnique;
        Relation actualindex = NULL;
        bool satisfiesConstraint = false;

        indexInfo = i < numIndices ? indexInfoArray[i] : unusedindexInfoArray[i - numIndices];

        if (indexRelation == NULL)
        {
            continue;
        }

        if (!indexInfo->ii_ReadyForInserts)
        {
            continue;
        }

        actualindex = indexRelation;

        O_FormIndexDatum(indexInfo, slot, values, isnull);

        if (!u_sess->attr.attr_common.unique_checks || !indexRelation->rd_index->indisunique)
        {
            checkUnique = UNIQUE_CHECK_NO;
        }
        else if (indexRelation->rd_index->indimmediate)
        {
            checkUnique = UNIQUE_CHECK_YES;
        }
        else
        {
            checkUnique = UNIQUE_CHECK_PARTIAL;
        }

        //        checkUnique = UNIQUE_CHECK_NO;

        satisfiesConstraint = index_insert(actualindex, /* index relation */
                                           values,      /* array of index Datums */
                                           isnull,      /* null flags */
                                           tupleid,     /* tid of heap tuple */
                                           actualheap,  /* heap relation */
                                           checkUnique);
    }
    return result;
}
