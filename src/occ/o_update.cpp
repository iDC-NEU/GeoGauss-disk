//
// Created by hcj2003 on 9/16/25.
//

#include "occ/o_heapam.h"
#include "occ/o_executor.h"
#include "occ/o_txn.h"
#include "occ/occ_internal.h"

#include "access/tableam.h"
#include "access/sysattr.h"
#include "access/visibilitymap.h"
#include "utils/lsyscache.h"
#include "access/tuptoaster.h"

#include "occ/occ_mem.h"

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

        txn->m_accessMgr->SetLastAccessedSlot(slot);

        rc = txn->UpdateLastRowState(AccessType::WR, update_index);

        HeapTuple currDraft = txn->GetLastAccessedDraft();
        currDraft->t_len = heap_tuple->t_len;

        if (update_index)
        {
            currDraft->t_self = heap_tuple->t_self;
            currDraft->t_tableOid = heap_tuple->t_tableOid;
            currDraft->t_bucketId = heap_tuple->t_bucketId;
            HeapTupleCopyBase(currDraft, heap_tuple);
        }

        memcpy_s((char *)currDraft->t_data, currDraft->t_len, (char *)heap_tuple->t_data, heap_tuple->t_len);

        //        rc = txn->OverwriteRow(currDraft);
    } while (0);

    return rc;
}

// 接口 只判断索引列修改,目前tpcc不涉及
TupleTableSlot *O_ExecUpdate(ItemPointer tupleid,
                             Oid oldPartitionOid,
                             int2 bucketid, HeapTupleHeader oldtuple, TupleTableSlot *slot, TupleTableSlot *planSlot, EPQState *epqstate,
                             ModifyTableState *node, bool canSetTag, bool partKeyUpdate, TM_Result *uresultp, char *partExprKeyStr, TM_FailureData *tmfdp)
{
    // 执行阶段需要先分配xid，为提交阶段准备
    TransactionId xid = GetCurrentTransactionId();

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

    block = ItemPointerGetBlockNumber(tupleid);
    buffer = ReadBuffer(relation, block);
    page = BufferGetPage(buffer);

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tupleid));

    oldtup.t_data = (HeapTupleHeader)PageGetItem(page, lp);
    oldtup.t_len = ItemIdGetLength(lp);
    oldtup.t_self = *tupleid;
    oldtup.t_tableOid = RelationGetRelid(relation);
    oldtup.t_bucketId = RelationGetBktid(relation);

    HeapSatisfiesHOTUpdate(relation, hot_attrs, key_attrs, id_attrs, &satisfies_hot,
                           &satisfies_key, &satisfies_id, &oldtup, (HeapTuple)tuple, page);

    if (!satisfies_hot)
    {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

        occ_heap_insert(relation, (HeapTuple)tuple, estate->es_output_cid, 0, NULL, false);

        if (!txn->m_accessMgr->EnsureRowAccessForModify(relation, tupleid, &oldtup)) {
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
            tableam_tops_exec_update_index_tuples(slot, oldslot, relation, node, tuple, tupleid,
                                                  exec_index_tuples_state, bucketid, modifiedIdxAttrs);
        }

        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

        ItemPointer pTSelf = NULL;
        pTSelf = tableam_tops_get_t_self(result_relation_desc, tuple);
        O_InsertRow(pTSelf, estate, (HeapTuple)tuple); // 注册读写集，用于写回rowcsn
    }
    else
    {
        if (!txn->m_accessMgr->EnsureRowAccessForModify(relation, tupleid, &oldtup)) {
            ereport(ERROR, (errmsg("OCC: failed to establish row access for HOT update")));
        }
        O_UpdateRow(txn, (HeapTuple)tuple, slot, estate);
    }

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    ReleaseBuffer(buffer);

    return NULL;
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
    //    Bitmapset* hot_attrs = NULL;
    //    Bitmapset* key_attrs = NULL;
    //    Bitmapset* id_attrs = NULL;
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

    //    bool satisfies_key = false;
    //    bool satisfies_id = false;
    bool satisfies_hot = true; // 此处已经判断过
    bool use_hot_update = false;

    Size new_tup_size, pagefree;
    int options = 0;

    bool all_visible_cleared = false;
    bool all_visible_cleared_new = false;

    HeapTuple old_key_tuple = NULL;
    bool old_key_copied = false;
    char relreplident;

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

    //    hot_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_ALL);
    //    key_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_KEY);
    //    id_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_IDENTITY_KEY);
    //
    //    HeapSatisfiesHOTUpdate(relation, hot_attrs, key_attrs, id_attrs, &satisfies_hot, &satisfies_key, &satisfies_id,
    //                           &oldtup, newtup, page);

    HeapTupleCopyBaseFromPage(&oldtup, BufferGetPage(buffer));

    xmax_old_tuple = xid;
    infomask_old_tuple = 0;
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

    HeapTupleHeaderAdjustCmax(oldtup.t_data, &cid, &is_combo, buffer);

    pagefree = PageGetHeapFreeSpace(page);
    new_tup_size = MAXALIGN(newtup->t_len);

    if (new_tup_size > pagefree)
    {
        /* Clear obsolete visibility flags ... */
        oldtup.t_data->t_infomask &= ~HEAP_XMAX_BITS;
        oldtup.t_data->t_infomask2 &= ~(HEAP_XMAX_LOCK_ONLY | HEAP_KEYS_UPDATED);
        Assert(TransactionIdIsValid(xmax_old_tuple));
        oldtup.t_data->t_infomask |= infomask_old_tuple;
        oldtup.t_data->t_infomask2 |= infomask2_old_tuple;
        HeapTupleClearHotUpdated(&oldtup);
        /* ... and store info about transaction updating this tuple */
        HeapTupleHeaderSetXmax(page, oldtup.t_data, xmax_old_tuple);
        HeapTupleHeaderSetCmax(oldtup.t_data, cid, is_combo);
        /* temporarily make it look not-updated */
        oldtup.t_data->t_ctid = oldtup.t_self;

        HeapTupleCopyBaseFromPage(&oldtup, page);

        already_marked = true;
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

        heaptup = newtup;

        if (new_tup_size > pagefree)
        {
            newbuf = RelationGetBufferForTuple(relation, heaptup->t_len, buffer, options, NULL, &vmbuffer_new,
                                               &vmbuffer, rel_end_block);
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

    START_CRIT_SECTION();

    if (!(options & HEAP_INSERT_SKIP_FSM))
    {
        PageSetPrunable(page, xid);
    }

    if (use_hot_update)
    {
        HeapTupleSetHotUpdated(&oldtup);
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

    oldtup.t_data->m_rowCSN = csn;
    oldtup.t_data->is_latest = false;
    oldtup.t_data->t_ctid = heaptup->t_self;

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

    if (!use_hot_update)
    {
        ResultRelInfo *result_rel_info = NULL;
        Relation result_relation_desc;
        result_rel_info = estate->es_result_relation_info;
        result_relation_desc = result_rel_info->ri_RelationDesc;

        if (result_rel_info->ri_NumIndices > 0)
        {
            Bitmapset *modifiedIdxAttrs = NULL;

            O_ExecInsertIndexTuples(slot, &(heaptup->t_self), estate, modifiedIdxAttrs);
        }
    }

    if (heaptup != newtup)
    {
        newtup->t_self = heaptup->t_self;
        heap_freetuple(heaptup);
    }

    //    bms_free(hot_attrs);
    //    bms_free(key_attrs);
    //    bms_free(id_attrs);
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

    xmax_old_tuple = xid;
    infomask_old_tuple = 0;
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
