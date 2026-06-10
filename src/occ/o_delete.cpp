//
// Created by hcj2003 on 9/21/25.
//

#include "occ/o_heapam.h"
#include "occ/o_executor.h"
#include "occ/o_txn.h"
#include "occ/occ_internal.h"

#include "access/visibilitymap.h"
#include "utils/lsyscache.h"
#include "access/tuptoaster.h"
#include "access/sysattr.h"

#include "occ/occ_mem.h"

static void ComputeNewXmaxInfomask(TransactionId xmax, uint16 old_infomask, uint16 old_infomask2,
                                   TransactionId add_to_xmax, LockTupleMode mode, bool is_update,
                                   TransactionId *result_xmax, uint16 *result_infomask, uint16 *result_infomask2);

static XLogRecPtr log_heap_new_cid(Relation relation, HeapTuple tup);

static XLogRecPtr log_heap_new_cid_insert(xl_heap_new_cid *xlrec, int bucketid);

static HeapTuple ExtractReplicaIdentity(Relation relation, HeapTuple tp, bool key_changed, bool *copy,
                                        char *relreplident);

static uint8 ComputeInfobits(uint16 infomask, uint16 infomask2);

// 接口
TupleTableSlot *O_ExecDelete(ItemPointer tupleid, Oid deletePartitionOid, int2 bucketid, HeapTupleHeader oldtuple,
                             TupleTableSlot *planSlot, EPQState *epqstate, ModifyTableState *node, bool canSetTag)
{
    // 执行阶段需要先分配xid，为提交阶段准备
    TransactionId xid = GetCurrentTransactionId();

    EState *estate = node->ps.state;
    ResultRelInfo *result_rel_info = NULL;
    Relation result_relation_desc;
    TM_Result result;
    TM_FailureData tmfd;
    Partition partition = NULL;
    Relation fake_relation = NULL;
    Relation part_relation = NULL;
    uint64 res_hash = 0;
    uint64 hash_del = 0;
    bool is_record = false;
    TupleTableSlot *slot = NULL;

    /*
     * get information on the (current) result relation
     */
    result_rel_info = estate->es_result_relation_info;
    result_relation_desc = result_rel_info->ri_RelationDesc;

    fake_relation = result_relation_desc;

    TupleTableSlot *oldslot = NULL;

    fake_relation = result_relation_desc;

    O_TxnManager *txn = GetTxnManager();

    HeapTupleData oldtup;
    Page page;
    BlockNumber block = ItemPointerGetBlockNumber(tupleid);
    Buffer buffer = ReadBuffer(result_relation_desc, block);
    page = BufferGetPage(buffer);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    ItemId lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tupleid));
    oldtup.t_data = (HeapTupleHeader)PageGetItem(page, lp);
    oldtup.t_len = ItemIdGetLength(lp);
    oldtup.t_self = *tupleid;
    oldtup.t_tableOid = RelationGetRelid(result_relation_desc);
    oldtup.t_bucketId = RelationGetBktid(result_relation_desc);
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);

    if (!txn->m_accessMgr->EnsureRowAccessForModify(result_relation_desc, tupleid, &oldtup)) {
        ereport(ERROR, (errmsg("OCC: failed to establish row access for delete")));
    }

    txn->DeleteLastRow();

    txn->m_accessMgr->SetLastAccessedEstate(estate);

    if (canSetTag)
    {
        if (is_record)
        {
            estate->es_modifiedRowHash = lappend(estate->es_modifiedRowHash, (void *)UInt64GetDatum(res_hash));
        }
        (estate->es_processed)++;
    }

    //    return ExecProject(result_rel_info->ri_projectReturning);
    return NULL;
}

//===============================================================

TM_Result O_heap_delete(Relation relation, ItemPointer tid, CommandId cid, uint64 csn)
{
    Page page;
    BlockNumber block;
    TransactionId xid = GetCurrentTransactionId();
    Buffer buffer;
    ItemId lp;
    HeapTupleData tp;
    bool is_combo = false;
    bool all_visible_cleared = false;

    TransactionId new_xmax;
    uint16 new_infomask;
    uint16 new_infomask2;

    HeapTuple old_key_tuple = NULL; /* replica identity of the tuple */
    bool old_key_copied = false;
    char identity;

    block = ItemPointerGetBlockNumber(tid);
    buffer = ReadBuffer(relation, block);
    page = BufferGetPage(buffer);

    Buffer vmbuffer = InvalidBuffer;

    if (PageIsAllVisible(page))
    {
        visibilitymap_pin(relation, block, &vmbuffer);
    }

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));

    tp.t_tableOid = RelationGetRelid(relation);
    tp.t_bucketId = RelationGetBktid(relation);
    tp.t_data = (HeapTupleHeader)PageGetItem(page, lp);
    tp.t_len = ItemIdGetLength(lp);
    tp.t_self = *tid;
    HeapTupleCopyBaseFromPage(&tp, page);

    HeapTupleHeaderAdjustCmax(tp.t_data, &cid, &is_combo, buffer);

    (void)heap_page_prepare_for_xid(relation, buffer, xid, false);

    HeapTupleCopyBaseFromPage(&tp, page);

    old_key_tuple = ExtractReplicaIdentity(relation, &tp, true, &old_key_copied, &identity);

    ComputeNewXmaxInfomask(HeapTupleGetRawXmax(&tp), tp.t_data->t_infomask, tp.t_data->t_infomask2, xid,
                           LockTupleExclusive, true, &new_xmax, &new_infomask, &new_infomask2);

    PageSetPrunable(page, xid);

    if (PageIsAllVisible(page))
    {
        all_visible_cleared = true;
        PageClearAllVisible(page);
        visibilitymap_clear(relation, BufferGetBlockNumber(buffer), vmbuffer);
    }

    /* store transaction information of xact deleting the tuple */
    tp.t_data->t_infomask &= ~HEAP_XMAX_BITS;
    tp.t_data->t_infomask2 &= ~(HEAP_XMAX_LOCK_ONLY | HEAP_KEYS_UPDATED);
    tp.t_data->t_infomask |= new_infomask;
    tp.t_data->t_infomask2 |= new_infomask2;
    HeapTupleHeaderClearHotUpdated(tp.t_data);
    HeapTupleHeaderSetXmax(page, tp.t_data, new_xmax);
    HeapTupleHeaderSetCmax(tp.t_data, cid, is_combo);

    tp.t_data->m_rowCSN = csn;

    /* Make sure there is no forward chain link in t_ctid */
    tp.t_data->t_ctid = tp.t_self;

    MarkBufferDirty(buffer);

    /* XLOG stuff */
    if (RelationNeedsWAL(relation))
    {
        xl_heap_delete xlrec;
        XLogRecPtr recptr;
        xl_heap_header xlhdr;
        bool useOldXlog;

        /* For logical decode we need combocids to properly decode the catalog */
        if (RelationIsAccessibleInLogicalDecoding(relation))
        {
            (void)log_heap_new_cid(relation, &tp);
        }

        xlrec.flags = all_visible_cleared ? XLH_DELETE_ALL_VISIBLE_CLEARED : 0;
        xlrec.offnum = ItemPointerGetOffsetNumber(&tp.t_self);
        xlrec.xmax = new_xmax;
        xlrec.infobits_set = ComputeInfobits(tp.t_data->t_infomask, tp.t_data->t_infomask2);
        useOldXlog = t_thrd.proc->workingVersionNum < ENHANCED_TUPLE_LOCK_VERSION_NUM ||
                     !(xlrec.infobits_set & XLHL_XMAX_IS_MULTI);
#ifdef ENABLE_MULTIPLE_NODES
        useOldXlog = true;
#endif

        if (old_key_tuple != NULL)
        {
            bool is_null = false;
            char relreplident;
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

            if (is_null)
            {
                relreplident = REPLICA_IDENTITY_NOTHING;
            }
            else
            {
                relreplident = CharGetDatum(replident);
            }

            if (relreplident == REPLICA_IDENTITY_FULL)
            {
                xlrec.flags |= XLH_DELETE_CONTAINS_OLD_TUPLE;
            }
            else
            {
                xlrec.flags |= XLH_DELETE_CONTAINS_OLD_KEY;
            }
            heap_freetuple(tuple);
        }

        XLogBeginInsert();
        XLogRegisterData((char *)&xlrec, useOldXlog ? SizeOfOldHeapDelete : SizeOfHeapDelete);
        CommitSeqNo curCSN = InvalidCommitSeqNo;
        LogCSN(&curCSN);

        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

        /*
         * Log replica identity of the deleted tuple if there is one
         */
        if (old_key_tuple != NULL)
        {
            xlhdr.t_infomask2 = old_key_tuple->t_data->t_infomask2;
            xlhdr.t_infomask = old_key_tuple->t_data->t_infomask;
            xlhdr.t_hoff = old_key_tuple->t_data->t_hoff;

            XLogRegisterData((char *)&xlhdr, SizeOfHeapHeader);
            XLogRegisterData((char *)old_key_tuple->t_data + offsetof(HeapTupleHeaderData, t_bits),
                             old_key_tuple->t_len - offsetof(HeapTupleHeaderData, t_bits));
        }

        /* filtering by origin on a row level is much more efficient */
        XLogIncludeOrigin();
        recptr =
            XLogInsert(RM_HEAP_ID, useOldXlog ? XLOG_HEAP_DELETE : XLOG_HEAP_DELETE | XLOG_TUPLE_LOCK_UPGRADE_FLAG);

        PageSetLSN(page, recptr);
    }

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    if (vmbuffer != InvalidBuffer)
    {
        ReleaseBuffer(vmbuffer);
    }

    ReleaseBuffer(buffer);

    if (old_key_tuple != NULL && old_key_copied)
    {
        heap_freetuple(old_key_tuple);
    }

    return TM_Ok;
}

//==============================================================================================

static void ComputeNewXmaxInfomask(TransactionId xmax, uint16 old_infomask, uint16 old_infomask2,
                                   TransactionId add_to_xmax, LockTupleMode mode, bool is_update,
                                   TransactionId *result_xmax, uint16 *result_infomask, uint16 *result_infomask2)
{
    TransactionId new_xmax;
    uint16 new_infomask;
    uint16 new_infomask2;

    Assert(TransactionIdIsCurrentTransactionId(add_to_xmax));

l5:
    new_infomask = 0;
    new_infomask2 = 0;
    if (old_infomask & HEAP_XMAX_INVALID)
    {
        if (is_update)
        {
            new_xmax = add_to_xmax;
            if (mode == LockTupleExclusive)
                new_infomask2 |= HEAP_KEYS_UPDATED;
        }
    }

    *result_infomask = new_infomask;
    *result_infomask2 = new_infomask2;
    *result_xmax = new_xmax;
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