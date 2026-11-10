//
// Created by hcj2003 on 10/13/25.
//

#include "occ/o_txn.h"
#include "occ/occ_mem.h"
#include "occ/o_staging.h"
#include "occ/occ_internal.h"

// 应该不需要在进行hot扫描
bool O_heap_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer, HeapTuple heap_tuple, Snapshot snapshot)
{
    TransactionId prev_xmax = InvalidTransactionId;
    Page dp = (Page)BufferGetPage(buffer);
    BlockNumber blkno;
    OffsetNumber offnum;

    blkno = ItemPointerGetBlockNumber(tid);
    offnum = ItemPointerGetOffsetNumber(tid);

    bool at_chain_start = true;
    bool valid;

    for (;;)
    {
        ItemId lp;
        lp = PageGetItemId(dp, offnum);

        if (!ItemIdIsNormal(lp))
        {
            if (ItemIdIsRedirected(lp) && at_chain_start)
            {
                offnum = ItemIdGetRedirect(lp);
                at_chain_start = false;
                continue;
            }
            return false;
        }

        heap_tuple->t_len = ItemIdGetLength(lp);
        heap_tuple->t_bucketId = RelationGetBktid(relation);
        ItemPointerSet(&heap_tuple->t_self, blkno, offnum);
        heap_tuple->t_tableOid = RelationGetRelid(relation);

        heap_tuple->t_data = (HeapTupleHeader)PageGetItem(dp, lp);

        if (TransactionIdIsValid(prev_xmax) && !TransactionIdEquals(prev_xmax, HeapTupleGetRawXmin(heap_tuple)))
        {
            return false;
        }

        return true;

        if (snapshot == nullptr)
        {
            return true;
        }

        if (snapshot->satisfies == SNAPSHOT_MVCC)
        {
            valid = HeapTupleSatisfiesMVCC(heap_tuple, snapshot, buffer, NULL);
        }
        else
        {
            valid = HeapTupleSatisfiesVisibility(heap_tuple, snapshot, buffer, NULL);
        }

        if (valid)
        {
            ItemPointerSetOffsetNumber(tid, offnum);
            return true;
        }

        if (HeapTupleIsHotUpdated(heap_tuple))
        {
            offnum = ItemPointerGetOffsetNumber(&heap_tuple->t_data->t_ctid);
            at_chain_start = false;
            prev_xmax = HeapTupleGetUpdateXid(heap_tuple);
        }
        else
        {
            return false;
        }
    }
    return false;
}

HeapTuple occ_sentinel::GetData(HeapTuple heap_tuple, Buffer buffer)
{
    ItemPointer tid = &tuple_id;

    if (OccStagingOverlayEnabled())
    {
        HeapTuple copy = nullptr;
        bool deleted = false;
        if (OccStagingTryReadTuple(relation, tid, &copy, &deleted))
        {
            if (deleted || copy == nullptr) {
                if (copy != nullptr) {
                    heap_freetuple(copy);
                }
                return nullptr;
            }
            return copy;
        }
    }

    O_heap_hot_search_buffer(tid, relation, buffer, heap_tuple, snapshot);

    return heap_tuple;
}

HeapTuple occ_sentinel::GetData(HeapTuple heap_tuple)
{
    ItemPointer tid = &tuple_id;

    if (OccStagingOverlayEnabled())
    {
        HeapTuple copy = nullptr;
        bool deleted = false;
        if (OccStagingTryReadTuple(relation, tid, &copy, &deleted))
        {
            if (deleted || copy == nullptr) {
                if (copy != nullptr) {
                    heap_freetuple(copy);
                }
                return nullptr;
            }
            return copy;
        }
    }

    Buffer buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));

    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    O_heap_hot_search_buffer(tid, relation, buffer, heap_tuple, snapshot);

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    ReleaseBuffer(buffer);

    return heap_tuple;
}
