//
// Created by hcj2003 on 9/16/25.
//

#include "occ/o_txn.h"
#include "occ/occ_internal.h"
#include "occ/o_executor.h"

Tuple O_index_getnext(IndexScanDesc scan, ScanDirection direction, bool* has_cur_xact_write)
{
    Tuple tuple;
    ItemPointer tid;

    HeapTuple currRow;

    for (;;) {
        if (likely(!scan->xs_continue_hot)) {
            tid = index_getnext_tid(scan, direction);
            if (tid == NULL) {
                break;
            }
        } else {
            /*
             * We are resuming scan of a HOT chain after having returned an
             * earlier member.	Must still hold pin on current heap page.
             */
            Assert(BufferIsValid(scan->xs_cbuf));
            Assert(ItemPointerGetBlockNumber(&scan->xs_ctup.t_self) == BufferGetBlockNumber(scan->xs_cbuf));
        }

        O_TxnManager* txn = GetTxnManager();

        RC rc = RC_OK;

        occ_sentinel sentinel(scan->heapRelation, scan->xs_ctup.t_self, scan->xs_snapshot);
        currRow = txn->RowLookup(AccessType::RD, sentinel, rc, scan);

        if (currRow != NULL)
            return (Tuple)currRow;

    }
    return NULL;
}

HeapTuple O_seq_scan(HeapScanDesc scan)
{
    O_TxnManager* txn = GetTxnManager();

    RC rc = RC_OK;

    occ_sentinel sentinel(scan->rs_base.rs_rd, scan->rs_ctup.t_self, scan->rs_base.rs_snapshot);

    HeapTuple currRow = txn->RowLookup(AccessType::RD, sentinel, rc, nullptr, &(scan->rs_ctup));

    return currRow;
}

bool O_check_unique(Relation relation, ItemPointerData tid)
{
    O_TxnManager* txn = GetTxnManager();

    RC rc = RC_OK;

    occ_sentinel sentinel(relation, tid);

    bool res = txn->CheckUnique(AccessType::RD, sentinel, rc);

    return res;
}
