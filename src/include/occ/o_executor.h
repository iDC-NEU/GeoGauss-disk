//
// Created by hcj2003 on 10/9/25.
//

#ifndef OPENGAUSS_O_EXECUTOR_H
#define OPENGAUSS_O_EXECUTOR_H

#include "executor/executor.h"

//extern List* O_ExecInsertIndexTuples(TupleTableSlot* slot, ItemPointer tupleid, EState* estate,
//                                     HeapTuple heap_tuple);

extern void O_InsertRow(ItemPointer tupleid, EState* estate,
                 HeapTuple heap_tuple);

extern TupleTableSlot* O_ExecUpdate(ItemPointer tupleid,
                                    Oid oldPartitionOid,
                                    int2 bucketid, HeapTupleHeader oldtuple, TupleTableSlot* slot, TupleTableSlot* planSlot, EPQState* epqstate,
                                    ModifyTableState* node, bool canSetTag, bool partKeyUpdate, TM_Result* uresultp, char* partExprKeyStr = NULL, TM_FailureData* tmfdp = NULL);

extern TupleTableSlot* O_ExecDelete(ItemPointer tupleid, Oid deletePartitionOid, int2 bucketid, HeapTupleHeader oldtuple,
                                  TupleTableSlot* planSlot, EPQState* epqstate, ModifyTableState* node, bool canSetTag);

extern Tuple O_index_getnext(IndexScanDesc scan, ScanDirection direction, bool* has_cur_xact_write = NULL);

extern HeapTuple O_seq_scan(HeapScanDesc scan);

extern bool O_check_unique(Relation relation, ItemPointerData tid);

#endif  // OPENGAUSS_O_EXECUTOR_H
