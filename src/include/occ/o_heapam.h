//
// Created by hcj2003 on 10/9/25.
//

#ifndef OPENGAUSS_O_HEAPAM_H
#define OPENGAUSS_O_HEAPAM_H

#include "access/hio.h"
#include "executor/executor.h"

extern TM_Result O_heap_update(Relation relation,  ItemPointer otid, HeapTuple newtup,
                               CommandId cid, uint64_t csn, TupleTableSlot* slot, EState* estate);
TM_Result O_heap_update2(Relation relation,  ItemPointer otid, HeapTuple newtup,
                         CommandId cid, uint64_t csn);

extern TM_Result O_heap_delete(Relation relation, ItemPointer tid, CommandId cid, uint64_t csn);

/* 提交阶段为已插入 heap 元组写入 CSN（索引在 INSERT 时已维护） */
extern void O_heap_commit_insert_csn(Relation relation, ItemPointer otid, uint64_t csn);

#endif  // OPENGAUSS_O_HEAPAM_H
