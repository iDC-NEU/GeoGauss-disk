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

extern bool O_heap_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer,
                                     HeapTuple heap_tuple, Snapshot snapshot);

/*
 * 沿 HOT 链解析索引 TID 对应的 heap 物理位置（ItemIdIsNormal）。
 * 不做 MVCC 可见性判断，仅跟随 redirect，供锁定/FK 检查使用。
 */
extern bool O_ResolvePhysicalTid(Relation relation, ItemPointer indexTid,
                               ItemPointerData *outPhysicalTid);

extern bool O_ResolveIndexPhysicalTid(IndexScanDesc scan, ItemPointer indexTid,
                                      ItemPointerData *outPhysicalTid);

#endif  // OPENGAUSS_O_HEAPAM_H
