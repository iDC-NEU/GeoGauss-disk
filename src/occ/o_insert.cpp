//
// Created by hcj2003 on 9/21/25.
//

#include "occ/o_executor.h"
#include "occ/o_txn.h"
#include "occ/occ_internal.h"

#include "catalog/index.h"

//接口
void O_InsertRow(ItemPointer tupleid, EState* estate,
                 HeapTuple heap_tuple)
{
    ResultRelInfo* resultRelInfo = NULL;
    Relation heapRelation;
    resultRelInfo = estate->es_result_relation_info;
    if (resultRelInfo == nullptr) {
        return;
    }
    heapRelation = resultRelInfo->ri_RelationDesc;
    if (heapRelation == nullptr) {
        return;
    }

    O_TxnManager* txn = GetTxnManager();
    if (txn == nullptr) {
        return;
    }
    occ_sentinel sentinel(heapRelation, *tupleid);
    txn->InsertRow(heap_tuple, sentinel);
}


//List* O_ExecInsertIndexTuples(TupleTableSlot* slot, ItemPointer tupleid, EState* estate,
//                            HeapTuple heap_tuple)
//{
//    List* result = NIL;
//    ResultRelInfo* resultRelInfo = NULL;
//    int i;
//    int totalIndices;
//    int numIndices;
//    int numUnusedIndices;
//    RelationPtr relationDescs;
//    RelationPtr unusedRelationDescs;
//    IndexInfo** indexInfoArray;
//    IndexInfo** unusedindexInfoArray;
//    Datum values[INDEX_MAX_KEYS];
//    bool isnull[INDEX_MAX_KEYS];
//    Relation actualheap;
//    Relation heapRelation;
//
//    resultRelInfo = estate->es_result_relation_info;
//    numIndices = resultRelInfo->ri_NumIndices;
//    relationDescs = resultRelInfo->ri_IndexRelationDescs;
//    indexInfoArray = resultRelInfo->ri_IndexRelationInfo;
//
//    numUnusedIndices = resultRelInfo->ri_NumUnusableIndices;
//    unusedRelationDescs = resultRelInfo->ri_UnusableIndexRelationDescs;
//    unusedindexInfoArray = resultRelInfo->ri_UnusableIndexRelationInfo;
//
//    heapRelation = resultRelInfo->ri_RelationDesc;
//
//    actualheap = heapRelation;
//    totalIndices = numIndices + numUnusedIndices;
//
//    for (i = 0; i < totalIndices; i++) {
//        Relation actualindex = NULL;
//        IndexInfo* indexInfo = NULL;
//        bool satisfiesConstraint = false;
//        IndexUniqueCheck checkUnique;
//        Relation indexRelation = i < numIndices ? relationDescs[i] : unusedRelationDescs[i - numIndices];
//
//        actualindex = indexRelation;
//
//        indexInfo = i < numIndices ?  indexInfoArray[i] : unusedindexInfoArray[i - numIndices];
//
//        FormIndexDatum(indexInfo, slot, estate, values, isnull);
//
//        if (!u_sess->attr.attr_common.unique_checks || !indexRelation->rd_index->indisunique) {
//            checkUnique = UNIQUE_CHECK_NO;
//        }
//        index_insert(actualindex, /* index relation */
//                                           values,                                     /* array of index Datums */
//                                           isnull,                                     /* null flags */
//                                           tupleid,                                    /* tid of heap tuple */
//                                           actualheap,                                 /* heap relation */
//                                           checkUnique);
//
//
//        O_TxnManager* txn = GetTxnManager();
//        occ_sentinel sentinel = {heapRelation, *tupleid};
//        txn->InsertRow(heap_tuple, sentinel);
//    }
//
//    return result;
//}