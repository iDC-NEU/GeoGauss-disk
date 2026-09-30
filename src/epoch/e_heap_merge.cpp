#include "postgres.h"

#include "epoch/e_heap_merge.h"
#include "epoch/e_row_key.h"

#include "access/heapam.h"
#include "access/tableam.h"
#include "access/htup.h"
#include "catalog/indexing.h"
#include "catalog/pg_partition_fn.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/resowner.h"

static Oid EpochHeapMergeGetRelationIdentityOrPK(Relation rel)
{
    Oid idxoid = RelationGetReplicaIndex(rel);
    if (!OidIsValid(idxoid)) {
        idxoid = RelationGetPrimaryKeyIndex(rel);
    }
    return idxoid;
}

static EState* EpochHeapMergeCreateEstate(Relation rel)
{
    EState* estate = CreateExecutorState();
    RangeTblEntry* rte = makeNode(RangeTblEntry);
    rte->rtekind = RTE_RELATION;
    rte->relid = RelationGetRelid(rel);
    rte->relkind = rel->rd_rel->relkind;
    estate->es_range_table = list_make1(rte);

    ResultRelInfo* resultRelInfo = makeNode(ResultRelInfo);
    InitResultRelInfo(resultRelInfo, rel, 1, 0);
    estate->es_result_relations = resultRelInfo;
    estate->es_num_result_relations = 1;
    estate->es_result_relation_info = resultRelInfo;
    estate->es_output_cid = GetCurrentCommandId(false);
    return estate;
}

static void EpochHeapMergeDestroyEstate(EState* estate)
{
    if (estate == nullptr) {
        return;
    }
    if (estate->es_result_relation_info != nullptr) {
        ExecCloseIndices(estate->es_result_relation_info);
    }
    FreeExecutorState(estate);
}

static void EpochHeapMergeCleanupFakeRel(Relation rel, FakeRelationPartition* fakeRelInfo)
{
    if (fakeRelInfo->needRleaseDummyRel && fakeRelInfo->partRel != nullptr) {
        releaseDummyRelation(&fakeRelInfo->partRel);
    }
    if (fakeRelInfo->partList != nullptr) {
        releasePartitionList(rel, &fakeRelInfo->partList, NoLock);
    }
}

static TupleTableSlot* EpochHeapMergeMakeSlot(EState* estate, Relation rel, HeapTuple tuple)
{
    TupleTableSlot* slot = ExecInitExtraTupleSlot(estate, rel->rd_tam_ops);
    ExecSetSlotDescriptor(slot, RelationGetDescr(rel));
    ExecStoreTuple(tuple, slot, InvalidBuffer, false);
    heap_slot_getallattrs(slot);
    return slot;
}

static bool EpochHeapMergeFindLocalRowCsn(Relation rel,
    EState* estate,
    HeapTuple searchTuple,
    uint64_t* out_csn,
    bool* out_found)
{
    FakeRelationPartition fakeRelInfo;
    Oid idxoid = EpochHeapMergeGetRelationIdentityOrPK(rel);
    if (!OidIsValid(idxoid)) {
        *out_found = false;
        return false;
    }

    TupleTableSlot* searchslot = EpochHeapMergeMakeSlot(estate, rel, searchTuple);
    TupleTableSlot* localslot = ExecInitExtraTupleSlot(estate, rel->rd_tam_ops);
    ExecSetSlotDescriptor(localslot, RelationGetDescr(rel));

    bool found = RelationFindReplTupleEpoch(estate, rel, idxoid, searchslot, localslot, &fakeRelInfo);
    if (found && localslot->tts_tuple != nullptr) {
        HeapTuple ht = (HeapTuple)localslot->tts_tuple;
        if (ht->t_data != nullptr) {
            *out_found = true;
            *out_csn = ht->t_data->m_rowCSN;
        } else {
            *out_found = false;
        }
    } else {
        *out_found = false;
    }

    EpochHeapMergeCleanupFakeRel(rel, &fakeRelInfo);
    return true;
}

bool EpochHeapFindRowCsn(const E_WireRow& row, uint64_t* out_csn, bool* out_found)
{
    if (out_csn == nullptr || out_found == nullptr) {
        return false;
    }
    *out_csn = 0;
    *out_found = false;

    if (t_thrd.utils_cxt.CurrentResourceOwner == nullptr) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("epoch heap merge: CurrentResourceOwner is NULL (worker session not bootstrapped)")));
    }

    Oid localRelid = E_ResolveLocalRelid(row);
    if (!OidIsValid(localRelid)) {
        return false;
    }

    Relation rel = heap_open(localRelid, AccessShareLock);
    if (rel == nullptr) {
        return false;
    }

    HeapTuple searchTuple = nullptr;
    if (!E_HeapTupleFromWireRow(row, &searchTuple)) {
        heap_close(rel, AccessShareLock);
        return false;
    }

    EState* estate = EpochHeapMergeCreateEstate(rel);
    uint64_t heap_csn = 0;
    bool found = false;
    bool ok = EpochHeapMergeFindLocalRowCsn(rel, estate, searchTuple, &heap_csn, &found);
    EpochHeapMergeDestroyEstate(estate);
    E_FreeWireHeapTuple(searchTuple);
    heap_close(rel, AccessShareLock);

    if (!ok) {
        return false;
    }
    *out_csn = heap_csn;
    *out_found = found;
    return true;
}
