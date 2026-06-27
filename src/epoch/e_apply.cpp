#include "postgres.h"

#include "epoch/e_txn_wire.h"
#include "epoch/e_config.h"
#include "epoch/e_row_key.h"

#include "occ/o_heapam.h"

extern List* O_ExecInsertIndexTuples(TupleTableSlot* slot, ItemPointer tupleid, EState* estate,
    Bitmapset* modifiedIdxAttrs);

#include "access/heapam.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "commands/trigger.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

static EState* E_CreateApplyEstate(Relation rel)
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
    estate->es_output_cid = GetCurrentCommandId(true);
    AfterTriggerBeginQuery();
    return estate;
}

static void E_DestroyApplyEstate(EState* estate)
{
    if (estate->es_result_relation_info != nullptr) {
        ExecCloseIndices(estate->es_result_relation_info);
    }
    AfterTriggerEndQuery(estate);
    FreeExecutorState(estate);
}

static void E_CleanupFakeRel(Relation rel, FakeRelationPartition* fakeRelInfo)
{
    if (fakeRelInfo->needRleaseDummyRel && fakeRelInfo->partRel != nullptr) {
        releaseDummyRelation(&fakeRelInfo->partRel);
    }
    if (fakeRelInfo->partList != nullptr) {
        releasePartitionList(rel, &fakeRelInfo->partList, NoLock);
    }
}

static TupleTableSlot* E_MakeSlotFromWireTuple(EState* estate, Relation rel, HeapTuple tuple)
{
    TupleTableSlot* slot = ExecInitExtraTupleSlot(estate, rel->rd_tam_ops);
    ExecSetSlotDescriptor(slot, RelationGetDescr(rel));
    ExecStoreTuple(tuple, slot, InvalidBuffer, false);
    heap_slot_getallattrs(slot);
    return slot;
}

static Oid E_GetRelationIdentityOrPK(Relation rel)
{
    Oid idxoid = RelationGetReplicaIndex(rel);
    if (!OidIsValid(idxoid)) {
        idxoid = RelationGetPrimaryKeyIndex(rel);
    }
    return idxoid;
}

static bool E_FindLocalTuple(EState* estate, Relation rel, TupleTableSlot* searchslot,
    TupleTableSlot* outslot, FakeRelationPartition* fakeRelInfo)
{
    Oid idxoid = E_GetRelationIdentityOrPK(rel);
    if (!OidIsValid(idxoid)) {
        return false;
    }
    return RelationFindReplTuple(estate, rel, idxoid, LockTupleExclusive, searchslot, outslot, fakeRelInfo);
}

static bool E_ApplyInsertRow(Relation rel, HeapTuple tuple, uint64_t csn)
{
    EState* estate = E_CreateApplyEstate(rel);
    EPQState epqstate;
    EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);
    FakeRelationPartition fakeRelInfo;
    TupleTableSlot* remoteslot = E_MakeSlotFromWireTuple(estate, rel, tuple);
    TupleTableSlot* localslot = ExecInitExtraTupleSlot(estate, rel->rd_tam_ops);
    ExecSetSlotDescriptor(localslot, RelationGetDescr(rel));

    ExecOpenIndices(estate->es_result_relation_info, false);
    bool found = E_FindLocalTuple(estate, rel, remoteslot, localslot, &fakeRelInfo);
    bool ok = false;
    if (found && localslot->tts_tuple != nullptr) {
        ItemPointer tid = tableam_tops_get_t_self(rel, localslot->tts_tuple);
        ok = (O_heap_update(rel, tid, tuple, InvalidCommandId, csn, nullptr, nullptr) == TM_Ok);
    } else {
        ItemPointerSetInvalid(&tuple->t_self);
        (void)heap_insert(rel, tuple, InvalidCommandId, 0, nullptr);
        if (!ItemPointerIsValid(&tuple->t_self)) {
            ok = false;
        } else {
            ExecStoreTuple(tuple, remoteslot, InvalidBuffer, false);
            heap_slot_getallattrs(remoteslot);
            (void)O_ExecInsertIndexTuples(remoteslot, &tuple->t_self, estate, NULL);
            O_heap_commit_insert_csn(rel, &tuple->t_self, csn);
            ok = true;
        }
    }

    E_CleanupFakeRel(rel, &fakeRelInfo);
    EvalPlanQualEnd(&epqstate);
    E_DestroyApplyEstate(estate);
    return ok;
}

static bool E_ApplyUpdateRow(Relation rel, HeapTuple tuple, uint64_t csn)
{
    EState* estate = E_CreateApplyEstate(rel);
    EPQState epqstate;
    EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);
    FakeRelationPartition fakeRelInfo;
    TupleTableSlot* remoteslot = E_MakeSlotFromWireTuple(estate, rel, tuple);
    TupleTableSlot* localslot = ExecInitExtraTupleSlot(estate, rel->rd_tam_ops);
    ExecSetSlotDescriptor(localslot, RelationGetDescr(rel));

    ExecOpenIndices(estate->es_result_relation_info, false);
    bool found = E_FindLocalTuple(estate, rel, remoteslot, localslot, &fakeRelInfo);
    bool ok = false;
    if (found && localslot->tts_tuple != nullptr) {
        ItemPointer tid = tableam_tops_get_t_self(rel, localslot->tts_tuple);
        ok = (O_heap_update(rel, tid, tuple, InvalidCommandId, csn, nullptr, nullptr) == TM_Ok);
    } else {
        ItemPointerSetInvalid(&tuple->t_self);
        (void)heap_insert(rel, tuple, InvalidCommandId, 0, nullptr);
        O_heap_commit_insert_csn(rel, &tuple->t_self, csn);
        ok = true;
    }

    E_CleanupFakeRel(rel, &fakeRelInfo);
    EvalPlanQualEnd(&epqstate);
    E_DestroyApplyEstate(estate);
    return ok;
}

static bool E_ApplyDeleteRow(Relation rel, HeapTuple tuple, uint64_t csn)
{
    EState* estate = E_CreateApplyEstate(rel);
    EPQState epqstate;
    EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);
    FakeRelationPartition fakeRelInfo;
    TupleTableSlot* searchslot = E_MakeSlotFromWireTuple(estate, rel, tuple);
    TupleTableSlot* localslot = ExecInitExtraTupleSlot(estate, rel->rd_tam_ops);
    ExecSetSlotDescriptor(localslot, RelationGetDescr(rel));

    ExecOpenIndices(estate->es_result_relation_info, false);
    bool found = E_FindLocalTuple(estate, rel, searchslot, localslot, &fakeRelInfo);
    bool ok = true;
    if (found && localslot->tts_tuple != nullptr) {
        ItemPointer tid = tableam_tops_get_t_self(rel, localslot->tts_tuple);
        ok = (O_heap_delete(rel, tid, InvalidCommandId, csn) == TM_Ok);
    }

    E_CleanupFakeRel(rel, &fakeRelInfo);
    EvalPlanQualEnd(&epqstate);
    E_DestroyApplyEstate(estate);
    return ok;
}

static void E_EnsureApplyTransaction(void)
{
    SetCurrentStatementStartTimestamp();
    if (!IsTransactionState()) {
        StartTransactionCommand();
    }
    PushActiveSnapshot(GetTransactionSnapshot());
}

static void E_EndApplyTransaction(void)
{
    PopActiveSnapshot();
    if (IsTransactionState()) {
        CommitTransactionCommand();
    }
}

bool E_ApplyWireTxn(const E_WireTxn& txn)
{
    if (txn.server_id == EpochGetNodeId()) {
        return true;
    }
    if (txn.rows.empty()) {
        return true;
    }

    /*
     * 每行单独提交，缩短 RowExclusiveLock 持有时间，避免与大量 async backend
     * 交叉持锁导致 worker 在 apply 大 epoch（如 merged=18）时永久阻塞。
     */
    bool anyOk = false;
    for (const auto& row : txn.rows) {
        E_EnsureApplyTransaction();

        Oid localRelid = E_ResolveLocalRelid(row);
        Relation rel = heap_open(localRelid, RowExclusiveLock);
        if (rel == nullptr) {
            E_EndApplyTransaction();
            continue;
        }

        HeapTuple tuple = nullptr;
        if (!E_HeapTupleFromWireRow(row, &tuple)) {
            heap_close(rel, RowExclusiveLock);
            E_EndApplyTransaction();
            continue;
        }

        tuple->t_data->m_rowCSN = txn.csn;
        tuple->t_data->is_latest = true;

        bool rowOk = false;
        switch (row.op_type) {
            case EPOCH_OP_INSERT:
                rowOk = E_ApplyInsertRow(rel, tuple, txn.csn);
                break;
            case EPOCH_OP_UPDATE:
                rowOk = E_ApplyUpdateRow(rel, tuple, txn.csn);
                break;
            case EPOCH_OP_DELETE:
                rowOk = E_ApplyDeleteRow(rel, tuple, txn.csn);
                break;
            default:
                rowOk = false;
                break;
        }

        E_FreeWireHeapTuple(tuple);
        heap_close(rel, RowExclusiveLock);
        E_EndApplyTransaction();

        if (!rowOk) {
            ereport(WARNING,
                (errmsg("epoch apply failed on rel %s(%u) op %u",
                    row.relname.empty() ? "?" : row.relname.c_str(),
                    localRelid,
                    static_cast<unsigned>(row.op_type))));
        } else {
            anyOk = true;
        }
    }

    return anyOk;
}
