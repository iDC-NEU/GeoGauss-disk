//
// Created by hcj2003 on 9/16/25.
//

#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/xact.h"
#include "access/tableam.h"
#include "catalog/heap.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/storage_gtt.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/matview.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/exec/execMerge.h"
#include "executor/node/nodeModifyTable.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "rewrite/rewriteHandler.h"
#include "replication/dataqueue.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "commands/copy.h"
#include "commands/copypartition.h"
#include "utils/portal.h"
#include "utils/snapmgr.h"
#include "vecexecutor/vecmergeinto.h"
#include "access/heapam.h"
#include "access/ustore/knl_uheap.h"
#include "access/ustore/knl_whitebox_test.h"
#include "gs_ledger/ledger_utils.h"
#include "gs_ledger/userchain.h"

#include "occ/o_txn.h"
#include "occ/o_sentinel.h"
#include "occ/o_txn_insert_action.h"

static TupleTableSlot* ExecModifyTable(PlanState* state)
{
    ModifyTableState* node = castNode(ModifyTableState, state);
    EState* estate = node->ps.state;
    TupleTableSlot* slot = NULL;
    TupleTableSlot* plan_slot = NULL;
    ItemPointer tuple_id = NULL;
    ItemPointerData tuple_ctid;
    HeapTupleHeader old_tuple = NULL;
    JunkFilter* junk_filter = NULL;
    PlanState* subPlanState = NULL;
    ResultRelInfo* result_rel_info = NULL;
    CmdType operation = node->operation;
    int resultRelationNum = node->mt_ResultTupleSlots ? list_length(node->mt_ResultTupleSlots): node->mt_nplans;
    int hi_options = 0;
    List *partition_list = NIL;
    char* partExprKeyStr = NULL;
    Oid old_partition_oid = InvalidOid;
    int2 bucketid = InvalidBktId;
    bool part_key_updated = ((ModifyTable*)node->ps.plan)->partKeyUpdated;

    subPlanState = node->mt_plans[node->mt_whichplan];

    for (;;) {
        if (!node->isinherit) {
            partExprKeyStr = node->partExprKeyStrArray[estate->result_rel_index];
        }

        result_rel_info = node->resultRelInfo + estate->result_rel_index;
        estate->es_result_relation_info = result_rel_info;
        junk_filter = result_rel_info->ri_junkFilter;

        //先读取
        plan_slot = FetchPlanSlot(subPlanState, node->mt_ProjInfos, node->isinherit);

        if (TupIsNull(plan_slot)) {
            node->mt_whichplan++;

            if (node->mt_whichplan < node->mt_nplans) {
                subPlanState = node->mt_plans[node->mt_whichplan];
                junk_filter = result_rel_info->ri_junkFilter;
                continue;
            } else {
                break;
            }
        }

        slot = plan_slot;

        if (junk_filter != NULL) {
            if (operation == CMD_UPDATE || operation == CMD_DELETE) {
                char relkind;
                Datum datum;
                bool isNull = false;

                relkind = result_rel_info->ri_RelationDesc->rd_rel->relkind;
                if (relkind == RELKIND_RELATION || RELKIND_IS_SEQUENCE(relkind)) {
                    datum = ExecGetJunkAttribute(slot, junk_filter->jf_junkAttNo, &isNull);
                    /* when mutil result relations doing left join, maybe exists NULL ctid. */
                    if (isNull) {
                        if (estate->result_rel_index == resultRelationNum - 1) {
                            estate->result_rel_index = 0;
                        } else {
                            estate->result_rel_index++;
                        }

                        continue;
                    }

                    tuple_id = (ItemPointer)DatumGetPointer(datum);
                    tuple_ctid = *tuple_id; /* be sure we don't free ctid!! */
                    tuple_id = &tuple_ctid;
                }
            }

            /*
             * apply the junk_filter if needed.
             */
            if (operation != CMD_DELETE) {
                slot = ExecFilterJunk(junk_filter, slot);
                slot->tts_tam_ops = result_rel_info->ri_RelationDesc->rd_tam_ops;
            }
        }

        switch (operation) {
            case CMD_INSERT: {
                bool replaceNull = CheckPluginReplaceNull() && !IsA(subPlanState, ResultState);
                if (!node->isReplace) {
                    slot = ExecInsert(node, slot, plan_slot, estate, node->canSetTag, hi_options, &partition_list,
                                      partExprKeyStr, replaceNull);
                }
                break;
            }
            case CMD_UPDATE:
                if (!IS_SPQ_RUNNING) {
                    slot = ExecUpdate(tuple_id,
                                      old_partition_oid,
                                      bucketid,
                                      old_tuple,
                                      slot,
                                      plan_slot,
                                      &node->mt_epqstate,
                                      node,
                                      node->canSetTag,
                                      part_key_updated,
                                      NULL,
                                      partExprKeyStr);
                }
                break;
            case CMD_DELETE:
                slot = ExecDelete(
                    tuple_id, old_partition_oid, bucketid, old_tuple, plan_slot, &node->mt_epqstate, node, node->canSetTag);
                break;
            default:
                break;
        }

        if (slot != NULL) {
            return slot;
        }
    }
    return NULL;
}

RC O_UpdateRow(OCCTransactionState s, HeapTuple heap_tuple)
{
    RC rc;

    do {
        rc = s->m_txnManager->UpdateLastRowState(AccessType::WR);

        HeapTuple currDraft = s->m_txnManager->GetLastAccessedDraft();

        currDraft = heap_tuple;

        rc = s->m_txnManager->OverwriteRow(currDraft, modified_columns);
    } while (0);

    return rc;

}

TupleTableSlot* O_ExecUpdate(ItemPointer tupleid,
                           Oid oldPartitionOid,
                           int2 bucketid, HeapTupleHeader oldtuple, TupleTableSlot* slot, TupleTableSlot* planSlot, EPQState* epqstate,
                           ModifyTableState* node, bool canSetTag, bool partKeyUpdate, TM_Result* uresultp, char* partExprKeyStr, TM_FailureData* tmfdp)
{
    EState* estate = node->ps.state;
    Tuple tuple = NULL;
    ResultRelInfo* result_rel_info = NULL;
    Relation result_relation_desc;
    TM_Result result;
    Relation fake_relation = NULL;
    Relation parent_relation = NULL;
    TM_FailureData tmfd;
    bool allow_update_self = (node->mt_upsert != NULL &&
                              node->mt_upsert->us_action != UPSERT_NONE) ? true : false;
    bool allowInplaceUpdate = true;
    bool is_record = false;

    result_rel_info = estate->es_result_relation_info;
    result_relation_desc = result_rel_info->ri_RelationDesc;

    if (result_rel_info->ri_FdwRoutine) {
        slot = result_rel_info->ri_FdwRoutine->ExecForeignUpdate(estate, result_rel_info, slot, planSlot);

        if (slot == NULL) {
            /* "do nothing" */
            return NULL;
        }

        /* FDW might have changed tuple */
        tuple = tableam_tslot_get_tuple_from_slot(result_relation_desc, slot);
    }
    else {
        OCCTransactionState s = GetOCCCurrentTransactionState();
        tuple = tableam_tslot_get_tuple_from_slot(result_relation_desc, slot);
        O_UpdateRow(s, (HeapTuple)tuple);
    }

    if (canSetTag) {
        if (is_record) {
            estate->es_modifiedRowHash = lappend(estate->es_modifiedRowHash, (void *)UInt64GetDatum(res_hash));
        }
        (estate->es_processed)++;
    }

    ExecARUpdateTriggers(estate,
                         result_rel_info,
                         oldPartitionOid,
                         bucketid,
                         new_partId,
                         tupleid,
                         (HeapTuple)tuple,
                         recheck_indexes);

    return ExecProcessReturning(result_rel_info->ri_projectReturning, slot, planSlot);
}


