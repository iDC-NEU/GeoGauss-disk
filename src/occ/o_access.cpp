//
// Created by hcj2003 on 10/8/25.
//

#include "occ/o_txn.h"
#include "occ/o_heapam.h"
#include "occ/o_executor.h"
#include "occ/o_delta_table.h"
#include "occ/occ_internal.h"

#include "utils/rel.h"
#include "access/xact.h"

static Relation O_OpenAccessRelation(O_Access *access, bool *needClose)
{
    *needClose = false;
    if (access->m_origSentinel.relation != nullptr && OidIsValid(access->m_origSentinel.relid)) {
        return access->m_origSentinel.relation;
    }
    if (!OidIsValid(access->m_origSentinel.relid)) {
        return nullptr;
    }
    *needClose = true;
    return heap_open(access->m_origSentinel.relid, RowExclusiveLock);
}

static void O_MirrorToDelta(O_Access *access, HeapTuple tuple, uint64_t csn, bool deleted)
{
    if (!g_enable_occ_delta || access == nullptr) {
        return;
    }

    O_DeltaTable &delta = O_DeltaTable::Instance();
    if (deleted) {
        delta.MarkDeleted(access->m_origSentinel.relation, &access->m_origSentinel.tuple_id, csn);
        return;
    }

    if (tuple != nullptr && tuple->t_data != nullptr) {
        tuple->t_data->m_rowCSN = csn;
        tuple->t_data->is_latest = true;
        delta.Upsert(access->m_origSentinel.relation, &access->m_origSentinel.tuple_id, tuple, csn);
    }
}

void O_Access::WriteGlobalChanges(uint64_t csn)
{
    if (m_type == RD) {
        return;
    }

    bool relClosed = false;
    Relation rel = O_OpenAccessRelation(this, &relClosed);
    if (rel == nullptr) {
        return;
    }

    /*
     * 始终以 heap 为权威存储落盘；delta 仅作读路径覆盖缓存。
     * INSERT 的索引在 Executor/OpFusion 插入阶段已维护，此处只补写 CSN。
     */
    switch (m_type) {
    case WR:
    case INV: {
        HeapTuple newVersion = m_localRow;
        if (newVersion == nullptr || newVersion->t_data == nullptr) {
            break;
        }
        CommandId cid = m_outputCid;
        if (cid == InvalidCommandId) {
            cid = GetCurrentCommandId(false);
        }
        newVersion->t_data->m_rowCSN = csn;
        newVersion->t_data->is_latest = true;
        if (update_index) {
            O_heap_update2(rel, &m_origSentinel.tuple_id, newVersion, cid, csn);
        } else {
            O_heap_update(rel, &m_origSentinel.tuple_id, newVersion, cid, csn, nullptr, nullptr);
        }
        O_MirrorToDelta(this, newVersion, csn, false);
        break;
    }
    case DEL: {
        CommandId cid = m_outputCid;
        if (cid == InvalidCommandId) {
            cid = GetCurrentCommandId(false);
        }
        O_heap_delete(rel, &m_origSentinel.tuple_id, cid, csn);
        break;
    }
        O_MirrorToDelta(this, nullptr, csn, true);
        break;
    case INS: {
        HeapTuple insTuple = m_localInsertRow ? m_localInsertRow : m_localRow;
        if (insTuple != nullptr && insTuple->t_data != nullptr) {
            insTuple->t_data->m_rowCSN = csn;
            insTuple->t_data->is_latest = true;
        }
        O_heap_commit_insert_csn(rel, &m_origSentinel.tuple_id, csn);
        O_MirrorToDelta(this, insTuple, csn, false);
        break;
    }
    default:
        break;
    }

    if (relClosed) {
        heap_close(rel, RowExclusiveLock);
    }
}
