//
// Created by hcj2003 on 10/8/25.
//

#include "occ/o_txn.h"
#include "occ/o_heapam.h"
#include "occ/o_executor.h"
#include "occ/o_staging.h"
#include "occ/occ_internal.h"

#include "epoch/e_config.h"

#include "utils/rel.h"
#include "access/heapam.h"
#include "access/xact.h"

static Relation O_OpenAccessRelation(O_Access *access, bool *needClose)
{
    Oid relid = OccResolveAccessRelid(access, nullptr);
    if (!OidIsValid(relid)) {
        *needClose = false;
        return nullptr;
    }
    access->m_origSentinel.relid = relid;

    O_TxnManager *txn = GetTxnManager();
    if (txn != nullptr) {
        return txn->GetCachedRelation(relid, nullptr, needClose);
    }

    *needClose = true;
    return heap_open(relid, RowExclusiveLock);
}

static bool O_StageAccessCommit(O_Access *access, Relation rel, ItemPointer tid, uint64_t csn)
{
    OccStagingEnsureInit();

    switch (access->m_type) {
    case WR:
    case INV: {
        HeapTuple newVersion = access->m_localRow;
        if (newVersion == nullptr || newVersion->t_data == nullptr) {
            return false;
        }
        newVersion->t_data->m_rowCSN = csn;
        newVersion->t_data->is_latest = true;
        return OccStageAccessCommit(rel, tid, newVersion, csn, access->m_type);
    }
    case DEL:
        return OccStageAccessCommit(rel, tid, nullptr, csn, access->m_type);
    case INS: {
        HeapTuple insTuple = access->m_localInsertRow ? access->m_localInsertRow : access->m_localRow;
        if (insTuple != nullptr && insTuple->t_data != nullptr) {
            insTuple->t_data->m_rowCSN = csn;
            insTuple->t_data->is_latest = true;
        }
        return OccStageAccessCommit(rel, tid, insTuple, csn, access->m_type);
    }
    default:
        return false;
    }
}

void O_Access::WriteGlobalChanges(uint64_t csn)
{
    WriteGlobalChanges(csn, m_origSentinel);
}

void O_Access::WriteGlobalChanges(uint64_t csn, const occ_sentinel &key)
{
    if (m_type == RD) {
        return;
    }

    Oid relid = InvalidOid;
    ItemPointerData tid;
    if (!OccPrepareAccessTarget(this, &key, &relid, &tid)) {
        return;
    }

    bool relClosed = false;
    Relation rel = O_OpenAccessRelation(this, &relClosed);
    if (rel == nullptr || !OccTidIsPlausible(&tid, rel)) {
        if (relClosed && rel != nullptr) {
            heap_close(rel, RowExclusiveLock);
        }
        return;
    }
    m_origSentinel.tuple_id = tid;

    /*
     * Tip staging（g_epoch_local_tip_staging）：UPDATE/DELETE 只写 tip staging，
     * 与 apply 共用 EpochTipStaging，避免本地 O_heap_update 把 tip 搬走后
     * flush inplace 永远对着旧 root 失败。INSERT 仍走堆上 finalize。
     */
    if (g_enable_occ_staging || (g_epoch_local_tip_staging && m_type != INS)) {
        (void)O_StageAccessCommit(this, rel, &tid, csn);
        if (relClosed) {
            heap_close(rel, RowExclusiveLock);
        }
        return;
    }

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
    case INS: {
        HeapTuple insTuple = m_localInsertRow ? m_localInsertRow : m_localRow;
        if (insTuple != nullptr && insTuple->t_data != nullptr) {
            insTuple->t_data->m_rowCSN = csn;
            insTuple->t_data->is_latest = true;
        }
        O_heap_commit_insert_csn(rel, &m_origSentinel.tuple_id, csn);
        break;
    }
    default:
        break;
    }

    if (relClosed) {
        heap_close(rel, RowExclusiveLock);
    }
}
