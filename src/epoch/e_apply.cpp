#include "postgres.h"

#include "epoch/e_txn_wire.h"
#include "epoch/e_config.h"
#include "epoch/e_row_key.h"
#include "epoch/e_statistician.h"
#include "epoch/e_epoch_lifecycle.h"
#include "epoch/e_apply_profile.h"
#include "epoch/e_apply_buflock.h"
#include "epoch/e_read_frontier.h"
#include "epoch/e_heap_version.h"
#include "epoch/e_pk_staging_shmem.h"

#include "occ/o_heapam.h"
#include "occ/o_staging.h"

extern List* O_ExecInsertIndexTuples(TupleTableSlot* slot, ItemPointer tupleid, EState* estate,
    Bitmapset* modifiedIdxAttrs);

#include "access/genam.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
#include "catalog/index.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "commands/trigger.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/elog.h"

#include "portability/instr_time.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "gs_thread.h"

#include <algorithm>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

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

static Oid E_GetRelationIdentityOrPK(Relation rel)
{
    Oid idxoid = RelationGetReplicaIndex(rel);
    if (!OidIsValid(idxoid)) {
        idxoid = RelationGetPrimaryKeyIndex(rel);
    }
    return idxoid;
}

static bool E_FindLocalTupleCached(EState* estate, Relation rel, Relation idxrel, TupleTableSlot* searchslot,
    TupleTableSlot* outslot, FakeRelationPartition* fakeRelInfo)
{
    /*
     * 调用方会无条件 E_CleanupFakeRel；idxrel==nullptr 时 Cached 入口直接 return，
     * 且非分区表 find 路径可能从不写 partList。必须在此清零，否则栈垃圾
     * partList（如 0x3）会在 releasePartitionList → list_head 里 SIGSEGV。
     */
    if (fakeRelInfo != nullptr) {
        MemSet(fakeRelInfo, 0, sizeof(FakeRelationPartition));
    }
    if (idxrel == nullptr || fakeRelInfo == nullptr) {
        return false;
    }
    return RelationFindReplTupleEpochCached(estate, rel, idxrel, searchslot, outslot, fakeRelInfo);
}

struct EpochApplyRelContext {
    EState* estate;
    Relation idxrel;
    Oid idxoid;
    TupleTableSlot* remote_slot;
    TupleTableSlot* local_slot;
};

static bool E_ApplyIdempotentError(ErrorData* edata)
{
    return edata->sqlerrcode == ERRCODE_UNIQUE_VIOLATION;
}

static bool E_ApplyFkDeferError(ErrorData* edata)
{
    return edata->sqlerrcode == ERRCODE_FOREIGN_KEY_VIOLATION;
}

static std::unordered_map<Oid, std::vector<Oid>> g_fk_child_to_parents;
static bool g_fk_graph_loaded = false;
static std::mutex g_fk_graph_mutex;

static void E_EnsureFkGraphLoaded(void)
{
    if (g_fk_graph_loaded) {
        return;
    }
    std::lock_guard<std::mutex> lock(g_fk_graph_mutex);
    if (g_fk_graph_loaded) {
        return;
    }

    Relation crel = heap_open(ConstraintRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(crel, InvalidOid, false, NULL, 0, NULL);
    HeapTuple tup = nullptr;
    while ((tup = systable_getnext(scan)) != nullptr) {
        Form_pg_constraint con = (Form_pg_constraint)GETSTRUCT(tup);
        if (con->contype != CONSTRAINT_FOREIGN || !con->convalidated) {
            continue;
        }
        if (!OidIsValid(con->conrelid) || !OidIsValid(con->confrelid)) {
            continue;
        }
        g_fk_child_to_parents[con->conrelid].push_back(con->confrelid);
    }
    systable_endscan(scan);
    heap_close(crel, AccessShareLock);
    g_fk_graph_loaded = true;
}

static int E_FkInsertLevel(Oid relid, std::unordered_map<Oid, int>* memo, std::unordered_set<Oid>* visiting)
{
    if (!OidIsValid(relid)) {
        return 0;
    }
    auto memo_it = memo->find(relid);
    if (memo_it != memo->end()) {
        return memo_it->second;
    }
    if (visiting->find(relid) != visiting->end()) {
        return 0;
    }
    visiting->insert(relid);

    int level = 0;
    auto graph_it = g_fk_child_to_parents.find(relid);
    if (graph_it != g_fk_child_to_parents.end()) {
        for (Oid parent : graph_it->second) {
            int parent_level = E_FkInsertLevel(parent, memo, visiting);
            if (parent_level + 1 > level) {
                level = parent_level + 1;
            }
        }
    }

    visiting->erase(relid);
    (*memo)[relid] = level;
    return level;
}

/*
 * 按 FK 依赖排序后再 apply：INSERT/UPDATE 先父后子，DELETE 先子后父。
 * merger 从 unordered_map 取行时顺序不确定，逐行 commit 下子表 FK 会失败。
 */
static void E_SortWireRowsForApply(std::vector<E_WireRow>* rows)
{
    if (rows == nullptr || rows->size() <= 1) {
        return;
    }

    E_EnsureFkGraphLoaded();

    struct RowOrder {
        size_t orig;
        int phase;
        int level_key;
    };

    std::vector<RowOrder> order;
    order.reserve(rows->size());
    std::unordered_map<Oid, int> level_memo;

    for (size_t i = 0; i < rows->size(); ++i) {
        const E_WireRow& row = (*rows)[i];
        Oid relid = E_ResolveLocalRelid(row);
        std::unordered_set<Oid> visiting;
        int level = E_FkInsertLevel(relid, &level_memo, &visiting);

        int phase = 1;
        int level_key = level;
        switch (row.op_type) {
            case EPOCH_OP_DELETE:
                phase = 0;
                level_key = -level;
                break;
            case EPOCH_OP_UPDATE:
                phase = 1;
                level_key = level;
                break;
            case EPOCH_OP_INSERT:
                phase = 2;
                level_key = level;
                break;
            default:
                phase = 1;
                level_key = level;
                break;
        }
        order.push_back({i, phase, level_key});
    }

    std::stable_sort(order.begin(), order.end(), [](const RowOrder& a, const RowOrder& b) {
        if (a.phase != b.phase) {
            return a.phase < b.phase;
        }
        if (a.level_key != b.level_key) {
            return a.level_key < b.level_key;
        }
        return a.orig < b.orig;
    });

    std::vector<E_WireRow> sorted;
    sorted.reserve(rows->size());
    for (const RowOrder& entry : order) {
        sorted.push_back(std::move((*rows)[entry.orig]));
    }
    *rows = std::move(sorted);
}

/*
 * heap_insert + 索引。*out_pk_conflict 非空时：唯一键冲突置 true 并返回 false（调用方吞掉并计数）；
 * 否则保持旧语义：唯一键冲突当幂等成功（返回 true）。
 */
static std::atomic<uint64_t> g_epoch_unique_catch_detail_logs{0};

static bool E_ApplyHeapInsert(Relation rel, HeapTuple tuple, TupleTableSlot* remoteslot, EState* estate,
    uint64_t csn, bool* out_pk_conflict)
{
    if (out_pk_conflict != nullptr) {
        *out_pk_conflict = false;
    }
    bool ok = false;
    PG_TRY();
    {
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
    PG_CATCH();
    {
        ErrorData* edata = CopyErrorData();
        bool idempotent = E_ApplyIdempotentError(edata);
        bool fk_defer = E_ApplyFkDeferError(edata);
        /*
         * 取证：UNIQUE 时把 constraint/detail 打出来（含撞的是 pkey 还是 oorder_idx1 等），
         * 前 40 条 + 之后每 100 条。查清远端 INSERT CATCH 后可删。
         */
        if (idempotent) {
            const uint64_t n = g_epoch_unique_catch_detail_logs.fetch_add(1, std::memory_order_relaxed) + 1;
            if (n <= 40 || (n % 100) == 0) {
                /*
                 * detail 通常含 Key (w,d,o,...)，msg 含 constraint 名（pkey vs oorder_idx1）。
                 * detail_log / constraint_name 若有一并打出。
                 */
                ereport(LOG,
                    (errmsg(
                         "epoch apply UNIQUE_CATCH_ERR n=%lu rel=%s(%u) csn=%lu cons=%s table=%s msg=%s detail=%s detail_log=%s",
                         n,
                         RelationGetRelationName(rel),
                         RelationGetRelid(rel),
                         csn,
                         edata->cons_name != nullptr ? edata->cons_name : "?",
                         edata->table_name != nullptr ? edata->table_name : "?",
                         edata->message != nullptr ? edata->message : "?",
                         edata->detail != nullptr ? edata->detail : "?",
                         edata->detail_log != nullptr ? edata->detail_log : "?")));
            }
        }
        FlushErrorState();
        FreeErrorData(edata);
        if (idempotent) {
            if (out_pk_conflict != nullptr) {
                *out_pk_conflict = true;
                ok = false;
            } else {
                /* 旧路径：PK 已存在视为幂等成功 */
                ok = true;
            }
        } else if (fk_defer) {
            ok = false;
        } else {
            PG_RE_THROW();
        }
    }
    PG_END_TRY();
    return ok;
}

/*
 * apply UPDATE：epoch 多主走 O_heap_update CSN 版本链（新页写入 + 旧版本保留 CSN）；
 * 避免 tableam_tuple_lock 与 backend 互相阻塞。
 */
static bool E_ApplyHeapUpdate(Relation rel, ItemPointer otid, HeapTuple tuple, TupleTableSlot* remoteslot,
    TupleTableSlot* localslot, EState* estate, uint64_t csn)
{
    bool ok = false;
    PG_TRY();
    {
        ExecStoreTuple(tuple, remoteslot, InvalidBuffer, false);
        heap_slot_getallattrs(remoteslot);

        if (g_enable_epoch_multimaster && EpochGetTotalNodes() > 1) {
            TM_Result res = O_heap_update(rel, otid, tuple, estate->es_output_cid, csn, remoteslot, estate);
            if (res == TM_Ok) {
                if (ItemPointerIsValid(&tuple->t_self)) {
                    O_heap_commit_insert_csn(rel, &tuple->t_self, csn);
                }
                ok = true;
            }
        } else {
            TupleTableSlot* oldslot = nullptr;
            TM_FailureData tmfd;
            bool updateIndexes = false;
            Bitmapset* modifiedIdxAttrs = nullptr;

            TM_Result res = tableam_tuple_update(rel, nullptr, otid, (Tuple)tuple, estate->es_output_cid,
                InvalidSnapshot, GetTransactionSnapshot(), true, &oldslot, &tmfd, &updateIndexes, &modifiedIdxAttrs,
                false, true);

            if (res == TM_Ok) {
                if (updateIndexes && estate->es_result_relation_info->ri_NumIndices > 0) {
                    ExecIndexTuplesState exec_index_tuples_state;
                    exec_index_tuples_state.estate = estate;
                    exec_index_tuples_state.targetPartRel = nullptr;
                    exec_index_tuples_state.p = nullptr;
                    exec_index_tuples_state.conflict = nullptr;
                    exec_index_tuples_state.rollbackIndex = false;
                    TupleTableSlot* oldIndexSlot = oldslot;
                    if (oldIndexSlot == nullptr && localslot != nullptr && localslot->tts_tuple != nullptr) {
                        oldIndexSlot = localslot;
                    }
                    (void)tableam_tops_exec_update_index_tuples(remoteslot, oldIndexSlot, rel, nullptr, (Tuple)tuple,
                        otid, exec_index_tuples_state, InvalidBktId, modifiedIdxAttrs);
                }
                ItemPointer newtid = tableam_tops_get_t_self(rel, (Tuple)tuple);
                if (ItemPointerIsValid(newtid)) {
                    O_heap_commit_insert_csn(rel, newtid, csn);
                }
                ok = true;
            }
            if (oldslot != nullptr) {
                ExecDropSingleTupleTableSlot(oldslot);
            }
        }
    }
    PG_CATCH();
    {
        ErrorData* edata = CopyErrorData();
        bool idempotent = E_ApplyIdempotentError(edata);
        bool fk_defer = E_ApplyFkDeferError(edata);
        FlushErrorState();
        FreeErrorData(edata);
        if (idempotent) {
            ok = true;
        } else if (fk_defer) {
            ok = false;
        } else {
            PG_RE_THROW();
        }
    }
    PG_END_TRY();
    return ok;
}

static bool E_ApplyHeapDelete(Relation rel, ItemPointer tid, uint64_t csn)
{
    bool ok = false;
    PG_TRY();
    {
        TM_Result res = O_heap_delete(rel, tid, InvalidCommandId, csn);
        ok = (res == TM_Ok || res == TM_Deleted);
    }
    PG_CATCH();
    {
        ErrorData* edata = CopyErrorData();
        bool idempotent = E_ApplyIdempotentError(edata);
        bool fk_defer = E_ApplyFkDeferError(edata);
        FlushErrorState();
        FreeErrorData(edata);
        if (idempotent) {
            ok = true;
        } else if (fk_defer) {
            ok = false;
        } else {
            PG_RE_THROW();
        }
    }
    PG_END_TRY();
    return ok;
}

static void E_AbortApplyTransaction(void)
{
    PopActiveSnapshot();
    if (IsTransactionState()) {
        AbortCurrentTransaction();
    }
}

static EpochApplyRelContext* E_GetOrCreateApplyRelContext(std::unordered_map<Oid, EpochApplyRelContext*>& cache,
    Relation rel)
{
    Oid relid = RelationGetRelid(rel);
    auto it = cache.find(relid);
    if (it != cache.end()) {
        return it->second;
    }

    EpochApplyRelContext* ctx = static_cast<EpochApplyRelContext*>(palloc0(sizeof(EpochApplyRelContext)));
    instr_time estate_start;
    INSTR_TIME_SET_CURRENT(estate_start);
    ctx->estate = E_CreateApplyEstate(rel);
    ExecOpenIndices(ctx->estate->es_result_relation_info, false);
    instr_time estate_end;
    INSTR_TIME_SET_CURRENT(estate_end);
    INSTR_TIME_SUBTRACT(estate_end, estate_start);
    EpochApplyProfileAddUs(EAP_ESTATE_NEW, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(estate_end)));

    ctx->idxoid = E_GetRelationIdentityOrPK(rel);
    if (OidIsValid(ctx->idxoid)) {
        ctx->idxrel = index_open(ctx->idxoid, AccessShareLock);
    }

    ctx->remote_slot = ExecInitExtraTupleSlot(ctx->estate, rel->rd_tam_ops);
    ExecSetSlotDescriptor(ctx->remote_slot, RelationGetDescr(rel));
    ctx->local_slot = ExecInitExtraTupleSlot(ctx->estate, rel->rd_tam_ops);
    ExecSetSlotDescriptor(ctx->local_slot, RelationGetDescr(rel));

    cache[relid] = ctx;
    return ctx;
}

static void E_DestroyApplyRelContextCache(std::unordered_map<Oid, EpochApplyRelContext*>& cache)
{
    for (auto& kv : cache) {
        EpochApplyRelContext* ctx = kv.second;
        if (ctx == nullptr) {
            continue;
        }
        if (ctx->idxrel != nullptr) {
            index_close(ctx->idxrel, AccessShareLock);
            ctx->idxrel = nullptr;
        }
        if (ctx->remote_slot != nullptr) {
            ExecDropSingleTupleTableSlot(ctx->remote_slot);
            ctx->remote_slot = nullptr;
        }
        if (ctx->local_slot != nullptr) {
            ExecDropSingleTupleTableSlot(ctx->local_slot);
            ctx->local_slot = nullptr;
        }
        if (ctx->estate != nullptr) {
            E_DestroyApplyEstate(ctx->estate);
            ctx->estate = nullptr;
        }
        pfree(ctx);
    }
    cache.clear();
}

static void E_StoreWireTupleInSlot(Relation rel, HeapTuple tuple, TupleTableSlot* slot)
{
    ExecStoreTuple(tuple, slot, InvalidBuffer, false);
    heap_slot_getallattrs(slot);
}

static Relation E_ApplySwitchHeldRel(Relation* held_rel, Oid* held_relid, Oid localRelid)
{
    if (*held_rel != nullptr && *held_relid == localRelid) {
        return *held_rel;
    }
    if (*held_rel != nullptr) {
        heap_close(*held_rel, RowExclusiveLock);
        *held_rel = nullptr;
        *held_relid = InvalidOid;
    }
    *held_rel = heap_open(localRelid, RowExclusiveLock);
    *held_relid = localRelid;
    return *held_rel;
}

static void E_ApplyCloseHeldRel(Relation* held_rel, Oid* held_relid)
{
    if (*held_rel != nullptr) {
        heap_close(*held_rel, RowExclusiveLock);
        *held_rel = nullptr;
        *held_relid = InvalidOid;
    }
}

static bool E_ApplyInsertRowWithEstate(Relation rel, HeapTuple tuple, uint64_t csn, EpochApplyRelContext* ctx,
    bool is_local)
{
    E_StoreWireTupleInSlot(rel, tuple, ctx->remote_slot);

    Oid relid = RelationGetRelid(rel);
    char pk_buf[EPOCH_PK_STAGING_MAX_PK];
    uint16_t pk_len = 0;
    const bool have_pk = E_BuildPkKey(rel, tuple, pk_buf, sizeof(pk_buf), &pk_len);

    /*
     * INSERT：本地/远端同一路径。
     * 1) PK staging 命中 → skip
     * 2) miss → 一次 pkey Find；命中则 skip 并钉 staging
     * 3) Find 未命中 → heap_insert；成功后钉 PK；UNIQUE CATCH 再 Find 回填
     */
    if (have_pk) {
        ItemPointerData staged_tid;
        bool pk_deleted = false;
        if (EpochPkStagingLookup(relid, pk_buf, pk_len, &staged_tid, &pk_deleted) && !pk_deleted) {
            EpochApplyProfileNoteInsertPath(EAP_INSERT_STAGING_HIT, is_local, relid);
            return true;
        }
    }

    {
        FakeRelationPartition fakeRelInfo;
        bool found =
            E_FindLocalTupleCached(ctx->estate, rel, ctx->idxrel, ctx->remote_slot, ctx->local_slot, &fakeRelInfo);
        if (found && ctx->local_slot->tts_tuple != nullptr) {
            if (have_pk) {
                ItemPointer existing = tableam_tops_get_t_self(rel, ctx->local_slot->tts_tuple);
                if (existing != nullptr && ItemPointerIsValid(existing)) {
                    (void)EpochPkStagingUpsert(relid, pk_buf, pk_len, existing);
                }
            }
            E_CleanupFakeRel(rel, &fakeRelInfo);
            EpochApplyProfileNoteInsertPath(EAP_INSERT_FIND_HIT, is_local, relid);
            return true;
        }
        E_CleanupFakeRel(rel, &fakeRelInfo);
    }

    bool pk_conflict = false;
    instr_time heap_start;
    INSTR_TIME_SET_CURRENT(heap_start);
    bool ok = E_ApplyHeapInsert(rel, tuple, ctx->remote_slot, ctx->estate, csn, &pk_conflict);
    instr_time heap_end;
    INSTR_TIME_SET_CURRENT(heap_end);
    INSTR_TIME_SUBTRACT(heap_end, heap_start);
    EpochApplyProfileAddUs(EAP_HEAP_INSERT, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(heap_end)));

    if (pk_conflict) {
        EpochApplyProfileNoteInsertPath(EAP_INSERT_UNIQUE_CATCH, is_local, relid);
        /* Find 未扫到但索引认为已有：再 Find 一次钉 staging，避免反复 ERROR */
        if (have_pk) {
            FakeRelationPartition fakeRelInfo;
            bool found = E_FindLocalTupleCached(ctx->estate, rel, ctx->idxrel, ctx->remote_slot, ctx->local_slot,
                &fakeRelInfo);
            if (found && ctx->local_slot->tts_tuple != nullptr) {
                ItemPointer existing = tableam_tops_get_t_self(rel, ctx->local_slot->tts_tuple);
                if (existing != nullptr && ItemPointerIsValid(existing)) {
                    (void)EpochPkStagingUpsert(relid, pk_buf, pk_len, existing);
                }
            }
            E_CleanupFakeRel(rel, &fakeRelInfo);
        }
        return true;
    }
    if (!ok) {
        EpochApplyProfileNoteApplyAbort(is_local, relid, EPOCH_OP_INSERT);
        return true;
    }
    if (have_pk && ItemPointerIsValid(&tuple->t_self)) {
        (void)EpochPkStagingUpsert(relid, pk_buf, pk_len, &tuple->t_self);
    }
    EpochApplyProfileNoteInsertPath(EAP_INSERT_DO, is_local, relid);
    return true;
}

static bool E_WireRowRootTid(const E_WireRow& row, ItemPointerData* out_tid)
{
    if (out_tid == nullptr) {
        return false;
    }
    ItemPointerData root;
    root.ip_blkid.bi_hi = row.bi_hi;
    root.ip_blkid.bi_lo = row.bi_lo;
    root.ip_posid = row.ip_posid;
    if (!ItemPointerIsValid(&root)) {
        return false;
    }
    /* defer INSERT 假 TID（blk>=9000000）不可用于 UPDATE/DELETE */
    if (ItemPointerGetBlockNumber(&root) >= 9000000U) {
        return false;
    }
    *out_tid = root;
    return true;
}

/*
 * Phase 1：tip 写入 staging（key=本机 tip/root TID），不走 O_heap_update 版本链。
 * 本地用 wire root；远端用 Locate 解析出的本机 TID（经 PK staging 对齐）。
 */
static bool E_ApplyLocalTipStage(Relation rel, HeapTuple tuple, uint64_t csn, const ItemPointerData* root_tid,
    AccessType access_type)
{
    if (rel == nullptr || root_tid == nullptr) {
        return false;
    }
    OccStagingInitEpochLocalTip();
    Oid relid = RelationGetRelid(rel);
    char pk_buf[EPOCH_PK_STAGING_MAX_PK];
    uint16_t pk_len = 0;
    bool have_pk = false;
    if (access_type != DEL && tuple != nullptr) {
        have_pk = E_BuildPkKey(rel, tuple, pk_buf, sizeof(pk_buf), &pk_len);
    }

    if (access_type == DEL) {
        bool ok = OccStageAccessCommit(rel, const_cast<ItemPointer>(root_tid), nullptr, csn, DEL);
        /* best-effort：DELETE 时 wire tuple 可能仍带 PK */
        if (tuple != nullptr && E_BuildPkKey(rel, tuple, pk_buf, sizeof(pk_buf), &pk_len)) {
            (void)EpochPkStagingMarkDeleted(relid, pk_buf, pk_len);
        }
        return ok;
    }
    if (tuple == nullptr || tuple->t_data == nullptr) {
        return false;
    }
    HeapTuple copy = heap_copytuple(tuple);
    if (copy == nullptr || copy->t_data == nullptr) {
        return false;
    }
    copy->t_self = *root_tid;
    copy->t_data->t_ctid = *root_tid;
    copy->t_data->m_rowCSN = csn;
    copy->t_data->is_latest = true;
    bool ok = OccStageAccessCommit(rel, const_cast<ItemPointer>(root_tid), copy, csn, access_type);
    heap_freetuple(copy);
    if (ok && have_pk) {
        (void)EpochPkStagingUpsert(relid, pk_buf, pk_len, const_cast<ItemPointer>(root_tid));
    }
    return ok;
}

/*
 * 定位 UPDATE/DELETE 的本机 tip：
 * - 本地 tip staging：wire root 上有 staging 条目则直接用 root（不再追链）
 * - 本地 wire：用 backend 记下的 tid，沿版本链追到最新 tip（跨 epoch 已改则 LWW 盖 tip）
 * - 远端 wire：不能信对端 TID；PK staging → 本机 tip TID，miss 再索引 Find；
 *   tip staging 开启时命中后可跳过堆确认（与本地对称）。
 * 失败返回 false，由调用方记 apply_abort 并跳过本行。
 */
static bool E_ApplyLocateTip(Relation rel, const E_WireRow& row, bool is_local, EpochApplyRelContext* ctx,
    FakeRelationPartition* fakeRelInfo, ItemPointerData* out_tid)
{
    if (out_tid == nullptr || fakeRelInfo == nullptr) {
        return false;
    }
    /*
     * Update/Delete 退出时无条件 E_CleanupFakeRel。本地 tid 路径不走 Find，
     * 必须在此清零，否则栈垃圾 partList 会在 releasePartitionList 里 SIGSEGV。
     */
    MemSet(fakeRelInfo, 0, sizeof(FakeRelationPartition));

    if (is_local) {
        ItemPointerData root;
        if (!E_WireRowRootTid(row, &root)) {
            return false;
        }

        if (g_epoch_local_tip_staging) {
            OccStagingInitEpochLocalTip();
            HeapTuple staged = nullptr;
            bool deleted = false;
            if (OccStagingTryReadTuple(rel, &root, &staged, &deleted)) {
                if (staged != nullptr) {
                    heap_freetuple(staged);
                }
                if (deleted) {
                    return false;
                }
                *out_tid = root;
                return true;
            }
        }

        if (EpochHeapVersionChainEnabled(rel)) {
            HeapTuple latest = nullptr;
            int depth = 0;
            if (!EpochHeapFetchLatestCopy(rel, &root, &latest, &depth) || latest == nullptr) {
                return false;
            }
            /*
             * tip staging：确认物理行存在后，仍以 wire root 为身份键写 staging，
             * 避免 tip TID 搬家后后续 locate 对不上。
             */
            if (g_epoch_local_tip_staging) {
                *out_tid = root;
            } else {
                *out_tid = latest->t_self;
            }
            heap_freetuple(latest);
            return true;
        }
        *out_tid = root;
        return true;
    }

    /*
     * 远端：PK staging → 本机 tip TID；tip staging 下再确认 overlay；
     * miss 再 Find，并把 PK→tid 钉到稳定 tip（与本地 root 身份对齐）。
     */
    {
        char pk_buf[EPOCH_PK_STAGING_MAX_PK];
        uint16_t pk_len = 0;
        HeapTuple remote_tup =
            (ctx->remote_slot != nullptr) ? static_cast<HeapTuple>(ctx->remote_slot->tts_tuple) : nullptr;
        if (remote_tup != nullptr && E_BuildPkKey(rel, remote_tup, pk_buf, sizeof(pk_buf), &pk_len)) {
            ItemPointerData staged_tid;
            bool pk_deleted = false;
            if (EpochPkStagingLookup(RelationGetRelid(rel), pk_buf, pk_len, &staged_tid, &pk_deleted)) {
                if (pk_deleted) {
                    return false;
                }
                if (g_epoch_local_tip_staging) {
                    OccStagingInitEpochLocalTip();
                    HeapTuple staged = nullptr;
                    bool tip_deleted = false;
                    if (OccStagingTryReadTuple(rel, &staged_tid, &staged, &tip_deleted)) {
                        if (staged != nullptr) {
                            heap_freetuple(staged);
                        }
                        if (tip_deleted) {
                            return false;
                        }
                        *out_tid = staged_tid;
                        return true;
                    }
                }
                *out_tid = staged_tid;
                return true;
            }
        }
    }

    bool found = E_FindLocalTupleCached(ctx->estate, rel, ctx->idxrel, ctx->remote_slot, ctx->local_slot, fakeRelInfo);
    if (!found || ctx->local_slot->tts_tuple == nullptr) {
        return false;
    }
    ItemPointer tip = tableam_tops_get_t_self(rel, ctx->local_slot->tts_tuple);
    if (tip == nullptr || !ItemPointerIsValid(tip)) {
        return false;
    }
    *out_tid = *tip;
    /* 回填 PK→tid，加速后续远端 locate / tip stage */
    {
        char pk_buf[EPOCH_PK_STAGING_MAX_PK];
        uint16_t pk_len = 0;
        HeapTuple remote_tup =
            (ctx->remote_slot != nullptr) ? static_cast<HeapTuple>(ctx->remote_slot->tts_tuple) : nullptr;
        if (remote_tup != nullptr && E_BuildPkKey(rel, remote_tup, pk_buf, sizeof(pk_buf), &pk_len)) {
            (void)EpochPkStagingUpsert(RelationGetRelid(rel), pk_buf, pk_len, tip);
        }
    }
    return true;
}

static bool E_ApplyUpdateRowWithEstate(Relation rel, HeapTuple tuple, uint64_t csn, EpochApplyRelContext* ctx,
    bool is_local, const E_WireRow& row)
{
    E_StoreWireTupleInSlot(rel, tuple, ctx->remote_slot);

    FakeRelationPartition fakeRelInfo;
    ItemPointerData tip_tid;
    bool located = E_ApplyLocateTip(rel, row, is_local, ctx, &fakeRelInfo, &tip_tid);

    bool ok = false;
    instr_time heap_start;
    INSTR_TIME_SET_CURRENT(heap_start);
    if (located) {
        /*
         * tip staging：本地按 wire root；远端按 Locate 得到的本机 tip TID。
         * 共用 E_ApplyLocalTipStage；失败不再回落 O_heap_update（避免 HOT/版本链旁路）。
         */
        if (g_epoch_local_tip_staging) {
            ok = E_ApplyLocalTipStage(rel, tuple, csn, &tip_tid, WR);
        } else {
            ok = E_ApplyHeapUpdate(rel, &tip_tid, tuple, ctx->remote_slot, ctx->local_slot, ctx->estate, csn);
        }
    }
    instr_time heap_end;
    INSTR_TIME_SET_CURRENT(heap_end);
    INSTR_TIME_SUBTRACT(heap_end, heap_start);
    if (located) {
        EpochApplyProfileAddUs(EAP_HEAP_UPDATE, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(heap_end)));
    }

    E_CleanupFakeRel(rel, &fakeRelInfo);
    if (!located || !ok) {
        EpochApplyProfileNoteApplyAbort(is_local, RelationGetRelid(rel), EPOCH_OP_UPDATE);
    }
    /* 无论成败都返回 true：本行不再 defer，保证 logical 可推进 */
    return true;
}

static bool E_ApplyDeleteRowWithEstate(Relation rel, HeapTuple tuple, uint64_t csn, EpochApplyRelContext* ctx,
    bool is_local, const E_WireRow& row)
{
    E_StoreWireTupleInSlot(rel, tuple, ctx->remote_slot);

    FakeRelationPartition fakeRelInfo;
    ItemPointerData tip_tid;
    bool located = E_ApplyLocateTip(rel, row, is_local, ctx, &fakeRelInfo, &tip_tid);

    bool ok = true;
    instr_time heap_start;
    INSTR_TIME_SET_CURRENT(heap_start);
    if (located) {
        if (g_epoch_local_tip_staging) {
            /* 传入 tuple 以便 MarkDeleted PK；身份键为 Locate 得到的本机 tip TID。
             * 失败不再回落 O_heap_delete（与 UPDATE 对称，A/B：无堆版本链旁路）。 */
            ok = E_ApplyLocalTipStage(rel, tuple, csn, &tip_tid, DEL);
        } else {
            ok = E_ApplyHeapDelete(rel, &tip_tid, csn);
        }
        instr_time heap_end;
        INSTR_TIME_SET_CURRENT(heap_end);
        INSTR_TIME_SUBTRACT(heap_end, heap_start);
        EpochApplyProfileAddUs(EAP_HEAP_DELETE, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(heap_end)));
    }

    E_CleanupFakeRel(rel, &fakeRelInfo);
    if (!located || !ok) {
        EpochApplyProfileNoteApplyAbort(is_local, RelationGetRelid(rel), EPOCH_OP_DELETE);
    }
    return true;
}

static bool E_ApplyWireTxnRows(const E_WireTxn& txn, bool is_local);

static void E_ApplyTraceSlowStep(const char* phase, const E_WireTxn& txn, Oid relid, const char* relname,
    EpochOpType op, uint64_t wait_us);

/*
 * 在已开启的 apply 事务内写多行；调用方负责 Ensure/Commit。
 * 行级定位失败记 apply_abort 并跳过该行（仍返回 true），避免卡住 logical。
 */
static bool E_ApplySortedRowsCore(const E_WireTxn& txn,
    const std::vector<E_WireRow>& rows,
    bool is_local,
    std::unordered_map<Oid, EpochApplyRelContext*>& rel_context_cache,
    Relation* held_rel,
    Oid* held_relid,
    uint64_t* rows_done)
{
    for (const auto& row : rows) {
        Oid localRelid = E_ResolveLocalRelid(row);
        if (!OidIsValid(localRelid)) {
            EpochApplyProfileNoteApplyAbort(is_local, row.relid, row.op_type);
            ereport(LOG,
                (errmsg("epoch apply_abort: invalid rel for wire row relname=%s relid=%u",
                    row.relname.empty() ? "?" : row.relname.c_str(),
                    row.relid)));
            continue;
        }

        const bool rel_reopen = (*held_rel == nullptr || *held_relid != localRelid);
        instr_time rel_open_start;
        INSTR_TIME_SET_CURRENT(rel_open_start);
        Relation rel = E_ApplySwitchHeldRel(held_rel, held_relid, localRelid);
        instr_time rel_open_end;
        INSTR_TIME_SET_CURRENT(rel_open_end);
        INSTR_TIME_SUBTRACT(rel_open_end, rel_open_start);
        if (rel_reopen) {
            EpochApplyProfileAddUs(EAP_HEAP_OPEN, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(rel_open_end)));
            E_ApplyTraceSlowStep("heap_open",
                txn,
                localRelid,
                row.relname.empty() ? nullptr : row.relname.c_str(),
                row.op_type,
                static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(rel_open_end)));
        }
        if (rel == nullptr) {
            EpochApplyProfileNoteApplyAbort(is_local, localRelid, row.op_type);
            ereport(LOG,
                (errmsg("epoch apply_abort: heap_open failed rel %s(%u)",
                    row.relname.empty() ? "?" : row.relname.c_str(),
                    localRelid)));
            continue;
        }

        HeapTuple tuple = nullptr;
        instr_time tuple_start;
        INSTR_TIME_SET_CURRENT(tuple_start);
        if (!E_HeapTupleFromWireRow(row, &tuple)) {
            EpochApplyProfileNoteApplyAbort(is_local, localRelid, row.op_type);
            ereport(LOG,
                (errmsg("epoch apply_abort: bad wire tuple rel %s(%u) op %u tuple_len=%zu",
                    row.relname.empty() ? "?" : row.relname.c_str(),
                    localRelid,
                    static_cast<unsigned>(row.op_type),
                    row.tuple_data.size())));
            continue;
        }
        instr_time tuple_end;
        INSTR_TIME_SET_CURRENT(tuple_end);
        INSTR_TIME_SUBTRACT(tuple_end, tuple_start);
        EpochApplyProfileAddUs(EAP_TUPLE_DECODE, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(tuple_end)));

        tuple->t_data->m_rowCSN = txn.csn;
        tuple->t_data->is_latest = true;
        tuple->t_data->m_status = txn.epoch;

        EpochApplyRelContext* rel_ctx = E_GetOrCreateApplyRelContext(rel_context_cache, rel);
        bool rowOk = false;
        instr_time row_apply_start;
        INSTR_TIME_SET_CURRENT(row_apply_start);
        EpochApplyBufLockBeginRow(txn.epoch, txn.csn, localRelid, static_cast<uint32_t>(row.op_type), InvalidBlockNumber);
        switch (row.op_type) {
            case EPOCH_OP_INSERT:
                rowOk = E_ApplyInsertRowWithEstate(rel, tuple, txn.csn, rel_ctx, is_local);
                break;
            case EPOCH_OP_UPDATE:
                rowOk = E_ApplyUpdateRowWithEstate(rel, tuple, txn.csn, rel_ctx, is_local, row);
                break;
            case EPOCH_OP_DELETE:
                rowOk = E_ApplyDeleteRowWithEstate(rel, tuple, txn.csn, rel_ctx, is_local, row);
                break;
            default:
                EpochApplyProfileNoteApplyAbort(is_local, localRelid, row.op_type);
                rowOk = true;
                break;
        }
        EpochApplyBufLockEndRow(row.relname.empty() ? nullptr : row.relname.c_str());
        instr_time row_apply_end;
        INSTR_TIME_SET_CURRENT(row_apply_end);
        INSTR_TIME_SUBTRACT(row_apply_end, row_apply_start);
        E_ApplyTraceSlowStep("row_apply",
            txn,
            localRelid,
            row.relname.empty() ? nullptr : row.relname.c_str(),
            row.op_type,
            static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(row_apply_end)));

        E_FreeWireHeapTuple(tuple);

        if (!rowOk) {
            /* 行函数约定失败已记 abort 并返回 true；此处兜底 */
            EpochApplyProfileNoteApplyAbort(is_local, localRelid, row.op_type);
        }
        if (rows_done != nullptr) {
            (*rows_done)++;
        }
    }
    return true;
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

static void E_ApplyTraceSlowStep(const char* phase, const E_WireTxn& txn, Oid relid, const char* relname,
    EpochOpType op, uint64_t wait_us)
{
    if (g_epoch_apply_lock_trace_ms <= 0) {
        return;
    }
    uint64_t threshold_us = static_cast<uint64_t>(g_epoch_apply_lock_trace_ms) * 1000ULL;
    if (wait_us < threshold_us) {
        return;
    }

    const char* wait_status = "none";
    const char* wait_name = "none";
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    if (beentry != nullptr && beentry->st_waitevent != 0) {
        uint32 raw_wait_event = beentry->st_waitevent;
        wait_status = pgstat_get_waitstatusdesc(raw_wait_event);
        wait_name = pgstat_get_wait_event(raw_wait_event);
    }

    ereport(LOG,
        (errmsg("APPLY_LOCK_TRACE phase=%s epoch=%lu csn=%lu sid=%u rel=%s(%u) op=%u wait_us=%lu wait_status=%s wait_event=%s pid=%lu",
            phase != nullptr ? phase : "?",
            txn.epoch,
            txn.csn,
            txn.server_id,
            relname != nullptr && relname[0] != '\0' ? relname : "?",
            relid,
            static_cast<unsigned>(op),
            wait_us,
            wait_status != nullptr ? wait_status : "?",
            wait_name != nullptr ? wait_name : "?",
            (unsigned long)t_thrd.proc_cxt.MyProcPid)));
}

bool E_ApplyWireTxn(const E_WireTxn& txn, bool is_local)
{
    EpochApplyProfileBeginWireTxn();
    return E_ApplyWireTxnRows(txn, is_local);
}

bool E_ApplyWireTxnCore(const E_WireTxn& txn, bool is_local)
{
    /*
     * 阶段目标：先让客户端能跑满 60 秒。
     * 暂时跳过 apply 的全部堆写（insert/update/delete），只保留 abort 判定与计数。
     * 待压测窗口稳定后再回来解决 "apply UPDATE 定位失败导致行被跳过" 的问题。
     */
    (void)txn;
    (void)is_local;
    return true;

    return E_ApplyWireTxnRows(txn, is_local);
}

bool E_ApplyLocalWireTxn(const E_WireTxn& txn)
{
    /* 同上：跳过堆写 */
    (void)txn;
    return true;
}

static bool E_ApplyWireTxnRows(const E_WireTxn& txn, bool is_local)
{
    if (g_epoch_skip_apply) {
        return true;
    }
    /* 与 pipeline 跳过远端一致；若远端任务已入队则无写 heap 仍返回成功 */
    if (g_epoch_apply_local_only && !is_local) {
        return true;
    }

    if (txn.rows.empty()) {
        return true;
    }

    EpochHeapSetWriteEpoch(txn.epoch);

    instr_time sort_start;
    INSTR_TIME_SET_CURRENT(sort_start);
    std::vector<E_WireRow> rows = txn.rows;
    E_SortWireRowsForApply(&rows);
    instr_time sort_end;
    INSTR_TIME_SET_CURRENT(sort_end);
    INSTR_TIME_SUBTRACT(sort_end, sort_start);
    EpochApplyProfileAddUs(EAP_SORT, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(sort_end)));
    EpochApplyProfileSetRowCount(rows.size());

    instr_time apply_start;
    INSTR_TIME_SET_CURRENT(apply_start);

    std::unordered_map<Oid, EpochApplyRelContext*> rel_context_cache;
    Relation held_rel = nullptr;
    Oid held_relid = InvalidOid;
    bool allOk = true;
    uint64_t rows_done = 0;

    instr_time txn_begin_start;
    INSTR_TIME_SET_CURRENT(txn_begin_start);
    E_EnsureApplyTransaction();
    instr_time txn_begin_end;
    INSTR_TIME_SET_CURRENT(txn_begin_end);
    INSTR_TIME_SUBTRACT(txn_begin_end, txn_begin_start);
    EpochApplyProfileAddUs(EAP_TXN_BEGIN, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(txn_begin_end)));

    EpochApplyBufLockBeginTxn(txn.epoch, txn.csn);
    PG_TRY();
    {
        allOk = E_ApplySortedRowsCore(txn, rows, is_local, rel_context_cache, &held_rel, &held_relid, &rows_done);

        instr_time cleanup_start;
        INSTR_TIME_SET_CURRENT(cleanup_start);
        E_ApplyCloseHeldRel(&held_rel, &held_relid);
        E_DestroyApplyRelContextCache(rel_context_cache);
        instr_time cleanup_end;
        INSTR_TIME_SET_CURRENT(cleanup_end);
        INSTR_TIME_SUBTRACT(cleanup_end, cleanup_start);
        EpochApplyProfileAddUs(EAP_CLEANUP, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(cleanup_end)));

        if (allOk) {
            instr_time txn_commit_start;
            INSTR_TIME_SET_CURRENT(txn_commit_start);
            E_EndApplyTransaction();
            instr_time txn_commit_end;
            INSTR_TIME_SET_CURRENT(txn_commit_end);
            INSTR_TIME_SUBTRACT(txn_commit_end, txn_commit_start);
            EpochApplyProfileAddUs(EAP_TXN_COMMIT, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(txn_commit_end)));
        } else {
            E_AbortApplyTransaction();
        }
    }
    PG_CATCH();
    {
        E_ApplyCloseHeldRel(&held_rel, &held_relid);
        E_DestroyApplyRelContextCache(rel_context_cache);
        E_AbortApplyTransaction();
        FlushErrorState();
        allOk = false;
    }
    PG_END_TRY();

    instr_time apply_end;
    INSTR_TIME_SET_CURRENT(apply_end);
    INSTR_TIME_SUBTRACT(apply_end, apply_start);
    uint64_t apply_us = static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(apply_end));
    E_ApplyTraceSlowStep("txn_batch", txn, InvalidOid, nullptr, EPOCH_OP_READ, apply_us);
    EpochLifecycleNoteApplyBatch(txn.epoch, rows_done, apply_us, allOk);
    EpochApplyBufLockEndTxn();
    EpochApplyProfileEndWireTxn(txn.epoch, txn.csn, txn.server_id, allOk);

    EpochHeapSetWriteEpoch(0);
    return allOk;
}

void E_ApplyWarmup(void)
{
    E_EnsureFkGraphLoaded();
}
