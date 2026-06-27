//
// Created by hcj2003 on 10/7/25.
//

#include "occ/occ_mem.h"
#include "occ/o_txn.h"
#include "occ/occ_internal.h"
#include "epoch/e_exec_track.h"

#include "access/heapam.h"
#include "utils/rel.h"

O_TxnManager::O_TxnManager(bool global)
    : m_threadId((uint64_t)-1),
      m_occManager(),
      m_csn(INVALID_CSN),
      m_internalStmtCount(0),
      m_cachedRelCnt(0),
      m_shareCachedRelCnt(0),
      m_global(global)
{
}

O_TxnManager::~O_TxnManager()
{
    if (m_accessMgr)
    {
        m_accessMgr->~O_TxnAccess();
//        occ_free(m_accessMgr);
        m_accessMgr = nullptr;
    }
}

bool O_TxnManager::Init()
{
    //    m_threadId = threadId;

    //    m_accessMgr = MemAllocAlignedObject<TxnAccess>(L1_CACHE_LINE, m_global);

    void *buffer = occ_session_malloc(sizeof(O_TxnAccess));
//    void *buffer = palloc(sizeof(O_TxnAccess));
    if (!buffer)
    {
        OccLogMemoryAllocationFailure(__FUNCTION__, __FILE__, __LINE__, sizeof(O_TxnAccess));
        return false;
    }
    m_accessMgr = new (buffer) O_TxnAccess();

    if (!m_accessMgr->Init(this))
    {
        return false;
    }
    return true;
}

bool O_TxnInsertAction::Init(O_TxnManager *_manager)
{
    bool rc = true;
    if (this->m_manager)
        return false;

    this->m_manager = _manager;
    //    m_insertSetSize = 0;
    //    m_insertArraySize = INSERT_ARRAY_DEFAULT_SIZE;
    //
    ////    void* ptr = MemAlloc(sizeof(InsItem) * m_insertArraySize, m_manager->m_global);
    //    void* ptr = malloc(sizeof(InsItem) * m_insertArraySize);
    //    if (ptr == NULL)
    //        return false;
    //    m_insertSet = reinterpret_cast<InsItem*>(ptr);
    //    for (uint64_t item = 0; item < m_insertArraySize; item++) {
    //        new (&m_insertSet[item]) InsItem();
    //    }
    return rc;
}

O_TxnInsertAction::~O_TxnInsertAction()
{
}

void O_TxnInsertAction::AddInsertToLocalAccess(
    HeapTuple heap_tuple, occ_sentinel pIndexInsertResult)
{
    O_TxnAccess *accessMgr = m_manager->m_accessMgr;
    HeapTuple accessRow = nullptr;
    accessRow = accessMgr->AddInsertToLocalAccess(pIndexInsertResult, heap_tuple);
}

void O_TxnInsertAction::ExecuteOptimisticInsert(HeapTuple heap_tuple, occ_sentinel sentinel)
{
    AddInsertToLocalAccess(heap_tuple, sentinel);
}

HeapTuple O_TxnManager::RowLookup(const AccessType type, occ_sentinel const &originalSentinel, RC &rc, IndexScanDesc scan, HeapTuple heap_tuple)
{
    HeapTuple local_row = nullptr;

    if (scan && scan->xs_continue_hot)
    {
        Tuple tuple = IndexFetchTuple(scan, nullptr);
        return (HeapTuple)tuple;
    }

    RC res = AccessLookup(type, originalSentinel, local_row);

    switch (res)
    {
    case RC::RC_LOCAL_ROW_FOUND:
        break;
    case RC::RC_LOCAL_ROW_NOT_FOUND:
        /*
         * 纯读且写集未命中：不拷贝进 hash，避免 Payment 等「先 UPDATE 再 SELECT」
         * 对无关行重复 IndexFetchTuple + heap_copytuple（off-CPU 主要热点）。
         */
        if (type == AccessType::RD && !g_enable_occ_delta) {
            if (scan != nullptr) {
                Tuple tuple = IndexFetchTuple(scan, nullptr);
                return (HeapTuple)tuple;
            }
            if (heap_tuple != nullptr) {
                return heap_tuple;
            }
            return nullptr;
        }
        local_row = m_accessMgr->MapRowtoLocalTable(type, originalSentinel, rc, scan, heap_tuple);
        break;
    case RC::RC_LOCAL_ROW_NOT_VISIBLE:
    case RC::RC_LOCAL_ROW_DELETED:
        return nullptr;
    case RC::RC_MEMORY_ALLOCATION_ERROR:
        rc = RC_MEMORY_ALLOCATION_ERROR;
        return nullptr;
    default:
        return nullptr;
    }

    if (local_row)
    {
        return local_row;
        //        return m_accessMgr->GetRowZeroCopyIfAny(local_row);
    }
    return nullptr;
}

bool O_TxnManager::CheckUnique(const AccessType type, occ_sentinel const &originalSentinel, RC &rc)
{
    HeapTuple local_row = nullptr;
    RC res = AccessLookup(type, originalSentinel, local_row);

    switch (res)
    {
    case RC::RC_LOCAL_ROW_NOT_VISIBLE:
    case RC::RC_LOCAL_ROW_DELETED:
        return true;
    default:
        return false;
    }

    return false;
}

RC O_TxnManager::AccessLookup(const AccessType type, occ_sentinel const &originalSentinel, HeapTuple &localRow)
{
    if (type == INS || type == WR || type == DEL) {
        EpochMaybeMarkTxnExecuting();
    }
    return m_accessMgr->AccessLookup(type, originalSentinel, localRow);
}

RC O_TxnManager::InsertRow(HeapTuple heap_tuple, occ_sentinel sentinel)
{
    EpochMaybeMarkTxnExecuting();
    RC rc;
    m_accessMgr->GetInsertMgr()->ExecuteOptimisticInsert(heap_tuple, sentinel);
    return rc;
}

RC O_TxnManager::UpdateLastRowState(AccessType state, bool update_index)
{
    if (state == WR || state == DEL) {
        EpochMaybeMarkTxnExecuting();
    }
    O_Access *ac = m_accessMgr->GetLastAccess();
    if (ac == nullptr) {
        return RC_ERROR;
    }
    return m_accessMgr->UpdateRowState(state, ac, update_index);
}

RC O_TxnManager::OverwriteRow(HeapTuple updatedRow)
{
    if (updatedRow == nullptr)
    {
        return RC_ERROR;
    }

    O_Access *access = m_accessMgr->GetLastAccess();

    access->m_stmtCount = GetStmtCount();
    return RC_OK;
}

HeapTuple O_TxnManager::GetLastAccessedDraft()
{
    return m_accessMgr->GetLastAccessedDraft();
}

void O_TxnManager::RecordCommit()
{
    CommitInternal();
}

void O_TxnManager::ReleaseOccLocks()
{
    m_occManager.ReleaseLocks(this);
}

bool O_TxnManager::RevalidateWriteSet()
{
    m_occManager.SetActiveTxn(this);
    bool ok = m_occManager.ValidateWriteSet(this);
    m_occManager.SetActiveTxn(nullptr);
    return ok;
}

void O_TxnManager::CommitInternal()
{
    m_occManager.WriteChanges(this);
    m_occManager.ReleaseLocks(this);
}

RC O_TxnManager::DeleteLastRow()
{
    EpochMaybeMarkTxnExecuting();
    RC rc;
    O_Access *access = m_accessMgr->GetLastAccess();
    if (access == nullptr)
        return RC_ERROR;

    rc = m_accessMgr->UpdateRowState(AccessType::DEL, access, false);

    if (rc != RC_OK)
        return rc;
    return rc;
}

RC O_TxnManager::ValidateCommit()
{
    RC res = RC_OK;
    if (res == RC_OK)
    {
        res = m_occManager.ValidateOcc(this);
    }

    return res;
}

void O_TxnManager::EndTransaction()
{
    m_occManager.ReleaseLocks(this);
    Cleanup();
}

void O_TxnManager::RollbackInternal(bool isPrepared)
{
    m_occManager.ReleaseLocks(this);

    Cleanup();
}

inline void O_TxnManager::Cleanup()
{
    ReleaseCachedRelations();

    if (m_accessMgr != nullptr) {
        m_accessMgr->ClearSet();
    }

    m_csn = INVALID_CSN;
    m_internalStmtCount = 0;
    m_occManager.CleanUp();
    O_ResetOccTxnMemory();
    /* 保留 txn_manager 供会话内复用，勿置 NULL（旧版 palloc 方案会随上下文回收悬空） */
}

Relation O_TxnManager::GetValidationRelation(Oid relid, bool *needClose)
{
    *needClose = false;
    if (!OidIsValid(relid)) {
        return nullptr;
    }

    for (uint32_t i = 0; i < m_shareCachedRelCnt; ++i) {
        if (m_shareCachedRelids[i] == relid) {
            return m_shareCachedRels[i];
        }
    }

    Relation rel = try_relation_open(relid, AccessShareLock);
    if (rel == nullptr) {
        return nullptr;
    }

    if (m_shareCachedRelCnt < OCC_REL_CACHE_MAX) {
        m_shareCachedRelids[m_shareCachedRelCnt] = relid;
        m_shareCachedRels[m_shareCachedRelCnt] = rel;
        m_shareCachedRelCnt++;
        return rel;
    }

    *needClose = true;
    return rel;
}

Relation O_TxnManager::GetCachedRelation(Oid relid, Relation hint, bool *needClose)
{
    (void)hint;
    *needClose = false;
    if (!OidIsValid(relid)) {
        return nullptr;
    }

    for (uint32_t i = 0; i < m_cachedRelCnt; ++i) {
        if (m_cachedRelids[i] == relid) {
            return m_cachedRels[i];
        }
    }

    if (m_cachedRelCnt < OCC_REL_CACHE_MAX) {
        Relation rel = heap_open(relid, RowExclusiveLock);
        m_cachedRelids[m_cachedRelCnt] = relid;
        m_cachedRels[m_cachedRelCnt] = rel;
        m_cachedRelCnt++;
        return rel;
    }

    *needClose = true;
    return heap_open(relid, RowExclusiveLock);
}

void O_TxnManager::ReleaseCachedRelations()
{
    for (uint32_t i = 0; i < m_shareCachedRelCnt; ++i) {
        if (m_shareCachedRels[i] != nullptr) {
            heap_close(m_shareCachedRels[i], AccessShareLock);
            m_shareCachedRels[i] = nullptr;
            m_shareCachedRelids[i] = InvalidOid;
        }
    }
    m_shareCachedRelCnt = 0;

    for (uint32_t i = 0; i < m_cachedRelCnt; ++i) {
        if (m_cachedRels[i] != nullptr) {
            heap_close(m_cachedRels[i], RowExclusiveLock);
            m_cachedRels[i] = nullptr;
            m_cachedRelids[i] = InvalidOid;
        }
    }
    m_cachedRelCnt = 0;
}
