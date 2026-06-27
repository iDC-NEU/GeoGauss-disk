//
// Created by hcj2003 on 10/7/25.
//

#include "occ/o_txn.h"
#include "occ/occ_mem.h"
#include "occ/occ_internal.h"
#include "occ/o_delta_table.h"
#include "occ/o_staging.h"

static FORCE_INLINE
    HeapTuple
    O_heap_copytuple_impl(HeapTuple tuple)
{
    HeapTuple newTuple;
    errno_t rc = EOK;

    if (!HeapTupleIsValid(tuple) || tuple->t_data == NULL)
    {
        return NULL;
    }

    Assert(!HEAP_TUPLE_IS_COMPRESSED(tuple->t_data));

    newTuple = (HeapTuple)occ_malloc(HEAPTUPLESIZE + tuple->t_len);
//    newTuple = (HeapTuple)palloc(HEAPTUPLESIZE + tuple->t_len);
    newTuple->tupTableType = HEAP_TUPLE;
    newTuple->t_len = tuple->t_len;
    newTuple->t_self = tuple->t_self;
    newTuple->t_tableOid = tuple->t_tableOid;
    newTuple->t_bucketId = tuple->t_bucketId;
    HeapTupleCopyBase(newTuple, tuple);

    newTuple->t_data = (HeapTupleHeader)((char *)newTuple + HEAPTUPLESIZE);
    rc = memcpy_s((char *)newTuple->t_data, tuple->t_len, (char *)tuple->t_data, tuple->t_len);
    securec_check(rc, "\0", "\0");
    return newTuple;
}

//static FORCE_INLINE
//    HeapTuple
//    O_heap_form_tuple_impl(TupleDesc tupleDescriptor, Datum *values, bool *isnull)
//{
//    HeapTuple tuple;    /* return tuple */
//    HeapTupleHeader td; /* tuple data */
//    Size len, data_len;
//    int hoff;
//    bool hasnull = false;
//    FormData_pg_attribute *att = tupleDescriptor->attrs;
//    int numberOfAttributes = tupleDescriptor->natts;
//    int i;
//
//    for (i = 0; i < numberOfAttributes; i++)
//    {
//        if (isnull[i])
//        {
//            hasnull = true;
//        }
//    }
//
//    len = offsetof(HeapTupleHeaderData, t_bits);
//
//    if (hasnull)
//    {
//        len += BITMAPLEN(numberOfAttributes);
//    }
//
//    if (tupleDescriptor->tdhasoid)
//    {
//        len += sizeof(Oid);
//    }
//
//    hoff = len = MAXALIGN(len); /* align user data safely */
//
//    data_len = heap_compute_data_size(tupleDescriptor, values, isnull);
//
//    len += data_len;
//
//    /*
//     * Allocate and zero the space needed.	Note that the tuple body and
//     * HeapTupleData management structure are allocated in one chunk.
//     */
//    Size allocSize = HEAPTUPLESIZE + len;
//    if (tupleDescriptor->tdhasuids)
//    { /* prealloc 8 bytes */
//        allocSize = HEAPTUPLESIZE + MAXALIGN(hoff + sizeof(uint64)) + data_len;
//    }
//    tuple = (HeapTuple)occ_malloc(allocSize);
//    tuple->t_data = td = (HeapTupleHeader)((char *)tuple + HEAPTUPLESIZE);
//
//    /*
//     * And fill in the information.  Note we fill the Datum fields even though
//     * this tuple may never become a Datum.
//     */
//    tuple->t_len = len;
//    ItemPointerSetInvalid(&(tuple->t_self));
//    tuple->t_tableOid = InvalidOid;
//    tuple->t_bucketId = InvalidBktId;
//    HeapTupleSetZeroBase(tuple);
//#ifdef PGXC
//    tuple->t_xc_node_id = 0;
//#endif
//
//    HeapTupleHeaderSetDatumLength(td, len);
//    HeapTupleHeaderSetTypeId(td, tupleDescriptor->tdtypeid);
//    HeapTupleHeaderSetTypMod(td, tupleDescriptor->tdtypmod);
//
//    HeapTupleHeaderSetNatts(td, numberOfAttributes);
//    td->t_hoff = hoff;
//
//    /* else leave infomask = 0 */
//    if (tupleDescriptor->tdhasoid)
//    {
//        td->t_infomask = HEAP_HASOID;
//    }
//
//    td->t_infomask &= ~HEAP_UID_MASK;
//
//    heap_fill_tuple(tupleDescriptor, values, isnull, (char *)td + hoff, data_len, &td->t_infomask,
//                    (hasnull ? td->t_bits : NULL));
//
//    return tuple;
//}

O_TxnAccess::O_TxnAccess()
    : m_writeSetCnt(0),
      m_rowCnt(0),
      m_accessesSetBuff(nullptr),
      m_txnManager(nullptr),
      m_insertManager(nullptr),
      m_lastAcc(nullptr)
{
    for (uint32_t i = 0; i < OCC_WRITE_SET_MAX; ++i) {
        m_writeSet[i].occupied = false;
        m_writeSet[i].access = nullptr;
    }
}

O_TxnAccess::~O_TxnAccess()
{
    if (m_insertManager)
    {
        delete m_insertManager;
    }

    m_txnManager = nullptr;
    m_insertManager = nullptr;
    m_accessesSetBuff = nullptr;
    m_lastAcc = nullptr;
}

bool O_TxnAccess::WriteSetInsert(const occ_sentinel &key, O_Access *access)
{
    occ_sentinel storeKey = key;
    if (access != nullptr) {
        Oid relid = OccResolveAccessRelid(access, &key);
        if (OidIsValid(relid)) {
            access->m_origSentinel.relid = relid;
            storeKey.relid = relid;
        }
        ItemPointerData tid = OccResolveAccessTid(access, &key);
        if (OccTidIsPlausible(&tid)) {
            access->m_origSentinel.tuple_id = tid;
            storeKey.tuple_id = tid;
        }
    }

    O_Access *existing = WriteSetFind(storeKey);
    if (existing != nullptr) {
        return true;
    }

    for (uint32_t i = 0; i < OCC_WRITE_SET_MAX; ++i) {
        if (!m_writeSet[i].occupied) {
            m_writeSet[i].key = storeKey;
            m_writeSet[i].access = access;
            m_writeSet[i].occupied = true;
            m_writeSetCnt++;
            return true;
        }
    }
    return false;
}

O_Access *O_TxnAccess::WriteSetFind(const occ_sentinel &key) const
{
    for (uint32_t i = 0; i < OCC_WRITE_SET_MAX; ++i) {
        if (m_writeSet[i].occupied && OccSentinelEqual::Match(m_writeSet[i].key, key)) {
            return m_writeSet[i].access;
        }
    }
    return nullptr;
}

bool O_TxnAccess::WriteSetErase(const occ_sentinel &key)
{
    for (uint32_t i = 0; i < OCC_WRITE_SET_MAX; ++i) {
        if (m_writeSet[i].occupied && OccSentinelEqual::Match(m_writeSet[i].key, key)) {
            m_writeSet[i].occupied = false;
            m_writeSet[i].access = nullptr;
            uint32_t cnt = 0;
            for (uint32_t j = 0; j < OCC_WRITE_SET_MAX; ++j) {
                if (m_writeSet[j].occupied) {
                    cnt++;
                }
            }
            m_writeSetCnt = cnt;
            return true;
        }
    }
    return false;
}

void O_TxnAccess::WriteSetRemoveReadEntries()
{
    for (uint32_t i = 0; i < OCC_WRITE_SET_MAX; ++i) {
        if (!m_writeSet[i].occupied) {
            continue;
        }
        O_Access *ac = m_writeSet[i].access;
        if (ac == nullptr) {
            continue;
        }
        if (ac->m_type == RD || ac->m_type == RD_FOR_UPDATE) {
            m_writeSet[i].occupied = false;
            m_writeSet[i].access = nullptr;
        }
    }

    uint32_t cnt = 0;
    for (uint32_t i = 0; i < OCC_WRITE_SET_MAX; ++i) {
        if (m_writeSet[i].occupied) {
            cnt++;
        }
    }
    m_writeSetCnt = cnt;
}

void O_TxnAccess::WriteSetClear()
{
    for (uint32_t i = 0; i < OCC_WRITE_SET_MAX; ++i) {
        m_writeSet[i].occupied = false;
        m_writeSet[i].access = nullptr;
    }
    m_writeSetCnt = 0;
}

bool O_TxnAccess::Init(O_TxnManager *manager)
{
    m_txnManager = manager;

    uint32_t alloc_size = sizeof(O_Access *) * m_accessSetSize;
    void *ptr = occ_session_malloc(alloc_size);
//    void *ptr = palloc(alloc_size);
//    void* ptr = MemAlloc(alloc_size, m_txnManager->m_global);
    if (!ptr)
    {
        OccLogMemoryAllocationFailure(__FUNCTION__, __FILE__, __LINE__, alloc_size);
        return false;
    }

    m_accessesSetBuff = static_cast<O_Access **>(ptr);
    for (uint64_t idx = 0; idx < m_accessSetSize; idx++)
    {
        m_accessesSetBuff[idx] = nullptr;
    }

    m_insertManager = new (std::nothrow) O_TxnInsertAction();
    if (!m_insertManager)
    {
        OccLogMemoryAllocationFailure(__FUNCTION__, __FILE__, __LINE__, sizeof(O_TxnInsertAction));
        free(ptr);
        return false;
    }
    m_insertManager->Init(manager);
    WriteSetClear();
    return true;
}

HeapTuple O_TxnAccess::AddInsertToLocalAccess(occ_sentinel org_sentinel, HeapTuple heap_tuple)
{
    O_Access *curr_access = nullptr;

    if (WriteSetFind(org_sentinel) == nullptr)
    {
        curr_access = GetNewInsertAccess(org_sentinel, heap_tuple);
        curr_access->m_origSentinel = org_sentinel;

        WriteSetInsert(org_sentinel, curr_access);
        return curr_access->GetTxnRow();
    }
    return nullptr;
}

O_Access *O_TxnAccess::GetNewInsertAccess(occ_sentinel const &sentinel, HeapTuple heap_tuple)
{
    O_Access *ac = GetAccessPtr(m_rowCnt);
    if (unlikely(ac == nullptr))
    {
        CreateNewAccess(INS);
        ac = GetAccessPtr(m_rowCnt);
    }

    ac->m_localInsertRow = O_heap_copytuple_impl(heap_tuple);
    ac->m_type = INS;
    ac->m_csn = heap_tuple->t_data->m_rowCSN;
    ac->m_origSentinel = sentinel;
    if (!OidIsValid(ac->m_origSentinel.relid)) {
        ac->m_origSentinel.relid = OccResolveRelid(sentinel);
    }
    if (!OidIsValid(ac->m_origSentinel.relid) && OidIsValid(heap_tuple->t_tableOid)) {
        ac->m_origSentinel.relid = heap_tuple->t_tableOid;
    }
    if (!OccTidIsPlausible(&ac->m_origSentinel.tuple_id) && OccTidIsPlausible(&heap_tuple->t_self)) {
        ac->m_origSentinel.tuple_id = heap_tuple->t_self;
    }
    m_rowCnt++;
    return ac;
}

RC O_TxnAccess::AccessLookup(const AccessType type, occ_sentinel const originalSentinel, HeapTuple &r_local_Row)
{
    if (m_writeSetCnt == 0)
    {
        return RC::RC_LOCAL_ROW_NOT_FOUND;
    }

    O_Access *curr_acc = RowLookup(originalSentinel);

    return LookupFilterRow(curr_acc, type, r_local_Row);
}

O_Access *O_TxnAccess::RowLookup(occ_sentinel const currentKey)
{
    return WriteSetFind(currentKey);
}

bool O_TxnAccess::EnsureRowAccessForModify(Relation rel, ItemPointer tid, HeapTuple oldHeapTuple)
{
    if (rel == nullptr || tid == nullptr || oldHeapTuple == nullptr || oldHeapTuple->t_data == nullptr) {
        return false;
    }

    occ_sentinel sentinel(rel, *tid);
    O_Access *ac = RowLookup(sentinel);
    if (ac != nullptr) {
        SetLastAccess(ac);
        return true;
    }

    HeapTuple row = O_heap_copytuple_impl(oldHeapTuple);
    if (row == nullptr) {
        return false;
    }

    ac = GetAccessPtr(m_rowCnt);
    if (unlikely(ac == nullptr)) {
        CreateNewAccess(RD_FOR_UPDATE);
        ac = GetAccessPtr(m_rowCnt);
    }
    if (ac == nullptr) {
        return false;
    }

    ac->m_origSentinel = sentinel;
    ac->m_globalRow = row;
    ac->m_localRow = row;
    ac->m_csn = row->t_data->m_rowCSN;
    ac->m_type = RD_FOR_UPDATE;
    ac->m_stmtCount = m_txnManager->GetStmtCount();

    WriteSetInsert(sentinel, ac);
    m_rowCnt++;
    SetLastAccess(ac);
    return true;
}

RC O_TxnAccess::LookupFilterRow(O_Access *curr_acc, const AccessType type, HeapTuple &r_local_Row)
{
    r_local_Row = nullptr;
    if (curr_acc != nullptr)
    {
        switch (curr_acc->m_type)
        {
        case AccessType::RD:
            // Cached row is not valid remove it!!
            if (curr_acc->m_stmtCount != m_txnManager->GetStmtCount())
            {
                WriteSetErase(curr_acc->m_origSentinel);
                return RC::RC_LOCAL_ROW_NOT_FOUND;
            }
            break;
        case AccessType::RD_FOR_UPDATE:
        case AccessType::WR:
            if (curr_acc->update_index)
                return RC::RC_LOCAL_ROW_NOT_VISIBLE;
            break;
        case AccessType::DEL:
            return RC::RC_LOCAL_ROW_DELETED;
        case AccessType::INS:
            if (curr_acc->update_index)
                return RC::RC_LOCAL_ROW_NOT_VISIBLE;
            break;
        }
    }
    else
    {
        return RC::RC_LOCAL_ROW_NOT_FOUND;
    }

    if (curr_acc->old_version)
    {
        r_local_Row = nullptr;
    }
    else
        r_local_Row = curr_acc->GetTxnRow();
    SetLastAccess(curr_acc);
    return RC::RC_LOCAL_ROW_FOUND;
}

HeapTuple O_TxnAccess::MapRowtoLocalTable(const AccessType type, occ_sentinel const &originalSentinel, RC &rc, IndexScanDesc scan, HeapTuple heap_tuple)
{
    O_Access *current_access = nullptr;
    rc = RC_OK;

    current_access = GetNewRowAccess(originalSentinel, type, rc, scan, heap_tuple);

    if (current_access == nullptr)
        return nullptr;

    current_access->m_type = (type != RD_FOR_UPDATE) ? RD : RD_FOR_UPDATE;

    WriteSetInsert(originalSentinel, current_access);

    SetLastAccess(current_access);
    current_access->m_stmtCount = m_txnManager->GetStmtCount();
    return current_access->GetGlobalVersion();
}

O_Access *O_TxnAccess::GetNewRowAccess(occ_sentinel const &originalSentinel, AccessType type, RC &rc, IndexScanDesc scan, HeapTuple heap_tuple)
{
    rc = RC_OK;
    HeapTuple row;
    Tuple tuple;

    if (OccStagingOverlayEnabled())
    {
        HeapTuple staged = nullptr;
        bool deleted = false;
        if (OccStagingTryReadTuple(originalSentinel.relation,
                                   const_cast<ItemPointer>(&originalSentinel.tuple_id),
                                   &staged, &deleted))
        {
            if (deleted || staged == nullptr) {
                if (staged != nullptr) {
                    heap_freetuple(staged);
                }
                return nullptr;
            }
            row = staged;
        }
        else if (g_enable_occ_delta && !g_enable_occ_staging)
        {
            O_DeltaEntry deltaEntry;
            bool hasDelta = O_DeltaTable::Instance().Lookup(
                originalSentinel.relation,
                const_cast<ItemPointer>(&originalSentinel.tuple_id),
                deltaEntry);

            if (hasDelta)
            {
                if (deltaEntry.deleted) {
                    return nullptr;
                }
                row = O_DeltaTable::Instance().LookupCopyTuple(
                    originalSentinel.relation,
                    const_cast<ItemPointer>(&originalSentinel.tuple_id));
                if (row == nullptr) {
                    return nullptr;
                }
            }
            else
            {
                if (scan)
                {
                    tuple = IndexFetchTuple(scan, nullptr);
                    row = O_heap_copytuple_impl((HeapTuple)tuple);
                }
                else
                {
                    row = O_heap_copytuple_impl(heap_tuple);
                }
            }
        }
        else
        {
            if (scan)
            {
                tuple = IndexFetchTuple(scan, nullptr);
                row = O_heap_copytuple_impl((HeapTuple)tuple);
            }
            else
            {
                row = O_heap_copytuple_impl(heap_tuple);
            }
        }
    }
    else if (g_enable_occ_delta)
    {
        /* delta 模式：先读 delta，失败再回退 heap */
        O_DeltaEntry deltaEntry;
        bool hasDelta = O_DeltaTable::Instance().Lookup(
            originalSentinel.relation,
            const_cast<ItemPointer>(&originalSentinel.tuple_id),
            deltaEntry);

        if (hasDelta)
        {
            if (deltaEntry.deleted || deltaEntry.tuple == nullptr)
            {
                return nullptr;
            }
            row = O_DeltaTable::Instance().LookupCopyTuple(
                originalSentinel.relation,
                const_cast<ItemPointer>(&originalSentinel.tuple_id));
            if (row == nullptr) {
                return nullptr;
            }
        }
        else
        {
            if (scan)
            {
                tuple = IndexFetchTuple(scan, nullptr);
                row = O_heap_copytuple_impl((HeapTuple)tuple);
            }
            else
            {
                row = O_heap_copytuple_impl(heap_tuple);
            }
        }
    }
    else
    {
        /* heap 模式：行为与最初版本一致，只从 heap 建立本地拷贝 */
        if (scan)
        {
            tuple = IndexFetchTuple(scan, nullptr);
            row = O_heap_copytuple_impl((HeapTuple)tuple);
        }
        else
        {
            row = O_heap_copytuple_impl(heap_tuple);
        }
    }

    if (row == nullptr)
    {
        return nullptr;
    }

    O_Access *ac = GetAccessPtr(m_rowCnt);
    if (unlikely(ac == nullptr))
    {
        CreateNewAccess(type);
        ac = GetAccessPtr(m_rowCnt);
    }

    ac->m_origSentinel.relation = originalSentinel.relation;
    ac->m_origSentinel.snapshot = originalSentinel.snapshot;
    ac->m_origSentinel.relid = OccResolveRelid(originalSentinel);
    ac->m_globalRow = row;
    if (!OidIsValid(ac->m_origSentinel.relid) && OidIsValid(row->t_tableOid)) {
        ac->m_origSentinel.relid = row->t_tableOid;
    }
    ac->m_origSentinel.tuple_id = row->t_self;
    if (!OccTidIsPlausible(&ac->m_origSentinel.tuple_id)) {
        ItemPointerData tid = OccResolveAccessTid(ac, &originalSentinel);
        if (OccTidIsPlausible(&tid)) {
            ac->m_origSentinel.tuple_id = tid;
        }
    }

    ac->m_type = type;

    if (scan)
    {
        ac->m_csn = scan->xs_rowcsn;
    }
    else
    {
        ac->m_csn = heap_tuple->t_data->m_rowCSN;
    }

    m_rowCnt++;
    return ac;
}

enum NS_ACTIONS : uint32_t
{
    NOCHANGE,
    GENERATE_DRAFT,
    FILTER_DELETES,
    NS_ERROR,
    INS_WR
};

typedef struct
{
    uint32_t nextState;
    uint32_t action;
} TxnStateMachineEntry;

static const TxnStateMachineEntry txnStateMachine[O_TxnAccess::TSM_SIZE][O_TxnAccess::TSM_SIZE] = {
    /* INVALID STATE */
    {{INV, NS_ACTIONS::NS_ERROR},           // INV
     {RD, NS_ACTIONS::NOCHANGE},            // RD
     {RD_FOR_UPDATE, NS_ACTIONS::NOCHANGE}, // RD_FOT_UPDATE
     {WR, NS_ACTIONS::GENERATE_DRAFT},      // WR
     {DEL, NS_ACTIONS::GENERATE_DRAFT},     // DEL
     {INS, NS_ACTIONS::NOCHANGE}},          // INS
    /* READ STATE */
    {{RD, NS_ACTIONS::NS_ERROR},
     {RD, NS_ACTIONS::NOCHANGE},
     {RD_FOR_UPDATE, NS_ACTIONS::NOCHANGE},
     {WR, NS_ACTIONS::GENERATE_DRAFT},
     {DEL, NS_ACTIONS::GENERATE_DRAFT},
     {RD, NS_ACTIONS::NS_ERROR}},
    /* READ_FOR_UPDATE STATE */
    {{RD_FOR_UPDATE, NS_ACTIONS::NS_ERROR},
     {RD_FOR_UPDATE, NS_ACTIONS::NOCHANGE},
     {RD_FOR_UPDATE, NS_ACTIONS::GENERATE_DRAFT},
     {WR, NS_ACTIONS::GENERATE_DRAFT},
     {DEL, NS_ACTIONS::GENERATE_DRAFT},
     {RD_FOR_UPDATE, NS_ACTIONS::NS_ERROR}},
    /* WRITE STATE */
    {{WR, NS_ACTIONS::NS_ERROR},
     {WR, NS_ACTIONS::NOCHANGE},
     {WR, NS_ACTIONS::NOCHANGE},
     {WR, NS_ACTIONS::NOCHANGE},
     {DEL, NS_ACTIONS::GENERATE_DRAFT},
     {WR, NS_ACTIONS::NS_ERROR}},
    /* DELETE STATE */
    {{DEL, NS_ACTIONS::NS_ERROR},
     {DEL, NS_ACTIONS::NOCHANGE},
     {DEL, NS_ACTIONS::NOCHANGE},
     {DEL, NS_ACTIONS::NS_ERROR},
     {DEL, NS_ACTIONS::NS_ERROR},
     {INS, NS_ACTIONS::NOCHANGE}},
    /* INSERT STATE */
    {{INS, NS_ACTIONS::NS_ERROR},
     {INS, NS_ACTIONS::NOCHANGE},
     {INS, NS_ACTIONS::NOCHANGE},
     {INS, NS_ACTIONS::INS_WR},
     {DEL, NS_ACTIONS::FILTER_DELETES},
     {INS, NS_ACTIONS::NS_ERROR}},
};

RC O_TxnAccess::UpdateRowState(AccessType type, O_Access *ac, bool update_index)
{
    if (ac == nullptr) {
        return RC_ERROR;
    }

    // 状态转换表
    AccessType currentState = ac->m_type;
    auto result = txnStateMachine[currentState][type];

    RC rc = RC_OK;
    AccessType currentType = ac->m_type;
    ac->m_type = (AccessType)result.nextState;
    NS_ACTIONS action = (NS_ACTIONS)result.action;

    if (update_index)
    {
        ac->update_index = update_index;
        ac->old_version = true;
    }

    switch (action)
    {
    case NS_ACTIONS::INS_WR:
        ac->update_ins = true;
    case NS_ACTIONS::GENERATE_DRAFT:
        rc = GenerateDraft(ac, currentType);
        if (rc != RC_OK)
        {
            break;
        }
        break;
    }

    return rc;
}

RC O_TxnAccess::GenerateDraft(O_Access *ac, AccessType acType)
{
    RC rc = RC_OK;
    switch (ac->m_type)
    {
    case DEL:
    case WR:
        return CreateMVCCDraft(ac, acType);
    case RD_FOR_UPDATE:
    default:
        break;
    }
    return rc;
}

RC O_TxnAccess::CreateMVCCDraft(O_Access *ac, AccessType acType)
{
    if (ac->m_localRow != nullptr) {
        return RC_OK;
    }

    HeapTuple globalRow = ac->GetGlobalVersion();
    if (globalRow == nullptr) {
        return RC_ERROR;
    }

    HeapTuple newRow = O_heap_copytuple_impl(globalRow);
    if (newRow == nullptr) {
        return RC_ERROR;
    }

    ac->m_localRow = newRow;
    return RC_OK;
}

void O_TxnAccess::ClearSet()
{
    m_lastAcc = nullptr;
    WriteSetClear();

    /*
     * Do not run ~O_Access() here: txn-scoped objects live in the OCC arena and
     * are bulk-reclaimed by O_ResetOccTxnMemory() in O_TxnManager::Cleanup().
     */
    for (unsigned int i = 0; i < m_rowCnt; i++) {
        m_accessesSetBuff[i] = nullptr;
    }
    m_rowCnt = 0;

    m_insertManager->ClearSet();
}

void O_TxnAccess::DestroyAccess(O_Access *access)
{

}

bool O_TxnAccess::CreateNewAccess(AccessType type)
{
    if (unlikely(m_rowCnt >= (m_accessSetSize - 1)))
    {
        bool rc = ReallocAccessSet();
    }
    m_accessesSetBuff[m_rowCnt] = AllocNewAccess(m_rowCnt);
    return true;
}

bool O_TxnAccess::ReallocAccessSet()
{
    bool rc = true;
    uint64_t new_array_size = m_accessSetSize * ACCESS_SET_EXTEND_FACTOR;

    uint64_t alloc_size = sizeof(O_Access *) * new_array_size;
    void *ptr = occ_session_malloc(alloc_size);

    errno_t erc = memset_s(ptr, sizeof(O_Access *) * new_array_size, 0, sizeof(O_Access *) * new_array_size);
    securec_check(erc, "\0", "\0");
    erc = memcpy_s(ptr, sizeof(O_Access *) * new_array_size, m_accessesSetBuff, sizeof(O_Access *) * m_accessSetSize);
    securec_check(erc, "\0", "\0");
    occ_session_free(m_accessesSetBuff);
    //    MemFree(m_accessesSetBuff, m_txnManager->m_global);
    SetAccessesSet(reinterpret_cast<O_Access **>(ptr));
    m_accessSetSize = new_array_size;
}
