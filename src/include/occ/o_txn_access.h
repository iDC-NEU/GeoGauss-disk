//
// Created by hcj2003 on 10/7/25.
//

#ifndef OPENGAUSS_O_TXN_ACCESS_H
#define OPENGAUSS_O_TXN_ACCESS_H

#include <unordered_map>
#include "occ/o_txn_insert_action.h"
#include "occ/o_sentinel.h"
#include "occ/o_access.h"
#include "occ/occ_mem.h"
class O_TxnManager;

struct OccSentinelHash
{
    size_t operator()(const occ_sentinel &s) const noexcept
    {
        return (size_t)sentinel_hash(s.relid, &s.tuple_id);
    }
};

struct OccSentinelEqual
{
    bool operator()(const occ_sentinel &a, const occ_sentinel &b) const noexcept
    {
        return a.relid == b.relid &&
               a.tuple_id.ip_blkid.bi_hi == b.tuple_id.ip_blkid.bi_hi &&
               a.tuple_id.ip_blkid.bi_lo == b.tuple_id.ip_blkid.bi_lo &&
               a.tuple_id.ip_posid == b.tuple_id.ip_posid;
    }
};

typedef std::unordered_map<occ_sentinel, O_Access *, OccSentinelHash, OccSentinelEqual> O_TxnOrderedSet_t;

typedef std::pair<occ_sentinel, O_Access *> O_RowAccessPair_t;

class O_TxnAccess
{
public:
    O_TxnAccess();

    ~O_TxnAccess();

    bool Init(O_TxnManager *manager);

    O_TxnInsertAction *GetInsertMgr()
    {
        return m_insertManager;
    }

    inline O_Access *GetAccessPtr(uint64_t index)
    {
        return m_accessesSetBuff[index];
    }

    bool ReallocAccessSet();

    inline O_TxnOrderedSet_t &GetOrderedRowSet()
    {
        return *m_rowsSet;
    }

    inline uint32_t m_Size() const
    {
        return m_rowCnt;
    }

    HeapTuple MapRowtoLocalTable(const AccessType type, occ_sentinel const &originalSentinel, RC &rc, IndexScanDesc scan, HeapTuple heap_tuple = nullptr);

    bool CreateNewAccess(AccessType type);

    O_Access *AllocNewAccess(uint32_t rowCnt)
    {
        void *buffer = occ_malloc(sizeof(O_Access));
//        void *buffer = palloc(sizeof(O_Access));
        O_Access *ac = new (buffer) O_Access();
        return ac;
    }

    O_Access *GetNewInsertAccess(occ_sentinel const &sentinel, HeapTuple heap_tuple);

    O_Access *GetNewRowAccess(occ_sentinel const &originalSentinel, AccessType type, RC &rc, IndexScanDesc scan, HeapTuple heap_tuple = nullptr);

    HeapTuple AddInsertToLocalAccess(occ_sentinel org_sentinel, HeapTuple heap_tuple);

    RC AccessLookup(const AccessType type, occ_sentinel const originalSentinel, HeapTuple &r_local_Row);

    O_Access *RowLookup(occ_sentinel const currentKey);

    RC UpdateRowState(AccessType type, O_Access *ac, bool update_index);

    static constexpr uint8_t TSM_SIZE = 6;

    RC GenerateDraft(O_Access *ac, AccessType acType);

    RC CreateMVCCDraft(O_Access *ac, AccessType acType);

    inline O_Access *GetLastAccess()
    {
        return m_lastAcc;
    }

    inline HeapTuple GetLastAccessedDraft()
    {
        return m_lastAcc->GetLocalVersion();
    }

    inline EState *GetLastAccessedEstate()
    {
        return m_lastAcc->GetEstate();
    }

    inline void SetLastAccessedEstate(EState *estate)
    {
        m_lastAcc->SetEstate(estate);
    }

    inline TupleTableSlot *GetLastAccessedSlot()
    {
        return m_lastAcc->GetSlot();
    }

    inline void SetLastAccessedSlot(TupleTableSlot *slot)
    {
        return m_lastAcc->SetSlot(slot);
    }

    void ClearSet(void);

    void DestroyAccess(O_Access *access);

    inline void SetAccessesSet(O_Access **set)
    {
        m_accessesSetBuff = set;
    }

private:
    static constexpr uint32_t DEFAULT_ACCESS_SIZE = 500;

    static constexpr uint32_t ACCESS_SET_EXTEND_FACTOR = 2;

    O_TxnOrderedSet_t *m_rowsSet = nullptr;

    O_TxnInsertAction *m_insertManager = nullptr;

    uint32_t m_accessSetSize = DEFAULT_ACCESS_SIZE;

    O_Access **m_accessesSetBuff = nullptr;

    uint32_t m_rowCnt = 0;

    O_Access *m_lastAcc = nullptr;

    O_TxnManager *m_txnManager = nullptr;

    inline void SetLastAccess(O_Access *acc)
    {
        m_lastAcc = acc;
    }

    RC LookupFilterRow(O_Access *curr_acc, const AccessType type, HeapTuple &r_local_Row);
};

#endif // OPENGAUSS_O_TXN_ACCESS_H
