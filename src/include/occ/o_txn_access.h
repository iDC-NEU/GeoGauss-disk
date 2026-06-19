//
// Created by hcj2003 on 10/7/25.
//

#ifndef OPENGAUSS_O_TXN_ACCESS_H
#define OPENGAUSS_O_TXN_ACCESS_H

#include "occ/o_txn_insert_action.h"
#include "occ/o_sentinel.h"
#include "occ/o_access.h"
#include "occ/occ_mem.h"

class O_TxnManager;

struct OccSentinelEqual
{
    static bool Match(const occ_sentinel &a, const occ_sentinel &b) noexcept
    {
        return a.relid == b.relid &&
               a.tuple_id.ip_blkid.bi_hi == b.tuple_id.ip_blkid.bi_hi &&
               a.tuple_id.ip_blkid.bi_lo == b.tuple_id.ip_blkid.bi_lo &&
               a.tuple_id.ip_posid == b.tuple_id.ip_posid;
    }
};

class O_TxnAccess
{
public:
    static constexpr uint32_t OCC_WRITE_SET_MAX = 32;

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

    inline uint32_t m_Size() const
    {
        return m_writeSetCnt;
    }

    template <typename Fn>
    void ForEachWriteEntry(Fn &&fn) const
    {
        for (uint32_t i = 0; i < OCC_WRITE_SET_MAX; ++i) {
            if (m_writeSet[i].occupied) {
                fn(m_writeSet[i].key, m_writeSet[i].access);
            }
        }
    }

    bool WriteSetInsert(const occ_sentinel &key, O_Access *access);
    O_Access *WriteSetFind(const occ_sentinel &key) const;
    bool WriteSetErase(const occ_sentinel &key);
    void WriteSetRemoveReadEntries();
    void WriteSetClear();

    HeapTuple MapRowtoLocalTable(const AccessType type, occ_sentinel const &originalSentinel, RC &rc, IndexScanDesc scan, HeapTuple heap_tuple = nullptr);

    bool CreateNewAccess(AccessType type);

    O_Access *AllocNewAccess(uint32_t rowCnt)
    {
        (void)rowCnt;
        void *buffer = occ_malloc(sizeof(O_Access));
        if (buffer == nullptr) {
            return nullptr;
        }
        errno_t rc = memset_s(buffer, sizeof(O_Access), 0, sizeof(O_Access));
        securec_check(rc, "\0", "\0");
        O_Access *ac = new (buffer) O_Access();
        return ac;
    }

    O_Access *GetNewInsertAccess(occ_sentinel const &sentinel, HeapTuple heap_tuple);

    O_Access *GetNewRowAccess(occ_sentinel const &originalSentinel, AccessType type, RC &rc, IndexScanDesc scan, HeapTuple heap_tuple = nullptr);

    HeapTuple AddInsertToLocalAccess(occ_sentinel org_sentinel, HeapTuple heap_tuple);

    RC AccessLookup(const AccessType type, occ_sentinel const originalSentinel, HeapTuple &r_local_Row);

    O_Access *RowLookup(occ_sentinel const currentKey);

    bool EnsureRowAccessForModify(Relation rel, ItemPointer tid, HeapTuple oldHeapTuple);

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

    inline void SetLastAccessedSlot(TupleTableSlot *slot, bool deepCopy = false)
    {
        return m_lastAcc->SetSlot(slot, deepCopy);
    }

    void ClearSet(void);

    void DestroyAccess(O_Access *access);

    inline void SetAccessesSet(O_Access **set)
    {
        m_accessesSetBuff = set;
    }

private:
    static constexpr uint32_t DEFAULT_ACCESS_SIZE = 64;

    static constexpr uint32_t ACCESS_SET_EXTEND_FACTOR = 2;

    struct WriteSetSlot {
        occ_sentinel key;
        O_Access *access;
        bool occupied;
    };

    WriteSetSlot m_writeSet[OCC_WRITE_SET_MAX];
    uint32_t m_writeSetCnt;

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
