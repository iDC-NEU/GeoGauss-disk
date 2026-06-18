//
// Created by hcj2003 on 10/7/25.
//

#ifndef OPENGAUSS_O_TXN_H
#define OPENGAUSS_O_TXN_H

#include "o_txn_access.h"
#include "o_occManager.h"

class O_TxnManager {
public:
    ~O_TxnManager();

    explicit O_TxnManager(bool global = false);

    bool Init();

public:
    // use global or local memory allocators
    bool m_global;

    inline uint32_t GetStmtCount() const
    {
        return m_internalStmtCount;
    }

    inline uint64_t GetCommitSequenceNumber() const
    {
        return m_csn;
    }

    inline void SetCommitSequenceNumber(uint64_t commitSequenceNumber)
    {
        m_csn = commitSequenceNumber;
    }

    HeapTuple RowLookup(const AccessType type, occ_sentinel const& originalSentinel,RC& rc, IndexScanDesc scan, HeapTuple heap_tuple = nullptr);

    bool CheckUnique(const AccessType type, occ_sentinel const& originalSentinel, RC& rc);

    RC AccessLookup(const AccessType type, occ_sentinel const& originalSentinel, HeapTuple& localRow);

    RC InsertRow(HeapTuple heap_tuple, occ_sentinel sentinel);

    RC UpdateLastRowState(AccessType state, bool update_index);

    RC OverwriteRow(HeapTuple updatedRow);

    HeapTuple GetLastAccessedDraft();

    void RecordCommit();

    RC DeleteLastRow();

    RC ValidateCommit();

    inline uint64_t GetThdId() const
    {
        return m_threadId;
    }

    void EndTransaction();

    inline void Rollback()
    {
        RollbackInternal(false);
    }

    inline void Cleanup();

    Relation GetCachedRelation(Oid relid, Relation hint, bool *needClose);
    void ReleaseCachedRelations();

private:
    static constexpr uint32_t OCC_REL_CACHE_MAX = 16;

    uint32_t m_internalStmtCount;

    Oid m_cachedRelids[OCC_REL_CACHE_MAX];
    Relation m_cachedRels[OCC_REL_CACHE_MAX];
    uint32_t m_cachedRelCnt;

    void CommitInternal();

    void RollbackInternal(bool isPrepared);

    OccTransactionManager m_occManager;

    uint64_t m_threadId;

    uint64_t m_csn;

public:
    /** @var Transaction cache (OCC optimization). */
    O_TxnAccess* m_accessMgr;
};

#endif  // OPENGAUSS_O_TXN_H
