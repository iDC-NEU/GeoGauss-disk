//
// Created by hcj2003 on 10/8/25.
//

#include "occ/o_txn.h"
#include "occ/o_delta_table.h"
#include "occ/occ_internal.h"

#include <algorithm>
#include <cstdint>
#include <unistd.h>
#include <vector>

/* 默认不启用 delta 覆盖模式 */
bool g_enable_occ_delta = false;

namespace {

struct SortedWriteEntry {
    uint64_t hash;
    O_Access* access;
    occ_sentinel sent;
};

static void CollectWriteSetByHash(O_TxnOrderedSet_t& orderedSet, std::vector<SortedWriteEntry>& out)
{
    out.clear();
    out.reserve(orderedSet.size());
    for (const auto& raPair : orderedSet) {
        O_Access* ac = raPair.second;
        occ_sentinel sent = ac->m_origSentinel;
        uint64_t h = sentinel_hash(sent.relid, &sent.tuple_id);
        out.push_back({h, ac, sent});
    }
    std::sort(out.begin(), out.end(),
              [](const SortedWriteEntry& a, const SortedWriteEntry& b) { return a.hash < b.hash; });
}

static uint32_t CountUniqueHashes(const std::vector<SortedWriteEntry>& entries)
{
    uint32_t count = 0;
    uint64_t prev = UINT64_MAX;
    for (const auto& e : entries) {
        if (e.hash != prev) {
            count++;
            prev = e.hash;
        }
    }
    return count;
}

static bool IsInsertOnlyWriteSet(const std::vector<SortedWriteEntry>& entries)
{
    if (entries.empty()) {
        return false;
    }
    for (const auto& e : entries) {
        if (e.access->m_type != INS) {
            return false;
        }
    }
    return true;
}

static bool OccRelationUsable(Relation relation, Oid relid)
{
    return relation != nullptr && OidIsValid(relid);
}

}  // namespace

void OccTransactionManager::WriteChanges(O_TxnManager* txMan)
{
    uint64_t commit_csn = txMan->GetCommitSequenceNumber();
    O_TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();

    for (const auto& raPair : orderedSet) {
        O_Access* access = raPair.second;
        access->WriteGlobalChanges(commit_csn);
    }

}

RC OccTransactionManager::ValidateOcc(O_TxnManager* txMan)
{
    uint32_t numSentinelLock = 0;
    m_rowsLocked = false;
    O_TxnAccess* txnAccess = txMan->m_accessMgr;
    RC rc = RC_OK;
    const uint32_t rowCount = txnAccess->m_Size();

    m_writeSetSize = 0;
    m_insertSetSize = 0;
    m_txnCounter++;

    do {
        /* 1.Perform pre-abort check and pre-processing */
        if (!PreAbortCheck(txMan)) {
            rc = RC_ABORT;
            break;
        }

        rc = LockHeaders(txMan, numSentinelLock);
        if (rc != RC_OK) {
            break;
        }

        if (!ValidateWriteSet(txMan)) {
            rc = RC_ABORT;
            break;
        }

    } while (0);

    if (rc == RC_OK) {
        m_rowsLocked = true;
    } else {
        if (numSentinelLock > 0) {
            ReleaseHeaderLocks(txMan, numSentinelLock);
        }
        m_rowsLocked = false;
    }

    return rc;
}

bool OccTransactionManager::PreAbortCheck(O_TxnManager* txMan)
{
    O_TxnAccess* tx = txMan->m_accessMgr;
    O_TxnOrderedSet_t& orderedSet = tx->GetOrderedRowSet();
    auto itr = orderedSet.begin();
    while (itr != orderedSet.end()) {
        O_Access* ac = (*itr).second;
        switch (ac->m_type) {
            case WR:
                m_writeSetSize++;
                break;
            case DEL:
                m_writeSetSize++;
                break;
            case INS:
                m_insertSetSize++;
                m_writeSetSize++;
                break;
            case RD_FOR_UPDATE:
            case RD:
                itr = orderedSet.erase(itr);
                //只验证写集
                continue;
            default:
                break;
        }

        if (m_preAbort) {
            if (!QuickHeaderValidation(ac)) {
                return false;
            }
        }
        (void)++itr;
    }
    return true;
}

RC OccTransactionManager::LockHeaders(O_TxnManager* txMan, uint32_t& numSentinelsLock)
{
    uint64_t sleepTime = 1;
    uint64_t thdId = txMan->GetThdId();
    O_TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    numSentinelsLock = 0;
    m_lockedSentinels.clear();

    std::vector<SortedWriteEntry> entries;
    CollectWriteSetByHash(orderedSet, entries);
    if (entries.empty()) {
        return RC_OK;
    }

    /*
     * 纯 INSERT 事务（TPC-C load 批量导入）：行由本事务创建，无需在提交时
     * 对 heap 加锁或 ReadBuffer；用本地副本校验即可，避免 Relation 失效崩溃。
     */
    if (IsInsertOnlyWriteSet(entries)) {
        for (const auto& e : entries) {
            if (!QuickInsertCheck(e.access)) {
                return RC_ABORT;
            }
        }
        return RC_OK;
    }

    const uint32_t uniqueHashCount = CountUniqueHashes(entries);

    while (true) {
        uint64_t lastHash = UINT64_MAX;
        numSentinelsLock = 0;
        bool lockIncomplete = false;

        for (const auto& e : entries) {
            if (e.hash != lastHash) {
                if (!e.sent.TryLock(thdId)) {
                    lockIncomplete = true;
                    break;
                }
                m_lockedSentinels.push_back(e.sent);
                numSentinelsLock++;
                lastHash = e.hash;
            }

            if (!QuickHeaderValidation(e.access)) {
                ReleaseHeaderLocks(txMan, numSentinelsLock);
                numSentinelsLock = 0;
                return RC_ABORT;
            }
        }

        if (!lockIncomplete && numSentinelsLock == uniqueHashCount) {
            return RC_OK;
        }

        ReleaseHeaderLocks(txMan, numSentinelsLock);
        numSentinelsLock = 0;

        if (m_preAbort) {
            for (const auto& e : entries) {
                if (!QuickHeaderValidation(e.access)) {
                    return RC_ABORT;
                }
            }
        }

        if (sleepTime > OCC_LOCK_TIME_OUT) {
            return RC_ABORT;
        }

        (void)usleep(5);
        sleepTime = sleepTime << 1;
    }
}

bool OccTransactionManager::QuickHeaderValidation(O_Access* access)
{
    if (access->m_type != INS) {
        return QuickVersionCheck(access);
    } else {
        return QuickInsertCheck(access);
    }
}

bool OccTransactionManager::QuickInsertCheck(O_Access* access)
{
    /* INSERT 优先用事务内本地副本校验，避免提交时 Relation 已失效 */
    if (access->m_localInsertRow != nullptr && access->m_localInsertRow->t_data != nullptr) {
        return (access->m_csn == access->m_localInsertRow->t_data->m_rowCSN);
    }

    occ_sentinel sent = access->m_origSentinel;
    HeapTupleData tuple;

    if (g_enable_occ_delta)
    {
        O_DeltaEntry deltaEntry;
        if (O_DeltaTable::Instance().Lookup(sent.relation, &sent.tuple_id, deltaEntry))
        {
            if (deltaEntry.deleted || deltaEntry.tuple == nullptr)
            {
                return false;
            }
            tuple = *deltaEntry.tuple;
            return (access->m_csn == tuple.t_data->m_rowCSN);
        }
    }

    if (!OccRelationUsable(sent.relation, sent.relid)) {
        return false;
    }

    Buffer buffer = ReadBuffer(sent.relation, ItemPointerGetBlockNumber(&sent.tuple_id));
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    sent.GetData(&tuple, buffer);

    bool result = (tuple.t_data != nullptr) && (access->m_csn == tuple.t_data->m_rowCSN);
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    ReleaseBuffer(buffer);

    return result;
}

bool OccTransactionManager::QuickVersionCheck(O_Access* access)
{
    HeapTuple localTuple = access->GetLocalVersion();
    if (localTuple == nullptr) {
        localTuple = access->m_globalRow;
    }
    if (localTuple != nullptr && localTuple->t_data != nullptr) {
        return ((access->m_csn == localTuple->t_data->m_rowCSN) && (localTuple->t_data->is_latest));
    }

    occ_sentinel sent = access->m_origSentinel;
    HeapTupleData tuple;

    if (g_enable_occ_delta)
    {
        O_DeltaEntry deltaEntry;
        if (O_DeltaTable::Instance().Lookup(sent.relation, &sent.tuple_id, deltaEntry))
        {
            if (deltaEntry.deleted || deltaEntry.tuple == nullptr)
            {
                return false;
            }
            tuple = *deltaEntry.tuple;
            return ((access->m_csn == tuple.t_data->m_rowCSN) && (tuple.t_data->is_latest));
        }
    }

    if (!OccRelationUsable(sent.relation, sent.relid)) {
        return false;
    }

    Buffer buffer = ReadBuffer(sent.relation, ItemPointerGetBlockNumber(&sent.tuple_id));
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    sent.GetData(&tuple, buffer);

    bool result = (tuple.t_data != nullptr) &&
                  (access->m_csn == tuple.t_data->m_rowCSN) &&
                  (tuple.t_data->is_latest);
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    ReleaseBuffer(buffer);

    return result;
}

bool OccTransactionManager::ValidateWriteSet(O_TxnManager* txMan)
{
    O_TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    for (const auto& raPair : orderedSet) {
        O_Access* ac = raPair.second;
        if (!QuickHeaderValidation(ac)) {
            return false;
        }
    }
    return true;
}

void OccTransactionManager::ReleaseHeaderLocks(O_TxnManager* txMan, uint32_t numOfLocks)
{
    (void)txMan;
    if (numOfLocks == 0 || m_lockedSentinels.empty()) {
        return;
    }

    uint32_t n = numOfLocks;
    if (n > m_lockedSentinels.size()) {
        n = (uint32_t)m_lockedSentinels.size();
    }

    for (uint32_t i = 0; i < n; ++i) {
        m_lockedSentinels[i].Unlock();
    }
    m_lockedSentinels.erase(m_lockedSentinels.begin(), m_lockedSentinels.begin() + n);
}

void OccTransactionManager::CleanUp()
{
    m_writeSetSize = 0;
    m_insertSetSize = 0;
    m_lockedSentinels.clear();
}