//
// Created by hcj2003 on 10/8/25.
//

#include "occ/o_txn.h"
#include "occ/o_delta_table.h"
#include "occ/occ_internal.h"
#include "occ/occ_stats.h"

#include "utils/rel.h"

#include <algorithm>
#include <cstdint>
#include <vector>

/* 默认不启用 delta 覆盖模式 */
bool g_enable_occ_delta = false;

namespace {

struct SortedWriteEntry {
    uint64_t hash;
    O_Access* access;
    occ_sentinel sent;
};

static void CollectWriteSetByHash(O_TxnAccess* access, std::vector<SortedWriteEntry>& out)
{
    out.clear();
    if (access == nullptr) {
        return;
    }
    access->ForEachWriteEntry([&](const occ_sentinel& sent, O_Access* ac) {
        uint64_t h = sentinel_hash(sent.relid, &sent.tuple_id);
        out.push_back({h, ac, sent});
    });
    std::sort(out.begin(), out.end(),
              [](const SortedWriteEntry& a, const SortedWriteEntry& b) { return a.hash < b.hash; });
}

static uint32_t CountLockableHashes(const std::vector<SortedWriteEntry>& entries)
{
    uint32_t count = 0;
    uint64_t prev = UINT64_MAX;
    for (const auto& e : entries) {
        if (e.access->m_type == INS) {
            continue;
        }
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

static Relation O_OpenRelationForValidation(O_TxnManager* txMan, Oid relid, Relation cachedRel, bool* needClose)
{
    if (txMan != nullptr) {
        return txMan->GetCachedRelation(relid, cachedRel, needClose);
    }

    *needClose = false;
    if (cachedRel != nullptr && OidIsValid(relid)) {
        return cachedRel;
    }
    if (!OidIsValid(relid)) {
        return nullptr;
    }
    *needClose = true;
    return heap_open(relid, RowExclusiveLock);
}

static void O_CloseRelationIfOpened(Relation rel, bool needClose)
{
    if (needClose && rel != nullptr) {
        heap_close(rel, RowExclusiveLock);
    }
}

static bool OccTupleVersionMatches(O_Access* access, HeapTupleHeader hdr, bool checkLatest)
{
    if (hdr == nullptr) {
        return false;
    }
    if (access->m_csn != hdr->m_rowCSN) {
        return false;
    }
    if (checkLatest && !hdr->is_latest) {
        return false;
    }
    return true;
}

}  // namespace

void OccTransactionManager::WriteChanges(O_TxnManager* txMan)
{
    uint64_t commit_csn = txMan->GetCommitSequenceNumber();
    O_TxnAccess* accessMgr = txMan->m_accessMgr;

    accessMgr->ForEachWriteEntry([&](const occ_sentinel& sent, O_Access* access) {
        (void)sent;
        access->WriteGlobalChanges(commit_csn);
    });
}

RC OccTransactionManager::ValidateOcc(O_TxnManager* txMan)
{
    uint32_t numSentinelLock = 0;
    m_rowsLocked = false;
    O_TxnAccess* txnAccess = txMan->m_accessMgr;
    RC rc = RC_OK;

    m_writeSetSize = 0;
    m_insertSetSize = 0;
    m_txnCounter++;
    SetActiveTxn(txMan);

    do {
        /* 1. 写集整理：剥离纯读条目，不在此阶段做 heap 校验 */
        if (!PreAbortCheck(txMan)) {
            m_lastAbortReason = OCC_ABORT_PRE_VALIDATION;
            O_OccStatsRecordAbort(OCC_ABORT_PRE_VALIDATION);
            rc = RC_ABORT;
            break;
        }

        m_lastAbortReason = OCC_ABORT_NONE;
        rc = LockHeaders(txMan, numSentinelLock);
        if (rc != RC_OK) {
            if (m_lastAbortReason != OCC_ABORT_NONE) {
                O_OccStatsRecordAbort(m_lastAbortReason);
            }
            break;
        }

    } while (0);

    if (rc == RC_OK) {
        m_rowsLocked = true;
    } else {
        m_abortsCounter++;
        if (numSentinelLock > 0) {
            ReleaseHeaderLocks(txMan, numSentinelLock);
        }
        m_rowsLocked = false;
    }

    SetActiveTxn(nullptr);
    return rc;
}

bool OccTransactionManager::PreAbortCheck(O_TxnManager* txMan)
{
    O_TxnAccess* tx = txMan->m_accessMgr;
    tx->WriteSetRemoveReadEntries();

    tx->ForEachWriteEntry([&](const occ_sentinel& sent, O_Access* ac) {
        (void)sent;
        switch (ac->m_type) {
            case WR:
            case DEL:
                m_writeSetSize++;
                break;
            case INS:
                m_insertSetSize++;
                m_writeSetSize++;
                break;
            default:
                break;
        }
    });
    return true;
}

RC OccTransactionManager::LockHeaders(O_TxnManager* txMan, uint32_t& numSentinelsLock)
{
    uint64_t sleepTime = 1;
    uint64_t thdId = txMan->GetThdId();
    O_TxnAccess* accessMgr = txMan->m_accessMgr;
    numSentinelsLock = 0;
    m_lockedSentinels.clear();

    std::vector<SortedWriteEntry> entries;
    CollectWriteSetByHash(accessMgr, entries);
    if (entries.empty()) {
        return RC_OK;
    }

    if (IsInsertOnlyWriteSet(entries)) {
        for (const auto& e : entries) {
            if (!QuickInsertCheck(e.access, false)) {
                m_lastAbortReason = OCC_ABORT_LOCK_VERSION;
                return RC_ABORT;
            }
        }
        return RC_OK;
    }

    const uint32_t lockableHashCount = CountLockableHashes(entries);

    while (true) {
        uint64_t lastHash = UINT64_MAX;
        numSentinelsLock = 0;
        bool lockIncomplete = false;

        for (const auto& e : entries) {
            if (e.hash != lastHash) {
                lastHash = e.hash;
                if (e.access->m_type == INS) {
                    /* 新插入行 TID 唯一，无需在提交时抢锁 */
                } else if (!e.sent.TryLock(thdId)) {
                    lockIncomplete = true;
                    break;
                } else {
                    m_lockedSentinels.push_back(e.sent);
                    numSentinelsLock++;
                }
            }

            if (!QuickHeaderValidation(e.access, false)) {
                ReleaseHeaderLocks(txMan, numSentinelsLock);
                numSentinelsLock = 0;
                m_lastAbortReason = OCC_ABORT_LOCK_VERSION;
                return RC_ABORT;
            }
        }

        if (!lockIncomplete && numSentinelsLock == lockableHashCount) {
            return RC_OK;
        }

        ReleaseHeaderLocks(txMan, numSentinelsLock);
        numSentinelsLock = 0;

        if (m_preAbort) {
            for (const auto& e : entries) {
                if (!QuickHeaderValidation(e.access, false)) {
                    m_lastAbortReason = OCC_ABORT_LOCK_VERSION;
                    return RC_ABORT;
                }
            }
        }

        if (sleepTime > OCC_LOCK_TIME_OUT) {
            m_lastAbortReason = OCC_ABORT_LOCK_TIMEOUT;
            return RC_ABORT;
        }

        for (int i = 0; i < 64; ++i) {
            __builtin_ia32_pause();
        }
        sleepTime = sleepTime << 1;
    }
}

bool OccTransactionManager::QuickHeaderValidation(O_Access* access, bool preAbortPhase)
{
    if (access->m_type != INS) {
        return QuickVersionCheck(access, preAbortPhase);
    }
    return QuickInsertCheck(access, preAbortPhase);
}

bool OccTransactionManager::QuickInsertCheck(O_Access* access, bool preAbortPhase)
{
    (void)preAbortPhase;

    if (access->m_localInsertRow != nullptr && access->m_localInsertRow->t_data != nullptr) {
        return (access->m_csn == access->m_localInsertRow->t_data->m_rowCSN);
    }

    occ_sentinel sent = access->m_origSentinel;
    HeapTupleData tuple;
    bool relClosed = false;
    Relation rel = O_OpenRelationForValidation(m_activeTxn, sent.relid, sent.relation, &relClosed);
    if (rel == nullptr) {
        return false;
    }

    if (g_enable_occ_delta) {
        O_DeltaEntry deltaEntry;
        if (O_DeltaTable::Instance().Lookup(rel, &sent.tuple_id, deltaEntry)) {
            if (deltaEntry.deleted || deltaEntry.tuple == nullptr) {
                O_CloseRelationIfOpened(rel, relClosed);
                return false;
            }
            tuple = *deltaEntry.tuple;
            bool ok = (access->m_csn == tuple.t_data->m_rowCSN);
            O_CloseRelationIfOpened(rel, relClosed);
            return ok;
        }
    }

    Buffer buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(&sent.tuple_id));
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    occ_sentinel readSent = sent;
    readSent.relation = rel;
    readSent.GetData(&tuple, buffer);

    bool ok = (tuple.t_data != nullptr) && (access->m_csn == tuple.t_data->m_rowCSN);

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);
    O_CloseRelationIfOpened(rel, relClosed);

    return ok;
}

bool OccTransactionManager::QuickVersionCheck(O_Access* access, bool preAbortPhase)
{
    (void)preAbortPhase;
    const bool checkLatest = true;

    if (access->m_type != WR && access->m_type != DEL) {
        HeapTuple localTuple = access->GetLocalVersion();
        if (localTuple == nullptr) {
            localTuple = access->m_globalRow;
        }
        if (localTuple != nullptr && localTuple->t_data != nullptr) {
            if (OccTupleVersionMatches(access, localTuple->t_data, checkLatest)) {
                return true;
            }
        }
    }

    occ_sentinel sent = access->m_origSentinel;
    HeapTupleData tuple;
    bool relClosed = false;
    Relation rel = O_OpenRelationForValidation(m_activeTxn, sent.relid, sent.relation, &relClosed);
    if (rel == nullptr) {
        return false;
    }

    if (g_enable_occ_delta) {
        O_DeltaEntry deltaEntry;
        if (O_DeltaTable::Instance().Lookup(rel, &sent.tuple_id, deltaEntry)) {
            if (deltaEntry.deleted || deltaEntry.tuple == nullptr) {
                O_CloseRelationIfOpened(rel, relClosed);
                return false;
            }
            tuple = *deltaEntry.tuple;
            bool ok = OccTupleVersionMatches(access, tuple.t_data, checkLatest);
            O_CloseRelationIfOpened(rel, relClosed);
            return ok;
        }
    }

    Buffer buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(&sent.tuple_id));
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    occ_sentinel readSent = sent;
    readSent.relation = rel;
    readSent.GetData(&tuple, buffer);

    bool ok = OccTupleVersionMatches(access, tuple.t_data, checkLatest);

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);
    O_CloseRelationIfOpened(rel, relClosed);

    return ok;
}

bool OccTransactionManager::ValidateWriteSet(O_TxnManager* txMan)
{
    O_TxnAccess* accessMgr = txMan->m_accessMgr;
    bool ok = true;
    accessMgr->ForEachWriteEntry([&](const occ_sentinel& sent, O_Access* ac) {
        (void)sent;
        if (ok && !QuickHeaderValidation(ac, false)) {
            ok = false;
        }
    });
    return ok;
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
    SetActiveTxn(nullptr);
}
