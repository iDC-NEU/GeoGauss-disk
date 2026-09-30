//
// Created by hcj2003 on 10/8/25.
//

#include "occ/o_txn.h"
#include "occ/o_delta_table.h"
#include "occ/o_staging.h"
#include "occ/occ_internal.h"
#include "occ/occ_stats.h"

#include "epoch/e_config.h"
#include "epoch/e_heap_version.h"

#include "access/heapam.h"
#include "storage/buf/bufmgr.h"
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

static Relation O_OpenRelationForValidation(O_TxnManager* txMan, Oid relid, bool* needClose)
{
    *needClose = false;
    if (!OidIsValid(relid)) {
        return nullptr;
    }
    if (txMan != nullptr) {
        return txMan->GetValidationRelation(relid, needClose);
    }
    *needClose = true;
    return try_relation_open(relid, AccessShareLock);
}

static void O_CloseRelationIfOpened(Relation rel, bool needClose)
{
    if (needClose && rel != nullptr) {
        heap_close(rel, AccessShareLock);
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

static bool OccQuickLocalDraftCheck(O_Access* access)
{
    const bool checkLatest = true;
    HeapTuple localTuple = access->GetLocalVersion();
    if (localTuple == nullptr) {
        localTuple = access->m_globalRow;
    }
    if (localTuple == nullptr || localTuple->t_data == nullptr) {
        return false;
    }
    return OccTupleVersionMatches(access, localTuple->t_data, checkLatest);
}

static bool OccAcquireBufferShareLock(Buffer buffer, OccWriteSetValidateMode mode)
{
    if (mode == OCC_WSV_HEAP_NOWAIT) {
        return TryLockBuffer(buffer, BUFFER_LOCK_SHARE, false);
    }
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    return true;
}

static bool OccReadBufferTupleVersion(O_Access* access, Relation rel, const occ_sentinel& sent, bool checkLatest,
    OccWriteSetValidateMode mode)
{
    if (EpochHeapVersionChainEnabled(rel)) {
        HeapTuple chain_copy = nullptr;
        bool ok = false;
        if (checkLatest) {
            if (EpochHeapFetchLatestCopy(rel, &sent.tuple_id, &chain_copy, NULL) && chain_copy != nullptr &&
                chain_copy->t_data != nullptr) {
                ok = OccTupleVersionMatches(access, chain_copy->t_data, true);
            }
        } else if (EpochHeapFetchPinnedCsnCopy(rel, &sent.tuple_id, access->m_csn, &chain_copy) &&
                   chain_copy != nullptr && chain_copy->t_data != nullptr) {
            ok = (access->m_csn == chain_copy->t_data->m_rowCSN);
        }
        if (chain_copy != nullptr) {
            heap_freetuple(chain_copy);
        }
        return ok;
    }

    Buffer buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(&sent.tuple_id));
    if (!OccAcquireBufferShareLock(buffer, mode)) {
        ReleaseBuffer(buffer);
        return false;
    }

    HeapTupleData tuple;
    occ_sentinel readSent = sent;
    readSent.relation = rel;
    readSent.GetData(&tuple, buffer);

    bool ok = checkLatest ? OccTupleVersionMatches(access, tuple.t_data, true)
                          : (tuple.t_data != nullptr && access->m_csn == tuple.t_data->m_rowCSN);

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);
    return ok;
}

}  // namespace

void OccTransactionManager::WriteChanges(O_TxnManager* txMan)
{
    uint64_t commit_csn = txMan->GetCommitSequenceNumber();
    O_TxnAccess* accessMgr = txMan->m_accessMgr;

    accessMgr->ForEachWriteEntry([&](const occ_sentinel& sent, O_Access* access) {
        access->WriteGlobalChanges(commit_csn, sent);
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
        /*
         * epoch 多主：pack 前不做 ValidateWriteSet / LockHeaders。
         * 写-写冲突由 CRDT merge + abort_csn 处理；heap 版本检查仅在 RECORD_COMMIT
         * RevalidateWriteSet（HEAP_NOWAIT）。PreAbortCheck 仅整理写集。
         */
        if (g_enable_epoch_multimaster && EpochGetTotalNodes() > 1) {
            break;
        }

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
            if (!QuickInsertCheck(e.access, e.sent, false, false)) {
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

            const bool hashLocked = (e.access->m_type == INS) ||
                                    (e.hash == lastHash && !lockIncomplete);
            if (!QuickHeaderValidation(e.access, e.sent, hashLocked, false)) {
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
                if (!QuickHeaderValidation(e.access, e.sent, false, false)) {
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

bool OccTransactionManager::QuickHeaderValidation(O_Access* access, const occ_sentinel& key, bool lockHeld,
    bool preAbortPhase, OccWriteSetValidateMode mode)
{
    if (access->m_type != INS) {
        return QuickVersionCheck(access, key, lockHeld, preAbortPhase, mode);
    }
    return QuickInsertCheck(access, key, lockHeld, preAbortPhase, mode);
}

bool OccTransactionManager::QuickInsertCheck(O_Access* access, const occ_sentinel& key, bool lockHeld,
    bool preAbortPhase, OccWriteSetValidateMode mode)
{
    (void)lockHeld;
    (void)preAbortPhase;

    if (mode == OCC_WSV_LOCAL_DRAFT) {
        if (access->m_localInsertRow != nullptr && access->m_localInsertRow->t_data != nullptr) {
            return (access->m_csn == access->m_localInsertRow->t_data->m_rowCSN);
        }
        return OccQuickLocalDraftCheck(access);
    }

    if (access->m_localInsertRow != nullptr && access->m_localInsertRow->t_data != nullptr) {
        return (access->m_csn == access->m_localInsertRow->t_data->m_rowCSN);
    }

    Oid relid = InvalidOid;
    ItemPointerData tid;
    if (!OccPrepareAccessTarget(access, &key, &relid, &tid)) {
        return false;
    }

    occ_sentinel sent = access->m_origSentinel;
    bool relClosed = false;
    Relation rel = O_OpenRelationForValidation(m_activeTxn, relid, &relClosed);
    if (rel == nullptr || !OccTidIsPlausible(&tid, rel)) {
        O_CloseRelationIfOpened(rel, relClosed);
        return false;
    }

    if (OccStagingOverlayEnabled()) {
        HeapTuple staged = nullptr;
        bool deleted = false;
        if (OccStagingTryReadTuple(rel, &sent.tuple_id, &staged, &deleted)) {
            if (deleted || staged == nullptr || staged->t_data == nullptr) {
                if (staged != nullptr) {
                    heap_freetuple(staged);
                }
                O_CloseRelationIfOpened(rel, relClosed);
                return false;
            }
            bool ok = (access->m_csn == staged->t_data->m_rowCSN);
            heap_freetuple(staged);
            O_CloseRelationIfOpened(rel, relClosed);
            return ok;
        }
    }

    if (g_enable_occ_delta && !g_enable_occ_staging) {
        O_DeltaEntry deltaEntry;
        if (O_DeltaTable::Instance().Lookup(rel, &sent.tuple_id, deltaEntry)) {
            if (deltaEntry.deleted) {
                O_CloseRelationIfOpened(rel, relClosed);
                return false;
            }
            HeapTuple copy = O_DeltaTable::Instance().LookupCopyTuple(rel, &sent.tuple_id);
            if (copy == nullptr || copy->t_data == nullptr) {
                if (copy != nullptr) {
                    heap_freetuple(copy);
                }
                O_CloseRelationIfOpened(rel, relClosed);
                return false;
            }
            bool ok = (access->m_csn == copy->t_data->m_rowCSN);
            heap_freetuple(copy);
            O_CloseRelationIfOpened(rel, relClosed);
            return ok;
        }
    }

    if (!OccReadBufferTupleVersion(access, rel, sent, false, mode)) {
        O_CloseRelationIfOpened(rel, relClosed);
        return false;
    }
    O_CloseRelationIfOpened(rel, relClosed);
    return true;
}

bool OccTransactionManager::QuickVersionCheck(O_Access* access, const occ_sentinel& key, bool lockHeld,
    bool preAbortPhase, OccWriteSetValidateMode mode)
{
    (void)lockHeld;
    (void)preAbortPhase;
    const bool checkLatest = true;

    if (mode == OCC_WSV_LOCAL_DRAFT) {
        return OccQuickLocalDraftCheck(access);
    }

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

    Oid relid = InvalidOid;
    ItemPointerData tid;
    if (!OccPrepareAccessTarget(access, &key, &relid, &tid)) {
        return false;
    }

    occ_sentinel sent = access->m_origSentinel;
    bool relClosed = false;
    Relation rel = O_OpenRelationForValidation(m_activeTxn, relid, &relClosed);
    if (rel == nullptr || !OccTidIsPlausible(&tid, rel)) {
        O_CloseRelationIfOpened(rel, relClosed);
        return false;
    }

    if (OccStagingOverlayEnabled()) {
        HeapTuple staged = nullptr;
        bool deleted = false;
        if (OccStagingTryReadTuple(rel, &sent.tuple_id, &staged, &deleted)) {
            if (deleted || staged == nullptr || staged->t_data == nullptr) {
                if (staged != nullptr) {
                    heap_freetuple(staged);
                }
                O_CloseRelationIfOpened(rel, relClosed);
                return false;
            }
            bool ok = OccTupleVersionMatches(access, staged->t_data, checkLatest);
            heap_freetuple(staged);
            O_CloseRelationIfOpened(rel, relClosed);
            return ok;
        }
    }

    if (g_enable_occ_delta && !g_enable_occ_staging) {
        O_DeltaEntry deltaEntry;
        if (O_DeltaTable::Instance().Lookup(rel, &sent.tuple_id, deltaEntry)) {
            if (deltaEntry.deleted) {
                O_CloseRelationIfOpened(rel, relClosed);
                return false;
            }
            HeapTuple copy = O_DeltaTable::Instance().LookupCopyTuple(rel, &sent.tuple_id);
            if (copy == nullptr || copy->t_data == nullptr) {
                if (copy != nullptr) {
                    heap_freetuple(copy);
                }
                O_CloseRelationIfOpened(rel, relClosed);
                return false;
            }
            bool ok = OccTupleVersionMatches(access, copy->t_data, checkLatest);
            heap_freetuple(copy);
            O_CloseRelationIfOpened(rel, relClosed);
            return ok;
        }
    }

    if (!OccReadBufferTupleVersion(access, rel, sent, checkLatest, mode)) {
        O_CloseRelationIfOpened(rel, relClosed);
        return false;
    }
    O_CloseRelationIfOpened(rel, relClosed);
    return true;
}

bool OccTransactionManager::ValidateWriteSet(O_TxnManager* txMan, OccWriteSetValidateMode mode)
{
    O_TxnAccess* accessMgr = txMan->m_accessMgr;
    bool ok = true;
    accessMgr->ForEachWriteEntry([&](const occ_sentinel& sent, O_Access* ac) {
        if (ok && !QuickHeaderValidation(ac, sent, false, false, mode)) {
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
