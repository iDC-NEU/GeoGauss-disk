//
// Created by hcj2003 on 10/8/25.
//

#ifndef OPENGAUSS_O_OCCMANAGER_H
#define OPENGAUSS_O_OCCMANAGER_H

#include <vector>
#include "occ/o_sentinel.h"
#include "occ/occ_stats.h"

class O_TxnManager;

static constexpr uint64_t OCC_LOCK_TIME_OUT = 1 << 16;

class OccTransactionManager {
public:
    void WriteChanges(O_TxnManager* txMan);

    RC ValidateOcc(O_TxnManager* tx);

    /** @brief Perform pre-processing and validation   */
    bool PreAbortCheck(O_TxnManager* txMan);

    /** @brief Validate Header for insert   */
    bool QuickHeaderValidation(O_Access* access, const occ_sentinel& key, bool lockHeld, bool preAbortPhase = false);

    /** @brief Perform validation of current access   */
    bool QuickVersionCheck(O_Access* access, const occ_sentinel& key, bool lockHeld, bool preAbortPhase = false);

    /** @brief Perform validation of current access   */
    bool QuickInsertCheck(O_Access* access, const occ_sentinel& key, bool lockHeld, bool preAbortPhase = false);

    /** @brief Lock all keys   */
    RC LockHeaders(O_TxnManager* txMan, uint32_t& numSentinelsLock);

    bool ValidateWriteSet(O_TxnManager* txMan);

    void ReleaseHeaderLocks(O_TxnManager* txMan, uint32_t numOfLocks);

    void ReleaseLocks(O_TxnManager* txMan)
    {
        if (m_rowsLocked) {
            ReleaseHeaderLocks(txMan, (uint32_t)m_lockedSentinels.size());
            m_rowsLocked = false;
        }
    }

    void CleanUp();

    void SetActiveTxn(O_TxnManager *txn)
    {
        m_activeTxn = txn;
    }

private:
    O_TxnManager *m_activeTxn{nullptr};
    uint64_t m_txnCounter{0};

    /** @var aborts counter   */
    uint64_t m_abortsCounter{0};

    /** @var Write set size. */
    uint32_t m_writeSetSize;

    /** @var Write set size. */
    uint32_t m_insertSetSize;

    bool m_rowsLocked{false};

    bool m_validationNoWait{true};

    bool m_preAbort{false};

    OccAbortReason m_lastAbortReason{OCC_ABORT_NONE};

    OccPreAbortFailReason m_lastPreAbortFailReason{OCC_PRE_FAIL_UNKNOWN};

    /** @var 已加锁的 hash 槽代表 sentinel（去重后），用于按序释放 */
    std::vector<occ_sentinel> m_lockedSentinels;
};

#endif  // OPENGAUSS_O_OCCMANAGER_H
