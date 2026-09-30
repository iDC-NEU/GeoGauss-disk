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

/*
 * pack 前（epoch 多主）：LOCAL_DRAFT，WR/INS 仅比本地 draft，不 ReadBuffer/LockBuffer。
 * RECORD 前（sync revalidate）：HEAP_NOWAIT，TryLockBuffer(SHARE) 失败则 abort。
 * LockHeaders / 单节点：HEAP_WAIT，阻塞 SHARE。
 */
enum OccWriteSetValidateMode {
    OCC_WSV_LOCAL_DRAFT = 0,
    OCC_WSV_HEAP_NOWAIT,
    OCC_WSV_HEAP_WAIT,
};

class OccTransactionManager {
public:
    void WriteChanges(O_TxnManager* txMan);

    /* 单节点：PreAbortCheck + LockHeaders；epoch 多主 pack 前无校验（RECORD 时 RevalidateWriteSet） */
    RC ValidateOcc(O_TxnManager* tx);

    /** @brief Perform pre-processing and validation   */
    bool PreAbortCheck(O_TxnManager* txMan);

    /** @brief Validate Header for insert   */
    bool QuickHeaderValidation(O_Access* access, const occ_sentinel& key, bool lockHeld, bool preAbortPhase = false,
        OccWriteSetValidateMode mode = OCC_WSV_HEAP_WAIT);

    /** @brief Perform validation of current access   */
    bool QuickVersionCheck(O_Access* access, const occ_sentinel& key, bool lockHeld, bool preAbortPhase = false,
        OccWriteSetValidateMode mode = OCC_WSV_HEAP_WAIT);

    /** @brief Perform validation of current access   */
    bool QuickInsertCheck(O_Access* access, const occ_sentinel& key, bool lockHeld, bool preAbortPhase = false,
        OccWriteSetValidateMode mode = OCC_WSV_HEAP_WAIT);

    /** @brief Lock all keys   */
    RC LockHeaders(O_TxnManager* txMan, uint32_t& numSentinelsLock);

    bool ValidateWriteSet(O_TxnManager* txMan, OccWriteSetValidateMode mode = OCC_WSV_HEAP_WAIT);

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

    bool m_preAbort{false};

    OccAbortReason m_lastAbortReason{OCC_ABORT_NONE};

    OccPreAbortFailReason m_lastPreAbortFailReason{OCC_PRE_FAIL_UNKNOWN};

    /** @var 已加锁的 hash 槽代表 sentinel（去重后），用于按序释放 */
    std::vector<occ_sentinel> m_lockedSentinels;
};

#endif  // OPENGAUSS_O_OCCMANAGER_H
