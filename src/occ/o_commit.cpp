#include "occ/o_txn.h"
#include "occ/o_commit.h"
#include "occ/o_csn_manager.h"
#include "occ/occ_internal.h"
#include "occ/occ_mem.h"
#include "occ/occ_stats.h"
#include "utils/elog.h"

static O_CSNManager csnManager;

O_CSNManager::O_CSNManager() : m_csn(INITIAL_CSN)
{
}

// 接口
void OCCXactCallback(XactEvent event)
{
    if (event == XACT_EVENT_START) {
        return;
    }

    O_TxnManager *txn = u_sess->occ_cxt.txn_manager;
    if (txn == nullptr) {
        return;
    }

    if (event == XACT_EVENT_COMMIT)
    {
        if (!O_TxnHasActiveWriteSet()) {
            return;
        }
        if (O_ValidateCommit() != RC_OK)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                     errmsg("occ: could not serialize access due to concurrent update"),
                     errhidestmt(true)));
        }
        else
        {
            O_OccStatsRecordCommit();
        }
    }
    else if (event == XACT_EVENT_RECORD_COMMIT)
    {
        if (!O_TxnHasActiveWriteSet()) {
            return;
        }
        uint64_t csn = csnManager.GetNextCSN();
        O_RecordCommit(csn);
    }
    else if (event == XACT_EVENT_END_TRANSACTION)
    {
        txn->EndTransaction();
    }
    else if (event == XACT_EVENT_ABORT)
    {
        txn->Rollback();
    }
}

RC O_ValidateCommit()
{
    O_TxnManager *txn = GetTxnManager();
    return txn->ValidateCommit();
}

void O_RecordCommit(uint64_t csn)
{
    O_TxnManager *txn = GetTxnManager();
    txn->SetCommitSequenceNumber(csn);
    txn->RecordCommit();
}
