#include "postgres.h"

#include "epoch/e_apply_stats.h"
#include "epoch/e_config.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_perf.h"
#include "epoch/e_shmem.h"

#include "occ/occ_stats.h"

void EpochApplyStatsNoteCommit(void)
{
    O_OccStatsRecordCommit();
    EpochPerfNoteApplyCommitTxn();
}

void EpochApplyStatsNoteAbort(OccAbortReason reason)
{
    O_OccStatsRecordAbort(reason);
    EpochPerfNoteApplyAbortTxn();
}

void EpochApplyStatsNoteFinalize(uint64_t epoch, uint32_t local_node, OccAbortReason reason)
{
    (void)epoch;
    (void)local_node;
    EpochApplyStatsNoteAbort(reason);
}

void EpochApplyStatsNoteWireTxnProcessed(uint64_t epoch, uint32_t local_node)
{
    (void)epoch;
    (void)local_node;
}

void EpochApplyStatsGetCounts(uint64_t* commit_out, uint64_t* abort_out)
{
    if (commit_out != nullptr) {
        *commit_out = 0;
    }
    if (abort_out != nullptr) {
        *abort_out = 0;
    }
    if (!g_enable_epoch_multimaster) {
        return;
    }
    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr) {
        return;
    }
    if (commit_out != nullptr) {
        *commit_out = state->perf.apply_commit_txn.load(std::memory_order_relaxed);
    }
    if (abort_out != nullptr) {
        *abort_out = state->perf.apply_abort_txn.load(std::memory_order_relaxed);
    }
}
