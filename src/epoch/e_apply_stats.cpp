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

/*
 * 一次性把某时间片的 merge 输家计入 abort（一个 csn 一个事务）。
 * 在 apply 游标推进时调用，保证 abort 与 commit 同口径：
 * 只统计已经走到 apply 的时间片，还压在发送/合并队列里的不算。
 */
void EpochApplyStatsNoteAbortN(uint64_t n)
{
    if (n == 0) {
        return;
    }
    O_OccStatsRecordAbort(OCC_ABORT_REMOTE_MERGE);
    EpochPerfAddApplyAbortTxn(n);
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
