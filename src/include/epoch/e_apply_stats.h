#ifndef OPENGAUSS_E_APPLY_STATS_H
#define OPENGAUSS_E_APPLY_STATS_H

#include <cstdint>

#include "occ/occ_stats.h"

/*
 * sync 多主写：backend COMMIT 仅 pack 返回，真正完成在 apply worker。
 * apply 成功/跳过落盘时在此入账，供压测后 gsql 查询与 epoch_perf 对齐。
 */
extern void EpochApplyStatsNoteCommit(void);
extern void EpochApplyStatsNoteAbort(OccAbortReason reason);
/* merge 输家 / abort_csn：abort 统计 + epoch committed +1 */
extern void EpochApplyStatsNoteFinalize(uint64_t epoch, uint32_t local_node, OccAbortReason reason);
/* 只读等 wire txn 落账：仅 committed +1，不计 abort */
extern void EpochApplyStatsNoteWireTxnProcessed(uint64_t epoch, uint32_t local_node);
extern void EpochApplyStatsGetCounts(uint64_t* commit_out, uint64_t* abort_out);

#endif /* OPENGAUSS_E_APPLY_STATS_H */
