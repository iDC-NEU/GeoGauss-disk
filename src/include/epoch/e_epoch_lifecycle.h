#ifndef OPENGAUSS_E_EPOCH_LIFECYCLE_H
#define OPENGAUSS_E_EPOCH_LIFECYCLE_H

#include <cstdint>

/*
 * EPOCH_LIFECYCLE 分段语义（分析时注意）：
 *
 * - seal_to_merge_us：seal→merge_complete，可与前序 epoch 的 apply 重叠。
 * - merge_to_apply_us / merge_to_first_submit_us：从 merge_complete 起算。
 *   merge 可超前 apply_epoch（gap）；预 feed 后 first_submit 可能贴近 merge，
 *   但 merge_to_apply 仍可能含「等前序 apply_epoch 执行完」的等待。
 *   二者仍不宜单独当作「apply 执行慢」的充分依据。
 * - first_submit_to_apply_us / apply_cpu_us / apply_starve_ticks：
 *   从本 epoch 首次入队到完成；若已预 feed，first_submit 可能早于成为 apply_epoch，
 *   则 first_submit_to_apply 会含等待成为执行目标的时间——看 apply 墙钟更宜结合
 *   apply_cpu 与「成为 apply_epoch 之后」的行为。
 */

extern void EpochLifecycleReset(void);
extern void EpochLifecycleEnsureStarted(uint64_t epoch);
extern void EpochLifecycleNoteSeal(uint64_t epoch);
extern void EpochLifecycleNoteMergeComplete(uint64_t epoch);
extern void EpochLifecycleNoteApplyComplete(uint64_t epoch);
/* 本 epoch 首次成功 ApplyPoolSubmit；若 merge 已超前，距 merge_complete 可很长 */
extern void EpochLifecycleNoteApplyFirstSubmit(uint64_t epoch);
extern void EpochLifecycleNoteApplyStarveTick(uint64_t epoch);
extern void EpochLifecycleNoteApplyBatch(uint64_t epoch, uint64_t row_count, uint64_t apply_us, bool ok);
extern void EpochLifecycleEmitOnAdvance(uint64_t epoch);

#endif /* OPENGAUSS_E_EPOCH_LIFECYCLE_H */
