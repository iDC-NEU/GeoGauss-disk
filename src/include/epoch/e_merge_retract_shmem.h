#ifndef OPENGAUSS_E_MERGE_RETRACT_SHMEM_H
#define OPENGAUSS_E_MERGE_RETRACT_SHMEM_H

#include <cstdint>

/*
 * FFW supersede 时 merge worker 通知 coordinator 从 g_merged_wire_txns 移除 wire_key。
 * 无条数上限（环形队列容量由 GUC epoch_merge_retract_capacity 决定，默认与 merge_done 同量级）。
 */
extern Size EpochMergeRetractShmemSize(void);
extern void EpochMergeRetractShmemInit(void);
extern void EpochMergeRetractPush(uint64_t epoch, uint64_t wire_key);
extern void EpochMergeRetractDrain(void (*fn)(uint64_t epoch, uint64_t wire_key));

#endif /* OPENGAUSS_E_MERGE_RETRACT_SHMEM_H */
