#ifndef OPENGAUSS_E_MERGE_POOL_H
#define OPENGAUSS_E_MERGE_POOL_H

#include <cstdint>

#include "epoch/e_txn_wire.h"

/*
 * merge 池：薄封装，只负责初始化与提交。
 * 队列与游标全部在 e_merge_shmem。
 */

extern void EpochMergePoolInit(void);
extern void EpochMergePoolShutdown(void);

/* 提交一条写集到它所属时间片的合并队列 */
extern bool EpochMergePoolSubmit(const E_WireTxn& txn);

#endif /* OPENGAUSS_E_MERGE_POOL_H */
