#ifndef OPENGAUSS_E_APPLY_SHMEM_H
#define OPENGAUSS_E_APPLY_SHMEM_H

#include <cstdint>

#include "epoch/e_txn_wire.h"

/*
 * apply 的全部职责：
 *
 *   1. 接收线程/merge 线程把赢家写集放进它所属 epoch 的并发队列；
 *   2. apply 线程从 apply_epoch 那一格取一条出来；
 *   3. csn 在 abort 集合里 → 跳过并计 abort；
 *      否则按 csn 去重后计一次 commit，并执行写回；
 *   4. 该格队列取空 → apply_epoch CAS +1。
 *
 * 没有任务槽、没有 done 环、没有 inflight 记账、没有 payload 序列化。
 */

/* 入队：生产者可以是接收线程或 merge 线程 */
extern bool EpochApplyQueueEnqueueTxn(uint64_t epoch, const E_WireTxn& txn);

/* 从指定 epoch 那一格取一条；返回 false 表示该格已空 */
extern bool EpochApplyQueueTryDequeue(uint64_t epoch, E_WireTxn* out);

/* 游标：下一个要写回的时间片 */
extern uint64_t EpochApplyQueueGetApplyEpoch(void);
extern void EpochApplyQueueSetApplyEpoch(uint64_t epoch);

/* 把游标从 completed_epoch 推进到 completed_epoch+1（CAS，并发安全） */
extern bool EpochApplyQueueTryAdvanceApplyEpoch(uint64_t completed_epoch);

extern void EpochApplyQueueRequestShutdown(void);
extern bool EpochApplyQueueShutdownRequested(void);

/* csn 去重：同一事务的多行只让第一次返回 true（用于 commit 计数） */
extern bool EpochApplyCsnSeenFirst(uint64_t epoch, uint64_t csn);

extern void EpochApplyQueueInit(void);
extern void EpochApplyQueueReset(void);

#endif /* OPENGAUSS_E_APPLY_SHMEM_H */
