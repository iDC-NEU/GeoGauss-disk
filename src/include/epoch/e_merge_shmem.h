#ifndef OPENGAUSS_E_MERGE_SHMEM_H
#define OPENGAUSS_E_MERGE_SHMEM_H

#include <cstdint>

#include "epoch/e_txn_wire.h"

/*
 * merge 的全部职责：
 *
 *   1. 接收线程把 wire txn 放进它所属 epoch 的并发队列；
 *   2. merge 线程从 merge_epoch 那一格取一条，做 CRDT merge；
 *   3. merge 完一笔就把 merged 计数 +1；
 *   4. merged == declared 时把 merge_epoch CAS +1
 *      （CAS 保证多个线程同时看到“已经齐了”时也只会推进一次）。
 *
 * 没有共享内存任务表、没有槽位状态、没有 done 环、没有 retract 环。
 */

/* 入队：生产者 = 接收线程 */
extern bool EpochMergeQueueEnqueueTxn(const E_WireTxn& txn);

/* 取一条：从 epoch 对应的队列里出队，供 merge 线程使用 */
extern bool EpochMergeQueueTryDequeue(uint64_t epoch, E_WireTxn* out);
/* 诊断用：该 epoch 的 merge 队列近似长度 */
size_t EpochMergeQueueSize(uint64_t epoch);

/* 游标：已合并到第几个时间片 */
extern uint64_t GetMergeEpoch(void);

/* 把游标从 completed_epoch 推进到 completed_epoch+1（CAS，并发安全） */
extern bool EpochMergeQueueTryAdvanceMergeEpoch(uint64_t completed_epoch);
extern void EpochMergeQueueSetMergeEpoch(uint64_t epoch);

/* 诊断：打一行该时间片的队列/游标状态 */
extern void EpochMergeQueueDescribeEpoch(uint64_t epoch, char* out, size_t outlen);

extern void EpochMergeQueueRequestShutdown(void);
extern bool EpochMergeQueueShutdownRequested(void);

/* 生命周期：只负责队列与游标的初始化/复位 */
extern void EpochMergeQueueInit(void);
extern void EpochMergeQueueReset(void);

#endif /* OPENGAUSS_E_MERGE_SHMEM_H */
