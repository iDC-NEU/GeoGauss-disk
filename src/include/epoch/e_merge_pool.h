#ifndef OPENGAUSS_E_MERGE_POOL_H
#define OPENGAUSS_E_MERGE_POOL_H

#include "epoch/e_crdt.h"
#include "epoch/e_merge_shmem.h"
#include "epoch/e_txn_wire.h"

/*
 * 单节点或未启用多主：coordinator 主线程 inline merge。
 * epoch_merge_threads>=1 且多节点：postmaster 启动 EPOCHMERGEWORKER（数量=threads）。
 */
extern void EpochMergePoolInit(void);
extern void EpochMergePoolShutdown(void);
extern bool EpochMergePoolUsesInlineMerge(void);
extern bool EpochMergePoolSubmit(const E_WireTxn& txn, uint64_t wire_key, bool is_local);
extern bool EpochMergePoolPollCompletions(void);
extern bool EpochMergePoolHasPendingWork(void);
extern bool EpochMergePoolShouldThrottleDispatch(void);
extern bool EpochMergePoolShouldThrottleDispatchForEpoch(uint64_t epoch);
extern void EpochMergePoolSetTaskDoneHook(void (*hook)(const struct EpochMergeDoneSlot* done));
/* Finish 后同进程内联调用（二维队列无全局 done 环） */
extern void EpochMergePoolInvokeDoneHook(const struct EpochMergeDoneSlot* done);
extern void EpochMergePoolPackDoneFromOutcome(EpochMergeDoneSlot* done,
    const E_WireTxn& txn,
    uint64_t wire_key,
    bool ok,
    const E_CrdtMergeOutcome& outcome);
/* 共享 merge_epoch 游标（对齐 apply_epoch） */
extern uint64_t EpochMergePoolGetMergeEpoch(void);
extern void EpochMergePoolEnsureMergeEpoch(uint64_t floor_epoch);
extern bool EpochMergePoolTryAdvanceMergeEpoch(uint64_t completed_epoch);

#endif /* OPENGAUSS_E_MERGE_POOL_H */
