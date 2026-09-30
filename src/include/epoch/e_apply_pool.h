#ifndef OPENGAUSS_E_APPLY_POOL_H
#define OPENGAUSS_E_APPLY_POOL_H

#include <cstdint>

#include "epoch/e_txn_wire.h"

/*
 * 方案 A：apply 任务抽象。
 * 单节点或未启用多主：coordinator 主线程 inline apply。
 * epoch_commit_threads>=1 且多节点：postmaster 启动 EPOCHAPPLYWORKER（数量=threads）。
 */
struct EpochApplyPoolTask {
    uint64_t epoch;
    uint64_t wire_key;
    uint32_t local_node;
    bool is_local;
    /* true：apply_txn 为原始 wire，执行前须 merge_complete 后 BuildApplyTxn */
    bool need_crdt_build;
    E_WireTxn apply_txn;
};

extern void EpochApplyPoolInit(void);
extern void EpochApplyPoolShutdown(void);

/* coordinator 是否在主线程内联 apply */
extern bool EpochApplyPoolUsesInlineApply(void);

/* 异步路径：从 done ring 取回 apply worker 完成事件 */
extern bool EpochApplyPoolPollCompletions(void);

/*
 * 异步路径：提交任务；若队列满返回 false，由调用方下轮重试同一 wire txn。
 * 同步路径：直接在调用线程执行 heap apply。
 */
extern bool EpochApplyPoolSubmit(const EpochApplyPoolTask& task);

/*
 * 异步路径：提交已序列化 payload。
 * extra_flags：如 EPOCH_APPLY_FLAG_NEED_CRDT_BUILD（原始 wire，关窗后 Build）。
 */
extern bool EpochApplyPoolSubmitSerialized(uint64_t epoch,
    uint64_t wire_key,
    uint32_t local_node,
    bool is_local,
    const char* payload,
    uint32_t payload_len,
    uint32_t extra_flags);

/* apply 线程完成回调（async 模式下由 e_pipeline 注册） */
extern void EpochApplyPoolSetTaskDoneHook(void (*hook)(uint64_t epoch, uint64_t wire_key, bool ok));

/* coordinator 轮询：全局或指定 epoch 是否仍有在途/已入队未完成任务 */
extern bool EpochApplyPoolHasPendingWork(void);
extern bool EpochApplyPoolHasPendingWorkForEpoch(uint64_t epoch);

/*
 * 指定 epoch 的在途已达上限时暂缓继续对该 epoch dispatch。
 * 当前 apply_epoch：上限≈tasks_per_slot；未来 prefill：上限≈commit_threads。
 */
extern bool EpochApplyPoolShouldThrottleDispatch(void);
extern bool EpochApplyPoolShouldThrottleDispatchForEpoch(uint64_t epoch);

/* 当前允许执行的 apply_epoch（与 logical 解耦推进） */
extern uint64_t EpochApplyPoolGetApplyEpoch(void);
extern void EpochApplyPoolEnsureApplyEpoch(uint64_t floor_epoch);
extern bool EpochApplyPoolTryAdvanceApplyEpoch(uint64_t completed_epoch);

#endif /* OPENGAUSS_E_APPLY_POOL_H */
