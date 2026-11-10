#ifndef OPENGAUSS_E_STUCK_TRACE_H
#define OPENGAUSS_E_STUCK_TRACE_H

#include <cstdint>

/*
 * 卡死定位「飞行记录器」：每个 epoch 线程把自己最近 N 步动作写进内存环，
 * 正常路径零 IO；一旦 watchdog 判定卡住（merge task 超时 / logical 不推进 /
 * merge submit 长时间阻塞），把全部线程的环按时间落盘。
 *
 * 目的：用逐行打印的方式定位到底停在哪一句，而不只是宏观阶段。
 */
enum EpochStuckTraceEvent {
    EST_NONE = 0,
    /* merge worker */
    EST_MERGE_CLAIM,
    EST_MERGE_DESERIALIZE,
    EST_MERGE_DESERIALIZE_FAIL,
    EST_MERGE_CRDT_ENTER,
    EST_CRDT_ENSURE_SLOT,
    EST_CRDT_COLLECT_SHARDS,
    EST_CRDT_ACQUIRE_BEGIN,
    EST_CRDT_ACQUIRE_SHARD,
    EST_CRDT_ACQUIRE_DONE,
    EST_CRDT_PREVIEW_ROW,
    EST_CRDT_MERGE_ROW,
    EST_CRDT_RELEASE_BEGIN,
    EST_CRDT_RELEASE_DONE,
    EST_MERGE_CRDT_DONE,
    EST_MERGE_FINISH,
    EST_MERGE_DONE_HOOK_ENTER,
    EST_MERGE_DONE_HOOK_ENQUEUE,
    EST_MERGE_DONE_HOOK_EXIT,
    EST_MERGE_TXN_DONE,
    /* 提交侧（recv / pack / backend） */
    EST_SUBMIT_ENTER,
    EST_SUBMIT_OK,
    EST_SUBMIT_FAIL_REBIND,
    EST_SUBMIT_FAIL_CELLS_FULL,
    EST_SUBMIT_RETRY,
    EST_RECV_TXN,
    EST_RECV_EPOCH_END,
    EST_DRAIN_LOCAL_EPOCH,
    EST_SEND_EPOCH_END,
    /* 游标 / 阶段 */
    EST_MERGE_CURSOR_ADVANCE,
    EST_MERGE_CURSOR_FORCE_ADVANCE,
    EST_LOGICAL_STALL,
    /* apply worker */
    EST_APPLY_CLAIM,
    EST_APPLY_COMMIT,
    EST_APPLY_DONE,
    EST_EV_COUNT
};

extern void EpochStuckTraceRegister(const char* thread_name);
/* a/b 为事件相关整数（epoch/shard/slot/retry），dump 时按事件名排版 */
extern void EpochStuckTraceNote(uint32_t event, uint32_t a, uint64_t b);
/* 落盘一次（内部 3s 限流，避免卡住时反复写）；force=true 忽略限流 */
extern void EpochStuckTraceDump(const char* reason, bool force);
/* logical 推进看门狗：epochphys 线程每跳调用一次 */
extern void EpochStuckTraceLogicalWatchdog(uint64_t logical_epoch);

#endif /* OPENGAUSS_E_STUCK_TRACE_H */
