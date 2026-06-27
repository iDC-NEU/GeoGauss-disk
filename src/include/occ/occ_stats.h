//
// OCC / epoch 提交路径统计：成功 commit 与序列化 abort 分类计数（进程内全局累加）。
//

#ifndef OPENGAUSS_OCC_STATS_H
#define OPENGAUSS_OCC_STATS_H

enum OccAbortReason {
    OCC_ABORT_NONE = 0,
    OCC_ABORT_PRE_VALIDATION,   /* PreAbortCheck / ValidateOcc 阶段失败 */
    OCC_ABORT_LOCK_VERSION,     /* LockHeaders 阶段版本校验失败 */
    OCC_ABORT_LOCK_TIMEOUT,     /* TryLock 自旋超时 */
    OCC_ABORT_REMOTE_MERGE,     /* epoch CRDT merge 判定本事务为输家 */
    OCC_ABORT_REVALIDATE,       /* epoch RECORD_COMMIT 前 revalidate 失败 */
};

enum OccPreAbortFailReason {
    OCC_PRE_FAIL_UNKNOWN = 0,
    OCC_PRE_FAIL_RELATION,
    OCC_PRE_FAIL_CSN,
    OCC_PRE_FAIL_NOT_LATEST,
    OCC_PRE_FAIL_INSERT,
};

void O_OccStatsRecordCommit();
void O_OccStatsRecordAbort(OccAbortReason reason);
void O_OccStatsRecordPreAbortDetail(OccPreAbortFailReason reason);

/* 将当前累计计数写入 pg_log（LOG 级别，前缀 OCC_STATS） */
void O_OccStatsLogSummary();

/* 默认 false：每 1024 次 commit/abort 采样一行；true 时每次 commit/abort 打一行 */
extern bool g_occ_enable_stats_log;

#endif  // OPENGAUSS_OCC_STATS_H
