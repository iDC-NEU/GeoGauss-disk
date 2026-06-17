//
// OCC 提交路径统计：成功 commit 与序列化 abort 分类计数（进程内全局累加）。
//

#ifndef OPENGAUSS_OCC_STATS_H
#define OPENGAUSS_OCC_STATS_H

enum OccAbortReason {
    OCC_ABORT_NONE = 0,
    OCC_ABORT_PRE_VALIDATION,   /* PreAbortCheck 阶段 CSN/版本校验失败 */
    OCC_ABORT_LOCK_VERSION,     /* LockHeaders 阶段版本校验失败 */
    OCC_ABORT_LOCK_TIMEOUT,     /* TryLock 自旋超时 */
};

void O_OccStatsRecordCommit();
void O_OccStatsRecordAbort(OccAbortReason reason);

/* 将当前累计计数写入 pg_log（LOG 级别，前缀 OCC_STATS） */
void O_OccStatsLogSummary();

#endif  // OPENGAUSS_OCC_STATS_H
