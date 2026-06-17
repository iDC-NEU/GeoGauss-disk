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

/* PreAbortCheck 失败子类（仅 abort_pre_validation 时累加） */
enum OccPreAbortFailReason {
    OCC_PRE_FAIL_UNKNOWN = 0,
    OCC_PRE_FAIL_RELATION,      /* 无法打开 relation */
    OCC_PRE_FAIL_CSN,           /* 全局 m_rowCSN 与事务内快照不一致 */
    OCC_PRE_FAIL_NOT_LATEST,    /* is_latest 为 false */
    OCC_PRE_FAIL_INSERT,        /* INSERT 行校验失败 */
};

void O_OccStatsRecordCommit();
void O_OccStatsRecordAbort(OccAbortReason reason);
void O_OccStatsRecordPreAbortDetail(OccPreAbortFailReason reason);

/* 将当前累计计数写入 pg_log（LOG 级别，前缀 OCC_STATS） */
void O_OccStatsLogSummary();

#endif  // OPENGAUSS_OCC_STATS_H
