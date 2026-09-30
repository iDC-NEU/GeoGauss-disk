#ifndef OPENGAUSS_E_EXEC_TRACK_H
#define OPENGAUSS_E_EXEC_TRACK_H

/*
 * XACT START：Assign 后立即 assign_on_start（executing +1），再 START gate。
 * 事务 END（no_occ / 写 / 只读）E_ResetTxnEpochContext，下一条 START 重新 Assign。
 */
void EpochEnsureTxnEpochOnAccess(bool is_write);
void EpochMaybeMarkTxnExecuting(void);
void EpochTrackExecutingInc(uint64_t epoch, const char* reason);
void EpochReleaseTxnExecuting(const char* reason);
void EpochMaybeLogTxnProgress(const char* tag, const char* detail);

#endif /* OPENGAUSS_E_EXEC_TRACK_H */
