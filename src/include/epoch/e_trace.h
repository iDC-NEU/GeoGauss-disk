#ifndef OPENGAUSS_E_TRACE_H
#define OPENGAUSS_E_TRACE_H

#include <cstdint>

/* 改为 0 并重新编译即可关闭 epoch_sync_trace.log 输出 */
#define EPOCH_SYNC_TRACE_ENABLED 0

void EpochTraceLogSealPrepare(uint64_t epoch, uint64_t txn_total);
void EpochTraceMaybeLogSealBlocked(uint64_t epoch, const char* reason, uint64_t executing,
    uint64_t change_set, uint64_t packed);
void EpochTraceMaybeLogPeerReceived(uint64_t epoch);
void EpochTraceMaybeLogMergeSummary(uint64_t epoch, uint64_t need_merge, uint64_t merged_success);
void EpochTraceLogApplyDone(uint64_t epoch, uint64_t applied_count);

#endif /* OPENGAUSS_E_TRACE_H */
