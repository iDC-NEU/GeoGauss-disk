#ifndef OPENGAUSS_E_MERGE_PROFILE_H
#define OPENGAUSS_E_MERGE_PROFILE_H

#include <cstdint>

enum EpochMergeProfilePhase {
    EMP_DESERIALIZE = 0,
    EMP_PREPARE_ROWS,
    EMP_PK_HEAP_OPEN,
    EMP_PK_TUPLE_DECODE,
    EMP_PK_INDEX_ATTRS,
    EMP_ENSURE_SLOT,
    EMP_COLLECT_SHARDS,
    EMP_SHARD_ACQUIRE,
    EMP_PREVIEW,
    EMP_MERGE_ROWS,
    EMP_SHARD_RELEASE,
    EMP_POST_HOOKS,
    EMP_QUEUE_WAIT,
    EMP_QUEUE_FINISH,
    EMP_PHASE_COUNT
};

extern bool g_epoch_merge_profile;

extern void EpochMergeProfileBeginWireTxn(void);
extern void EpochMergeProfileAddUs(EpochMergeProfilePhase phase, uint64_t us);
extern void EpochMergeProfileSetRowCount(uint64_t rows);
extern void EpochMergeProfileSetShardCount(uint64_t shards);
extern void EpochMergeProfileEndWireTxn(uint64_t epoch, uint64_t csn, uint32_t server_id, bool ok);

/*
 * 卡死定位（仅诊断）：记录每个 merge worker 当前正在执行的 task 与阶段。
 * 无 gdb 环境下靠它判断「CLAIMED 之后究竟停在哪一步」。
 */
extern void EpochMergeProfileSetWorker(int worker_id);
extern void EpochMergeProfileNoteTask(uint64_t epoch, uint64_t wire_key);
extern void EpochMergeProfileNotePhase(EpochMergeProfilePhase phase);
extern void EpochMergeProfileNoteTxnDone(void);
/* 周期调用（epochphys 线程 10ms 一跳，内部限流 5s），有超时 task 时打 MERGE_STUCK */
extern void EpochMergeProfileMaybeReportStuck(void);

#endif /* OPENGAUSS_E_MERGE_PROFILE_H */
