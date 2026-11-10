#ifndef OPENGAUSS_E_PERF_H
#define OPENGAUSS_E_PERF_H

#include <cstdint>

#include "epoch/e_types.h"

struct E_WireTxn;

/* backend START/COMMIT gate 等待（微秒累计） */
enum EpochPerfGateKind : uint8_t {
    EPOCH_PERF_GATE_START = 0,
    EPOCH_PERF_GATE_LOGICAL_COMMIT = 1,
    EPOCH_PERF_GATE_PACK_ADMIT = 2,
};

bool EpochPerfIsEnabled(void);
void EpochPerfReset(void);
void EpochPerfMaybeReport(void);

/* 按 epoch 粒度的粗延迟：封口时刻 -> apply 完成时刻 */
void EpochPerfNoteEpochSeal(uint64_t epoch);
void EpochPerfNoteEpochApplied(uint64_t epoch);
uint64_t EpochPerfEpochLatAvgUs(void);

/* 快照闸门超时放行的累计次数 */
void EpochPerfNoteSnapTimeout(void);
uint64_t EpochPerfSnapTimeoutCount(void);

void EpochPerfAddGateWait(EpochPerfGateKind kind, uint64_t wait_us, uint64_t spin_count);
void EpochPerfNoteLogicalAdvanceOk(void);
void EpochPerfNoteLogicalAdvanceTry(void);
void EpochPerfNoteLogicalStall(const char* reason);
void EpochPerfNoteSealBlock(const char* reason);
void EpochPerfNoteSealOk(void);
void EpochPerfNoteWorkerPoll(void);
void EpochPerfNotePhysicalAdvance(void);

void EpochPerfNoteWireTxnSend(const E_WireTxn& txn, uint64_t wire_bytes);
void EpochPerfNoteEpochEndSend(uint64_t wire_bytes);
void EpochPerfNoteWireRecv(const E_WireTxn& txn, uint64_t wire_bytes);
void EpochPerfNoteHelloSend(void);
void EpochPerfNoteTxnRequestSend(void);
void EpochPerfNoteApplyCommitTxn(void);
void EpochPerfNoteApplyAbortTxn(void);
/* 一次加 n 笔 abort（时间片推进时汇总该片 merge 输家用） */
void EpochPerfAddApplyAbortTxn(uint64_t n);
/* apply 任务入队 → 本节点写回完成的延迟（微秒），按 NewOrder/Payment 分类计数 */
/* 事务起点(XACT START) → 本节点写回完成的延迟（微秒），仅统计本节点发起的事务 */
/* 输出 APPLY_LAT 统计行 */
/* 输出 COMMIT_LAT 统计行 */
void EpochPerfNoteMergeHeapAbort(void);

#endif /* OPENGAUSS_E_PERF_H */

/* 诊断日志统一时间戳：HH:MM:SS.mmm（thread_local 缓冲，可直接内联使用） */
const char* EpochDbgTs(void);

/*
 * 所有诊断打印统一走这个宏，保证每行都带同一格式的时间戳，
 * 这样三节点的 beat / eof_enq / mark_mc 才能按时刻对齐比较。
 */
/* 诊断写独立文件，不进 pg_log（prep 会清 pg_log，重跑就丢） */
void EpochDiagWrite(const char* fmt, ...);

#ifndef EPOCH_DBG_FPRINTF
#define EPOCH_DBG_FPRINTF(...) do { \
        EpochDiagWrite(__VA_ARGS__); \
    } while (0)
#endif
