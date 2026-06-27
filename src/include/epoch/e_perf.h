#ifndef OPENGAUSS_E_PERF_H
#define OPENGAUSS_E_PERF_H

#include <cstdint>

#include "epoch/e_types.h"

struct E_WireTxn;

/* backend START/COMMIT gate 等待（微秒累计） */
enum EpochPerfGateKind : uint8_t {
    EPOCH_PERF_GATE_START = 0,
    EPOCH_PERF_GATE_LOGICAL_COMMIT = 1,
    EPOCH_PERF_GATE_REMOTE_EXEC = 2,
    EPOCH_PERF_GATE_REMOTE_RECORD = 3,
    EPOCH_PERF_GATE_ASYNC_ADMIT = 4,
    EPOCH_PERF_GATE_ASYNC_PACK = 5,
};

bool EpochPerfIsEnabled(void);
void EpochPerfReset(void);
void EpochPerfMaybeReport(void);

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

#endif /* OPENGAUSS_E_PERF_H */
