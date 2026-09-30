#ifndef OPENGAUSS_E_STATISTICIAN_H
#define OPENGAUSS_E_STATISTICIAN_H

#include <cstdint>

#include <vector>

#include "epoch/e_txn_wire.h"

/* sync 多主：客户端 pack 即返回，merge/apply/关窗由 epoch worker 后台完成 */
bool EpochIsSyncBackgroundWorkerCommit(void);
/* sync 多主客户端 backend：跳过 COMMIT/ABORT WAL 与 XLogWaitFlush；heap 持久化由 apply 负责 */
bool EpochShouldSkipBackendWal(void);
bool EpochBuildEpochEndWire(uint64_t epoch, E_WireTxn* out);
void EpochOnEpochEndWireReceived(const E_WireTxn& txn);

bool EpochCheckAndSetNodeReceiveCompleted(uint64_t epochId, uint64_t nodeId);
bool EpochIsAllNodesReceiveCompleted(uint64_t epochId);
/* 不含本端：本端 recv 在 self seal 后才完成 */
bool EpochIsAllPeersReceiveCompleted(uint64_t epochId);

bool EpochTrySealEpoch(uint64_t epoch);
void EpochDiagReset(void);
void EpochDiagMaybeReport(void);
/* logical advance stall 时写入 epoch_diag（不依赖 recv_diag_verbose） */
void EpochDiagLogEpochStallSnapshot(uint64_t epoch, const char* reason);
/* merge 输家 / resource_failure 写入 epoch_diag，并校验 abort_csn */
void EpochDiagLogMergeLoserAbortCsn(uint64_t epoch, uint64_t csn, uint32_t server_id, bool resource_failure,
    bool in_abort_csn);
/* apply 关窗卡住时 dump 首笔 pending wire txn */
void EpochDiagLogApplyPendingFirst(uint64_t epoch, uint64_t csn, uint32_t server_id, uint64_t wire_key,
    bool in_abort_csn, bool has_writable_rows, bool merge_inflight, bool apply_inflight);
void EpochRecvDiagReset(void);
void EpochRecvDiagLogSent(uint64_t epoch, uint64_t sent_count);
void EpochRecvDiagLogSealBlock(uint64_t epoch, const char* reason);
void EpochRecvDiagLogEpochEnd(uint64_t epoch, uint32_t from_node, uint64_t expected_pack);
/* epoch-end wire 全路径诊断：ARRIVE/ENTER/STALE/FUTURE_DROP/DUP/RESYNC/PROCESSED */
void EpochRecvDiagLogEpochEndEvent(const char* tag, uint64_t epoch, uint32_t from_node, uint64_t pack,
    uint64_t logical, int already_send, uint64_t prev_should, int recv_done);
/* merge 握手：g_peer_epoch_end_received 已置位 */
void EpochRecvDiagLogPeerEpochEndMark(uint64_t epoch, uint32_t from_node, uint64_t logical);
void EpochRecvDiagLogPeerTxnWireDone(uint64_t epoch, uint32_t from_node, uint64_t should, uint64_t got);
/* merge 未完成时各 peer 的 end/send/recv 状态 */
void EpochRecvDiagLogMergePeerEndGate(uint64_t epoch, uint64_t logical, const char* peer_detail);
/* 首次或重传确认后的 pack 计数摘要（RECV_OK / RECV_MISMATCH） */
void EpochRecvDiagOnApply(uint64_t epoch, uint64_t row_count, bool ok);
void EpochRecvDiagLogApplyIssue(uint64_t epoch, const char* issue, uint64_t merged, uint64_t build_rows);
void EpochRecvDiagLogApplySkip(uint64_t epoch, const char* reason, uint64_t merged, uint64_t committed,
    uint64_t peer_should, uint64_t peer_rcv, int has_remote_rows);
void EpochRecvDiagLogApplyTrace(uint64_t epoch, const char* tag, uint64_t merged, uint64_t committed,
    uint64_t peer_should, uint64_t peer_rcv, int remote_work, int merger_found, int has_remote_rows,
    int has_rows, uint64_t build_rows);
void EpochRecvDiagLogApplyBegin(uint64_t epoch, uint64_t build_rows, uint64_t merged, uint64_t committed);
void EpochRecvDiagLogWireRelSummary(const char* tag, uint64_t epoch, uint64_t csn, uint32_t from_node,
    const std::vector<E_WireRow>& rows);
void EpochRecvDiagLogLogicalApplyGate(uint64_t epoch, int apply_ok, int merger_pending, uint64_t merged,
    uint64_t committed);
void EpochRecvDiagLogLogicalAdvanceFail(uint64_t epoch, const char* reason);
void EpochRecvDiagLogLogicalAdvanceOk(uint64_t epoch, uint64_t elapsed_us, uint64_t stall_recv,
    uint64_t stall_send, uint64_t stall_other);
void EpochRecvDiagLogHandshakeStall(uint64_t epoch, const char* reason, const char* sub_reason);
void EpochLogicalAdvanceTraceReset(uint64_t epoch);
void EpochLogicalAdvanceTraceNoteStall(const char* reason);
void EpochLogicalAdvanceTraceOnOk(uint64_t epoch);
const char* EpochHandshakeSendSubReason(uint64_t epoch);
void EpochRecvDiagLogMergePkFail(uint32_t relid, uint32_t local_relid, const char* relname, EpochOpType op);
void EpochRecvDiagLogMergeHeapAbort(uint64_t epoch, uint64_t txn_csn, uint32_t server_id, const char* relname,
    uint32_t relid, EpochOpType op, bool row_found, uint64_t heap_csn, uint64_t expected_csn);
void EpochRecvDiagLogStall(uint64_t epoch, const char* reason);
/* backend 自旋 gate / COMMIT 阶段（需 epoch_recv_diag=on） */
void EpochRecvDiagLogBackendGate(const char* gate, uint64_t commit_epoch, uint64_t spins);
void EpochRecvDiagLogBackendGateDone(const char* gate, uint64_t commit_epoch, uint64_t spins,
    uint64_t logical_entry, uint64_t logical_exit, uint64_t wait_us);
void EpochRecvDiagLogBackendPhase(const char* phase, uint64_t commit_epoch);
void EpochRecvDiagLogPhysicalTxnLimit(uint64_t commit_epoch, uint64_t since, uint64_t limit, uint64_t spins);
void EpochRecvDiagLogPhysicalTxnReserved(uint64_t commit_epoch, uint64_t slot_num, uint64_t limit);
void EpochRecvDiagLogPhysicalAdvance(uint64_t from_epoch,
    uint64_t to_epoch,
    uint64_t since,
    uint64_t limit,
    int idle_force);
/* executing +/-1、事务 hook、OCC 在途进度（需 epoch_recv_diag=on） */
void EpochRecvDiagLogExecChange(int delta, const char* reason, uint64_t epoch);
void EpochRecvDiagLogXactHook(const char* event, const char* detail);
void EpochRecvDiagLogTxnProgress(const char* tag, const char* detail);
void EpochWireDiagLogTxnSend(const E_WireTxn& txn);
void EpochWireDiagLogTxnRecv(const E_WireTxn& txn);
void EpochRecvDiagLogHello(const char* tag, uint32_t from_node);
void EpochRecvDiagLogEpochEndSend(const char* result, uint64_t epoch, uint64_t wire_bytes);
void EpochOnPeerConnected(uint32_t peerId);
/* merge 等待 peer epoch-end 超时/卡住时重发本端已 seal 的 epoch-end */
void EpochRetransmitSealedEpochEnd(uint64_t epoch);

/* 当前 logical epoch 尚有写事务 pack 未齐，不可继续推高 physical */
bool EpochLogicalEpochHasPendingPack(uint64_t epoch);

/* 单 seal 策略：seal 后不再撤销 send_completed */
void EpochMaybeInvalidateStaleSeal(uint64_t epoch);

/* 对端 epoch-end 已收到的最高 epoch（用于限制本端超前 seal/advance） */
uint64_t EpochGetPeerMaxSealedEpoch(uint32_t peer_node);
void EpochNotePeerSealedEpoch(uint32_t peer_node, uint64_t epoch);
/* logical 已越过的 epoch 因 late pack 重开 slot 时，恢复对端 recv 完成态 */
void EpochRestorePastEpochPeerFlags(uint64_t epoch);

#endif /* OPENGAUSS_E_STATISTICIAN_H */
