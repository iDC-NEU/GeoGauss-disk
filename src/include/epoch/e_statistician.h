#ifndef OPENGAUSS_E_STATISTICIAN_H
#define OPENGAUSS_E_STATISTICIAN_H

#include <cstdint>

#include "epoch/e_txn_wire.h"

bool EpochIsAsyncMultimasterMode(void);
bool EpochBuildEpochEndWire(uint64_t epoch, E_WireTxn* out);
void EpochOnEpochEndWireReceived(const E_WireTxn& txn);

bool EpochCheckAndSetNodeReceiveCompleted(uint64_t epochId, uint64_t nodeId);
bool EpochIsAllNodesReceiveCompleted(uint64_t epochId);
/* 不含本端：本端 recv 在 self seal 后才完成 */
bool EpochIsAllPeersReceiveCompleted(uint64_t epochId);

bool EpochTrySealEpoch(uint64_t epoch);
void EpochDiagReset(void);
void EpochDiagMaybeReport(void);
void EpochRecvDiagReset(void);
void EpochRecvDiagLogSent(uint64_t epoch, uint64_t sent_count);
void EpochRecvDiagLogSealBlock(uint64_t epoch, const char* reason);
void EpochRecvDiagLogEpochEnd(uint64_t epoch, uint32_t from_node, uint64_t expected_pack);
void EpochRecvDiagOnApply(uint64_t epoch, uint64_t row_count, bool ok);
void EpochRecvDiagLogApplyIssue(uint64_t epoch, const char* issue, uint64_t merged, uint64_t build_rows);
void EpochRecvDiagLogApplySkip(uint64_t epoch, const char* reason, uint64_t merged, uint64_t committed,
    uint64_t peer_should, uint64_t peer_rcv, int has_remote_rows);
void EpochRecvDiagLogMergePkFail(uint32_t relid, uint32_t local_relid, const char* relname, EpochOpType op);
void EpochRecvDiagLogStall(uint64_t epoch, const char* reason);
void EpochWireDiagLogTxnSend(const E_WireTxn& txn);
void EpochWireDiagLogTxnRecv(const E_WireTxn& txn);
void EpochRecvDiagLogHello(const char* tag, uint32_t from_node);
void EpochPublishTxnRequest(uint64_t epoch);
void EpochHandleTxnRequest(const E_WireTxn& txn);
void EpochMaybeRequestMissingTxns(void);
void EpochOnPeerConnected(uint32_t peerId);

/* 当前 logical epoch 尚有写事务 pack 未齐，不可继续推高 physical */
bool EpochLogicalEpochHasPendingPack(uint64_t epoch);

/* 空 seal 后 change_set/pack 增加时撤销 send_completed，以便 re-seal */
void EpochMaybeInvalidateStaleSeal(uint64_t epoch);

/* 对端 epoch-end 已收到的最高 epoch（用于限制本端超前 seal/advance） */
uint64_t EpochGetPeerMaxSealedEpoch(uint32_t peer_node);
void EpochNotePeerSealedEpoch(uint32_t peer_node, uint64_t epoch);
/* logical 已越过的 epoch 因 late pack 重开 slot 时，恢复对端 recv 完成态 */
void EpochRestorePastEpochPeerFlags(uint64_t epoch);

#endif /* OPENGAUSS_E_STATISTICIAN_H */
