#ifndef OPENGAUSS_E_STATISTICIAN_H
#define OPENGAUSS_E_STATISTICIAN_H

#include <cstdint>

#include "epoch/e_txn_wire.h"

bool EpochBuildEpochEndWire(uint64_t epoch, E_WireTxn* out);
void EpochOnEpochEndWireReceived(const E_WireTxn& txn);

bool EpochCheckAndSetNodeReceiveCompleted(uint64_t epochId, uint64_t nodeId);
bool EpochIsAllNodesReceiveCompleted(uint64_t epochId);

bool EpochTrySealEpoch(uint64_t epoch);
void EpochDiagReset(void);
void EpochDiagMaybeReport(void);
void EpochRecvDiagReset(void);
void EpochRecvDiagLogSent(uint64_t epoch, uint64_t sent_count);
void EpochRecvDiagLogSealBlock(uint64_t epoch, const char* reason);
void EpochRecvDiagLogEpochEnd(uint64_t epoch, uint32_t from_node, uint64_t expected_pack);
void EpochRecvDiagOnApply(uint64_t epoch, uint64_t row_count, bool ok);
void EpochRecvDiagLogStall(uint64_t epoch, const char* reason);
void EpochWireDiagLogTxnSend(const E_WireTxn& txn);
void EpochWireDiagLogTxnRecv(const E_WireTxn& txn);
void EpochRecvDiagLogHello(const char* tag, uint32_t from_node);
void EpochPublishTxnRequest(uint64_t epoch);
void EpochHandleTxnRequest(const E_WireTxn& txn);
void EpochMaybeRequestMissingTxns(void);
void EpochOnPeerConnected(uint32_t peerId);

/* 当前 logical epoch 尚有 change_set 未 pack/abort 完，不可继续推高 physical */
bool EpochLogicalEpochHasPendingPack(uint64_t epoch);

#endif /* OPENGAUSS_E_STATISTICIAN_H */
