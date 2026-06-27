#ifndef OPENGAUSS_E_DYN_NET_H
#define OPENGAUSS_E_DYN_NET_H

#include <cstdint>
#include <string>
#include <vector>

struct E_WireTxn;
#include "dn_types.h"

bool EpochDynNetIsEnabled(void);
bool EpochDynNetProxyEnabled(void);
bool EpochDynNetIsProxySlowNode(void);
void EpochDynNetInit(void);
void EpochDynNetShutdown(void);

/* Shaped publish/receive — drop-in for EpochNetPublish / TryReceive when enabled */
bool EpochDynNetPublish(const std::string& payload);
bool EpochDynNetPublishWireTxn(const std::string& wire, const E_WireTxn& txn);
bool EpochDynNetTryReceive(std::string* payload);
void EpochDynNetFlushOutgoing(void);

/* Envelope dispatch (metadata / partial snapshot) — returns true if consumed */
bool EpochDynNetHandleWireEnvelope(const std::string& wire);

/* Feed observer + optional CATCHUP on worker tick */
void EpochDynNetObserveWireTxn(const E_WireTxn& txn);
void EpochDynNetObserveEpochEnd(uint64_t epoch);
void EpochDynNetEnqueueIncoming(const std::string& payload, int from_node, size_t raw_bytes);
void EpochDynNetMaybeCatchup(void);
void EpochDynNetMaybeEmitProxySnapshot(uint64_t epoch);
void EpochDynNetGcBeforeEpoch(uint64_t epoch);
void EpochDynNetDrainAndDispatch(void);

void EpochDynNetRecordWireForProxy(const E_WireTxn& txn, int32_t channel_id);

/* Wire ↔ dynamic-net bridge */
DnRaftEntry EpochDynNetWireToRaftEntry(const E_WireTxn& txn);
E_WireTxn EpochDynNetDnTxnToWire(const DnTransaction& txn, int32_t epoch);
uint64_t EpochDynNetWireEntryHash(const E_WireTxn& txn);

#endif /* OPENGAUSS_E_DYN_NET_H */
