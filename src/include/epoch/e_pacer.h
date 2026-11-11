#ifndef OPENGAUSS_E_PACER_H
#define OPENGAUSS_E_PACER_H

#include <cstdint>

struct E_WireTxn;

/* Pacer 同步层：DBR 改期与 PA 代理转发。 */

int EpochPacerDecideItem(uint64_t epoch, uint64_t csn, uint32_t server_id, bool is_eof,
    uint64_t* out_target);
void EpochPacerSweepDecisions(uint64_t up_to_epoch);

bool EpochPacerDbrEnabled(void);
bool EpochPacerIsSlowSelf(void);
bool EpochPacerPaEnabled(void);

void EpochPacerPackTick(void);

uint32_t EpochPacerPaProxyNodeId(void);
bool EpochPacerPaShouldSkipPeer(uint32_t peer, uint64_t epoch);
void EpochPacerPaNoteEpochDecision(uint64_t epoch, bool skip, uint32_t from_node);
void EpochPacerPaNoteDecideRequest(uint64_t epoch);

void EpochPacerPaMaybeBufferMerged(const E_WireTxn& txn);
void EpochPacerPaResendEpoch(uint64_t epoch);

void EpochPacerPaNoteSlowState(const E_WireTxn& ctl);
void EpochPacerPaRestoreState(void);

void EpochPacerNoteMergeRetry(void);
uint64_t EpochPacerMergeRetryCount(void);
void EpochPacerNoteDeferred(void);
uint64_t EpochPacerDeferredCount(void);

#endif /* OPENGAUSS_E_PACER_H */
