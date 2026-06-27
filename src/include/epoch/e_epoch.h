#ifndef OPENGAUSS_E_EPOCH_H
#define OPENGAUSS_E_EPOCH_H

#include <cstdint>

extern void EpochAdvancePhysicalOnce(void);
extern void EpochAdvancePhysicalBurst(void);
extern bool EpochIsLogicalReady(uint64_t logicalEpoch);
extern bool EpochTryAdvanceLogical(void);
extern bool EpochWaitForCommitGate(uint64_t commitEpoch);
extern bool EpochIsRemoteMergeReady(uint64_t epochId);
extern bool EpochWaitForRemoteMerge(uint64_t commitEpoch);
extern bool EpochWaitForLogicalPast(uint64_t commitEpoch);
extern bool EpochWaitForSyncCommit(uint64_t commitEpoch);
extern bool EpochWaitForRecordCommitted(uint64_t startEpoch);
extern uint64_t EpochAssignTxnEpoch(void);
extern bool EpochLocalTxnExcComplete(uint64_t epochId);

#endif /* OPENGAUSS_E_EPOCH_H */
