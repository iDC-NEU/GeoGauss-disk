#ifndef OPENGAUSS_E_EPOCH_H
#define OPENGAUSS_E_EPOCH_H

#include <cstdint>

extern void EpochAdvancePhysicalOnce(void);
extern void EpochAdvancePhysicalBurst(void);
extern bool EpochIsLogicalReady(uint64_t logicalEpoch);
extern bool EpochTryAdvanceLogical(void);
extern bool EpochWaitForCommitGate(uint64_t commitEpoch);
extern bool EpochWaitForLogicalPast(uint64_t commitEpoch);
extern uint64_t EpochAssignTxnEpoch(void);
/* 本节点 assign 槽：满则 false，backend 等待 local physical 推进 */
extern bool EpochTryReservePhysicalTxnSlot(uint64_t* out_slot_num);
extern uint64_t EpochGetPhysicalTxnSinceAdvance(void);
extern bool EpochLocalTxnExcComplete(uint64_t epochId);
extern bool EpochAllNodesSendCompleted(uint64_t epochId);
extern bool EpochIsLocalCommitStageReady(uint64_t epochId);

#endif /* OPENGAUSS_E_EPOCH_H */
