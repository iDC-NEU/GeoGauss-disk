#ifndef OPENGAUSS_E_EPOCH_H
#define OPENGAUSS_E_EPOCH_H

#include <cstdint>

/*
 * 给指定时间片封口：塞收尾包，并把 physical 推进到下一片。
 * 由 backend 抢到“每片第 24 个名额”时调用。
 */
extern void EpochSealEpoch(uint64_t epoch);
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

/* 每隔 10 秒打印一次各游标状态（由独立线程调用，不参与业务锁） */
extern void EpochStateLog(void);
