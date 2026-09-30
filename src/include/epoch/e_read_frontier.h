#ifndef OPENGAUSS_E_READ_FRONTIER_H
#define OPENGAUSS_E_READ_FRONTIER_H

#include "postgres.h"

/*
 * wait：事务 START 时钉扎 read_frontier = logical，读路径只见 commit_epoch ≤ frontier 的版本。
 * gate=on 时不启用，由 START gate 保证堆可见性。
 */
extern void EpochPinReadFrontierOnStart(void);
extern void EpochResetReadFrontier(void);
extern bool EpochReadFrontierActive(void);
extern uint64_t EpochGetReadFrontier(void);

/*
 * prune 安全 frontier：min(各 backend pinned frontier)；无 pin 时用 logical-KEEP。
 * 仅当后继版本 m_status<=该值时，才可回收被取代的中间版本。
 */
extern uint64_t EpochGetPruneSafeFrontier(void);

/* heap 写入时记录版本所属 logical epoch（存于 HeapTupleHeader.m_status） */
extern void EpochHeapSetWriteEpoch(uint64_t epoch);
extern uint64_t EpochHeapCommitEpochForWrite(void);

#endif /* OPENGAUSS_E_READ_FRONTIER_H */
