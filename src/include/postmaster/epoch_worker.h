#ifndef EPOCH_WORKER_H
#define EPOCH_WORKER_H

extern void EpochApplyWorkerMain(void);
extern void EpochMergeWorkerMain(void);
extern void EpochCoordWorkerMain(void);
extern void EpochSendWorkerMain(void);
extern void EpochTipFlushWorkerMain(void);
extern void EpochWorkerInitApplySession(void);

#endif /* EPOCH_WORKER_H */
