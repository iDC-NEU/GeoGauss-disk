#ifndef OPENGAUSS_E_EPOCH_CAPACITY_H
#define OPENGAUSS_E_EPOCH_CAPACITY_H

#include <cstdint>

#include "epoch/e_types.h"

/*
 * 运行时容量由 postmaster GUC 决定（见 e_config.cpp）；以下为编译期默认值与上限。
 * 共享内存按 GUC 在启动时一次性分配，运行中不可变。
 */
static constexpr uint32_t EPOCH_DEFAULT_TXN_QUEUE_CAPACITY = 8192;
static constexpr uint32_t EPOCH_DEFAULT_ABORT_CSN_PER_EPOCH = 65536;
static constexpr uint32_t EPOCH_DEFAULT_CRDT_SLOT_COUNT = 32;
static constexpr uint32_t EPOCH_DEFAULT_CRDT_ROWS_PER_SLOT = 8192;
static constexpr uint32_t EPOCH_DEFAULT_CRDT_INSERTS_PER_SLOT = 2048;
/* 未来 epoch 按格位预放；容量须覆盖 [merge_epoch, physical) 在途，与 apply 同档 */
static constexpr uint32_t EPOCH_DEFAULT_MERGE_QUEUE_CAPACITY = 8192;
static constexpr uint32_t EPOCH_DEFAULT_MERGE_DONE_CAPACITY = 8192;
/*
 * GeoGauss：apply 入环不许失败，满则 ERROR。
 * 实际每列深度 = capacity / (epoch 槽数 × apply 线程数)。
 * 32 槽 × 10 线程时，8192 只有深度 25。DBR 会把数百个写集堆进同一个 epoch，
 * 81920 对应深度 256（单 epoch 可容纳 2560），盖住这种堆积。
 */
static constexpr uint32_t EPOCH_DEFAULT_APPLY_QUEUE_CAPACITY = 81920;
static constexpr uint32_t EPOCH_DEFAULT_APPLY_DONE_CAPACITY = 8192;

extern uint32_t EpochGetTxnQueueCapacity(void);
extern uint32_t EpochGetAbortCsnPerEpoch(void);
extern uint32_t EpochGetCrdtSlotCount(void);
extern uint32_t EpochGetCrdtRowsPerSlot(void);
extern uint32_t EpochGetCrdtInsertsPerSlot(void);
extern uint32_t EpochGetMergeQueueCapacity(void);
extern uint32_t EpochGetMergeDoneCapacity(void);
extern uint32_t EpochGetApplyQueueCapacity(void);
extern uint32_t EpochGetApplyDoneCapacity(void);
/*
 * Apply/Merge/Txn/remote_merge 等工作队列的 epoch 分槽数。
 * 与 CRDT 相同：EpochGetCrdtSlotCount()（epoch%N + bound_epoch）。
 * 切勿用 epoch_cache_slots（默认 8192）：payload 队列每槽容量会归零/内存爆炸。
 */
extern uint32_t EpochGetWorkQueueSlotCount(void);

#endif /* OPENGAUSS_E_EPOCH_CAPACITY_H */
