#ifndef OPENGAUSS_E_TXN_QUEUE_H
#define OPENGAUSS_E_TXN_QUEUE_H

#include <cstdint>

#include "storage/lock/lwlock.h"

/*
 * 本地 pack 出站缓冲：按 epoch%N 分环（N=WorkQueueSlotCount，与 CRDT 同）。
 * Push(epoch, …)；Drain 用 PopAny 轮询各环。
 */
struct EpochTxnEpochRing {
    uint64_t bound_epoch;
    uint32_t head;
    uint32_t tail;
    uint32_t count;
};

struct EpochTxnQueueHeader {
    LWLock* lock;
    uint32_t slot_count;
    uint32_t slots_per_epoch;
    uint32_t total_capacity;
};

extern Size EpochTxnQueueShmemSize(uint32_t capacity);
extern void EpochTxnQueueShmemInit(uint32_t capacity);
extern bool EpochTxnQueuePush(uint64_t epoch, const char* data, uint32_t len);
/* 从指定 epoch 环弹出；空则 false */
extern bool EpochTxnQueuePop(uint64_t epoch, char* data, uint32_t max_len, uint32_t* out_len);
/* 轮询非空环弹出（DrainLocalQueue） */
extern bool EpochTxnQueuePopAny(char* data, uint32_t max_len, uint32_t* out_len, uint64_t* out_epoch);
extern uint32_t EpochTxnQueueCount(void);

#endif /* OPENGAUSS_E_TXN_QUEUE_H */
