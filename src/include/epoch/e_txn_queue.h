#ifndef OPENGAUSS_E_TXN_QUEUE_H
#define OPENGAUSS_E_TXN_QUEUE_H

#include <cstdint>

#include "storage/lock/lwlock.h"

#include "epoch/e_types.h"   /* EPOCH_MAX_NODES */



extern Size EpochTxnQueueShmemSize(uint32_t capacity);
extern void EpochTxnQueueShmemInit(uint32_t capacity);
/*
 * 发送队列按 peer 拆分：每个对端一份独立的 epoch 环形队列。
 *
 * 目的是让"某个对端慢"只阻塞它自己那条发送线程，不影响其它对端
 * （原来只有一份队列 + 单线程串行发送，慢 peer 会把整条流水线拖停）。
 */
extern bool EpochTxnQueuePush(uint32_t peer, uint64_t epoch, const char* data, uint32_t len);
/* 从指定 peer、指定 epoch 的环弹出；空则 false */
extern bool EpochTxnQueuePop(uint32_t peer, uint64_t epoch, char* data, uint32_t max_len,
    uint32_t* out_len, uint32_t* out_index);
/* 所有 peer 合计 / 单个 peer */
extern uint32_t EpochTxnQueueCount(void);
extern uint32_t EpochTxnQueueCountPeer(uint32_t peer);

#endif /* OPENGAUSS_E_TXN_QUEUE_H */
