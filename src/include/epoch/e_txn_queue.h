#ifndef OPENGAUSS_E_TXN_QUEUE_H
#define OPENGAUSS_E_TXN_QUEUE_H

#include <cstdint>

#include "storage/lock/lwlock.h"

struct EpochTxnQueueHeader {
    LWLock* lock;
    uint32_t capacity;
    uint32_t head;
    uint32_t tail;
    uint32_t count;
};

extern Size EpochTxnQueueShmemSize(uint32_t capacity);
extern void EpochTxnQueueShmemInit(uint32_t capacity);
extern bool EpochTxnQueuePush(const char* data, uint32_t len);
extern bool EpochTxnQueuePop(char* data, uint32_t max_len, uint32_t* out_len);
extern uint32_t EpochTxnQueueCount(void);

#endif /* OPENGAUSS_E_TXN_QUEUE_H */
