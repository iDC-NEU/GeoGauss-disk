#ifndef OPENGAUSS_E_SEND_QUEUE_H
#define OPENGAUSS_E_SEND_QUEUE_H

#include <cstdint>
#include <string>

/*
 * Pack → Send 解耦队列（对齐 GeoGauss：Pack×N 入队，Send×1 出队发 ZMQ）。
 * wire 为序列化后的整包；epoch-end 与普通 txn 共用同一队列保序。
 */
struct EpochSendItem {
    uint64_t epoch = 0;
    bool is_epoch_end = false;
    std::string wire;
};

void EpochSendQueueInit(void);
void EpochSendQueueShutdown(void);

bool EpochSendEnqueue(uint64_t epoch, bool is_epoch_end, const std::string& wire);
bool EpochSendTryDequeue(EpochSendItem* out);
/* epoch-end 单独排队，上行暂停数据写集时仍可先发出去。 */
bool EpochSendTryDequeueEpochEnd(EpochSendItem* out);

size_t EpochSendQueueApproxSize(void);

/* 已入队、尚未送进 ZMQ 的数据字节。epoch-end 不计入。 */
uint64_t EpochSendOutstandingBytes(void);
void EpochSendNoteSent(size_t nbytes);

#endif /* OPENGAUSS_E_SEND_QUEUE_H */
