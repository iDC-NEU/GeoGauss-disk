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

size_t EpochSendQueueApproxSize(void);

#endif /* OPENGAUSS_E_SEND_QUEUE_H */
