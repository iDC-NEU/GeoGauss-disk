#ifndef OPENGAUSS_E_TYPES_H
#define OPENGAUSS_E_TYPES_H

#include <cstdint>

/* 环形缓存槽位数；须大于 physical-logical 最大滞后，避免计数覆盖 */
static constexpr uint32_t EPOCH_DEFAULT_CACHE_SLOTS = 8192;
static constexpr uint32_t EPOCH_MAX_NODES = 16;
static constexpr uint32_t EPOCH_MAX_NODE_ID = EPOCH_MAX_NODES - 1;
/* BuildMergedTxn 落盘标记：区别于本节点原始 wire，允许合并结果在本地 apply */
static constexpr uint32_t EPOCH_MERGED_SERVER_ID = UINT32_MAX;

/* 逻辑 epoch 相对物理 epoch 的延迟窗口（GeoGauss delay_epoch_num 默认 0） */
static constexpr uint32_t EPOCH_DEFAULT_DELAY = 0;

/* 共享内存事务队列槽位数（每槽最大 EPOCH_MAX_WIRE_TXN_BYTES） */
static constexpr uint32_t EPOCH_TXN_QUEUE_CAPACITY = 8192;
/* e_perf gate 种类数（与 EpochPerfGateKind 一致） */
static constexpr uint32_t EPOCH_PERF_GATE_COUNT = 6;

enum EpochOpType : uint8_t {
    EPOCH_OP_READ = 0,
    EPOCH_OP_INSERT = 1,
    EPOCH_OP_UPDATE = 2,
    EPOCH_OP_DELETE = 3,
};

enum EpochTxnState : uint8_t {
    EPOCH_TXN_EMPTY = 0,
    EPOCH_TXN_ABORT = 1,
    EPOCH_TXN_COMMIT = 2,
};

enum EpochTxnType : uint8_t {
    EPOCH_TXN_CLIENT = 0,
    EPOCH_TXN_REMOTE = 1,
    EPOCH_TXN_EPOCH_END = 2,
    EPOCH_TXN_COMMITTED = 3,
};

#endif /* OPENGAUSS_E_TYPES_H */
