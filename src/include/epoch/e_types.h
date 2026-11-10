#ifndef OPENGAUSS_E_TYPES_H
#define OPENGAUSS_E_TYPES_H

#include <cstdint>

/* 环形缓存槽位数；须大于 physical-logical 最大滞后，避免计数覆盖 */
/*
 * 共享内存计数器的槽位数，必须与队列格数 EPOCH_TXN_EPOCH_COUNT 一致：
 * 否则 epoch 与 epoch+槽数 落进同一格，旧时间片的计数被新片顶掉，
 * merged 读出来变 0，那片就永远合不完（实测 epoch 888 卡死）。
 */
static constexpr uint32_t EPOCH_DEFAULT_CACHE_SLOTS = 10000;
static constexpr uint32_t EPOCH_MAX_NODES = 16;
static constexpr uint32_t EPOCH_MAX_NODE_ID = EPOCH_MAX_NODES - 1;
/* BuildMergedTxn 落盘标记：区别于本节点原始 wire，允许合并结果在本地 apply */
static constexpr uint32_t EPOCH_MERGED_SERVER_ID = UINT32_MAX;

/* 逻辑 epoch 相对物理 epoch 的延迟窗口（GeoGauss delay_epoch_num 默认 0） */
static constexpr uint32_t EPOCH_DEFAULT_DELAY = 0;

/* 共享内存事务队列默认槽位数；运行时容量见 epoch_txn_queue_capacity GUC / EpochGetTxnQueueCapacity() */
static constexpr uint32_t EPOCH_TXN_QUEUE_CAPACITY = 8192;
static constexpr uint32_t EPOCH_MAX_APPLY_WORKERS = 64;
static constexpr uint32_t EPOCH_MAX_MERGE_WORKERS = 64;
static constexpr uint32_t EPOCH_MAX_SEND_WORKERS = 64;
/* tip flush：由 GUC epoch_tip_flush_threads 控制，上限见此常量 */
static constexpr uint32_t EPOCH_MAX_TIP_FLUSH_WORKERS = 16;
/* e_perf gate 种类数（与 EpochPerfGateKind 一致） */
static constexpr uint32_t EPOCH_PERF_GATE_COUNT = 3;
/* backend read_frontier 共享槽（按 pgprocno 索引；越界则不发布） */
static constexpr uint32_t EPOCH_FRONTIER_SLOTS = 4096;
/*
 * 无 pinned reader 时 prune 保留的 logical 代差。
 * 过小会在短事务重叠窗口误回收仍被 OCC 握着的 tip TID；TPCC 高并发下取大一些。
 */
static constexpr uint64_t EPOCH_PRUNE_KEEP_EPOCHS = 8;
/*
 * pinned frontier 落后 logical 超过该代差视为卡住/泄漏，改用 lag_cap，
 * 否则最慢 backend 会把 f_safe 钉死，prune 停摆 → 页膨胀 → 锁等待 → 更慢（正反馈）。
 */
static constexpr uint64_t EPOCH_PRUNE_STALE_PIN_EPOCHS = 64;
/* tip 链热路径单次最多回收条数 */
static constexpr int EPOCH_PRUNE_HOT_MAX_ITEMS = 32;
/* 页满 / 机会性路径：同页限量回收（含跨页后继的中间版本） */
static constexpr int EPOCH_PRUNE_PAGE_MAX_ITEMS = 128;
/* 兼容旧注释；大页不再跳过全页限量 prune */
static constexpr int EPOCH_PRUNE_FULL_MAXOFF = 48;

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
