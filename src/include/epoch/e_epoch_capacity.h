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

/*
 * 发送队列（EpochTxnQueue）的两个维度。
 * 每个 epoch 队列自带一把自旋锁：同一 epoch 的并发 push 串行，不同 epoch 完全并行。
 *
 * EPOCH_TXN_EPOCH_COUNT = 8192：能同时堆积 8192 个 epoch 的待发数据。
 *   慢节点 5Mbps 发一个 epoch(约100KB) 需 160ms → 8192 个 ≈ 22 分钟堆积余量，
 *   实际只堆积几十~几百个 → 不会因容量触发背压。
 * EPOCH_TXN_PER_EPOCH = 64：每 epoch 目标 25 条，但推进检查每 10ms 才看一次，
 *   64 个 terminal 在这个窗口内可能入队 30+ 条（实测出现过 33 条），
 *   若容量只有 32，收尾包会塞不进队列。留一倍余量到 64。
 */
/*
 * 环形格数 512 → 2048：
 * 慢节点的 physical 领先上限放宽到 1000 片（它自己的 apply 跟不上没关系，
 * 写集对快节点仍有用），因此环必须显著大于 1000，否则 epoch E 与 E+格数
 * 会落进同一格，把还没发出去的写集覆盖掉。
 * 代价：发送队列共享内存 384MB → 1.5GB。
 */
/*
 * 发送队列改为"每 peer 一份"后，共享内存 = N × 25 × 12KB × total_nodes。
 * 10000 时三节点要 9GB，降到 4096 后为 3.8GB（只比原来单份 3.0GB 多 0.8GB）。
 */
/*
 * 环形格数 4096 → 1024：
 * physical 现在被快照闸门 + 进度下界卡住（phys - floor < N-100），
 * 不再需要 4000 格的堆积余量；把省下的内存让给"每格容量"。
 */
/*
 * 环形格数：1024 → 256（压测用，验证回绕鲁棒性）。
 * 约束（都靠闸门保证）：
 *   physical 领先 min(logical, 最慢 send) 不得超过 N-100 = 156
 *   physical 领先 apply 不得超过 EPOCH_SNAPSHOT_LAG_MAX = 100
 * 因此任意一侧的落后量都 < N，不会出现"新片覆盖未消费的旧片"。
 */
static constexpr uint32_t EPOCH_TXN_EPOCH_COUNT = 256;
/*
 * 每片名额固定为 24：backend 抢号，< 24 入写集、== 24 由它封口并发收尾包。
 *
 * 队列容量取 100（不是 25）：
 * DBR 延后会把写集压回更晚的片，目标格要能容纳"本片 24 条 + 若干延后条目"。
 * 之前取 25 时，延后立刻把目标格压满（实测 queue_full 38 次），只能退回直发。
 *
 * 内存核算：1024 格 × 3 peer × 100 槽 × 12KB ≈ 3.6GB（与原来 4096×3×25 相同）。
 */
static constexpr uint32_t EPOCH_TXN_SLOTS_PER_EPOCH = 24;
static constexpr uint32_t EPOCH_TXN_PER_EPOCH = 100;

/*
 * epoch → 槽位映射：**0 号槽永久空置**，只用 [1, N-1]。
 *
 * 为什么不能用 epoch % N：
 *   0 在这套结构里天然是"未初始化/空"的哨兵语义（q->epoch=0 表示未绑定、
 *   count=0 表示空）。epoch % N 落到 0 号槽时（如 epoch=1024、N=1024），
 *   会撞上这些哨兵假设 —— 实测 recv 永久卡在 epoch 1024、连自环 EOF 都收不到。
 *
 * 改成 (epoch-1) % (N-1) + 1 后：epoch 1→槽1、epoch N-1→槽N-1、epoch N→槽1，
 * 槽位永远落在 [1, N-1]，0 号槽不参与，容量只少 1 格。
 */
static inline uint32_t EpochSlotOf(uint64_t epoch)
{
    if (epoch == 0) {
        return 1;
    }
    return 1 + static_cast<uint32_t>((epoch - 1) % (EPOCH_TXN_EPOCH_COUNT - 1));
}

