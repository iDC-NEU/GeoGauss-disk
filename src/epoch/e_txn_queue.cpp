#include "postgres.h"

#include <atomic>

#include "epoch/e_txn_queue.h"
#include "epoch/e_perf.h"
#include "epoch/e_recv_ledger.h"
#include "epoch/e_config.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_txn_wire.h"

#include "storage/ipc.h"
#include "storage/shmem.h"

/*
 * ============================ 发送队列（本地待发缓冲） ============================
 *
 * 【存什么】一笔笔 wire txn —— 事务在 commit 时序列化后的字节，等待发给 peer。
 *          不是"行"，是"事务"。
 *
 * 【怎么组织】[epoch][队列]，每个 epoch 一个 FIFO 队列：
 *             epoch_queues[EpochSlotOf(epoch)]
 *           之所以按 epoch 分开：慢节点（上行带宽受限）时后面的 epoch 会先产出、
 *           前面的还没发完，需要同时容纳多个 epoch 的待发数据。
 *
 * 【为什么在共享内存】backend 是独立进程（每个 TPCC terminal 一个），
 *           生产者在 backend 子进程，消费者是 postmaster 里的 sender 线程 → 跨进程。
 *
 * 【为什么每项定长】共享内存里无法用 std::vector/string 动态分配，
 *           定长才能按 index 算术寻址。12KB 覆盖 TPCC 最大写集（Delivery ≈ 8.1KB）。
 *
 * 【锁】每个 epoch 队列自带一把自旋锁：
 *         - 同一 epoch 内，多个 backend（不同 terminal）可能同时 commit → 串行
 *         - 不同 epoch 的队列互不影响 → 完全并行
 *       临界区只有一次 memcpy + 3 个整数更新，所以用自旋锁（不用 LWLock）。
 * ==============================================================================
 */

/* 一条 wire txn：一笔事务序列化后的字节 */
struct EpochTxnWire {
    uint32_t len;
    uint32_t pad;
    char data[EPOCH_MAX_WIRE_TXN_BYTES];
};

/* 一个 epoch 的待发队列 */
struct EpochTxnEpochQueue {
    slock_t lock;                                  /* 本队列的锁（同 epoch 串行，跨 epoch 并行） */
    uint64_t epoch;                                /* 当前绑定哪个 epoch（换了就重置） */
    uint32_t head;                                 /* 出队位置（sender 取） */
    uint32_t tail;                                 /* 入队位置（backend 写） */
    uint32_t count;                                /* 当前待发条数 */
    EpochTxnWire wires[EPOCH_TXN_PER_EPOCH];       /* 队列内容：一笔笔 wire txn */
};

/* 全部：每个 peer 各有 EPOCH_TXN_EPOCH_COUNT 个 epoch 队列 */
struct EpochTxnQueue {
    /* 所有 peer 合计的待发总数（push++/pop--） */
    std::atomic<uint32_t> total_count;
    /* 每个 peer 各自的待发总数，供状态日志与背压判断 */
    std::atomic<uint32_t> peer_count[EPOCH_MAX_NODES];
    /* 变长数组：peer_slots × EPOCH_TXN_EPOCH_COUNT 个队列 */
    EpochTxnEpochQueue epoch_queues[1];
};

static EpochTxnQueue* g_epochTxnQueue = nullptr;
static uint32_t g_epochTxnDrainRr = 0;

/* 该 peer 的队列段在变长数组里的起点 */
static inline size_t EpochTxnQueuePeerBase(uint32_t peer)
{
    return static_cast<size_t>(peer) * EPOCH_TXN_EPOCH_COUNT;
}

static inline EpochTxnEpochQueue* EpochTxnQueueAt(uint32_t peer, uint64_t epoch)
{
    return &g_epochTxnQueue->epoch_queues[EpochTxnQueuePeerBase(peer) +
        (EpochSlotOf(epoch))];
}

/* 变长部分的实际字节数（含可变数组） */
static inline Size EpochTxnQueueBytes(uint32_t peer_slots)
{
    if (peer_slots == 0) {
        peer_slots = 1;
    }
    return offsetof(EpochTxnQueue, epoch_queues) +
        static_cast<Size>(peer_slots) * EPOCH_TXN_EPOCH_COUNT * sizeof(EpochTxnEpochQueue);
}

Size EpochTxnQueueShmemSize(uint32_t capacity)
{
    /* capacity 参数保留兼容；容量由 EPOCH_TXN_EPOCH_COUNT × EPOCH_TXN_PER_EPOCH 决定 */
    (void)capacity;
    fprintf(stderr, "EPOCH_TXNQ size=%zu MB=%zu (epochs=%u per=%u wire=%u peers=%u)\n",
        EpochTxnQueueBytes(EpochGetTotalNodes()), EpochTxnQueueBytes(EpochGetTotalNodes()) >> 20,
        (unsigned)EPOCH_TXN_EPOCH_COUNT, (unsigned)EPOCH_TXN_PER_EPOCH,
        (unsigned)EPOCH_MAX_WIRE_TXN_BYTES, (unsigned)EpochGetTotalNodes());
    fflush(stderr);
    return EpochTxnQueueBytes(EpochGetTotalNodes());
}

void EpochTxnQueueShmemInit(uint32_t capacity)
{
    (void)capacity;
    bool found = false;
    g_epochTxnQueue = static_cast<EpochTxnQueue*>(
        ShmemInitStruct("Epoch Txn Queue", EpochTxnQueueBytes(EpochGetTotalNodes()), &found));
    if (g_epochTxnQueue == nullptr || found) {
        return;
    }
    g_epochTxnQueue->total_count.store(0, std::memory_order_relaxed);
    for (uint32_t p = 0; p < EPOCH_MAX_NODES; ++p) {
        g_epochTxnQueue->peer_count[p].store(0, std::memory_order_relaxed);
    }
    const uint32_t peers = EpochGetTotalNodes();
    for (uint32_t p = 0; p < peers; ++p) {
        for (uint32_t i = 0; i < EPOCH_TXN_EPOCH_COUNT; ++i) {
            EpochTxnEpochQueue& q = g_epochTxnQueue->epoch_queues[EpochTxnQueuePeerBase(p) + i];
            SpinLockInit(&q.lock);
            q.epoch = 0;
            q.head = 0;
            q.tail = 0;
            q.count = 0;
        }
    }
}

bool EpochTxnQueuePush(uint32_t peer, uint64_t epoch, const char* data, uint32_t len)
{
    if (g_epochTxnQueue == nullptr || data == nullptr || len == 0 ||
        len > EPOCH_MAX_WIRE_TXN_BYTES || epoch == 0 || peer >= EpochGetTotalNodes()) {
        return false;
    }
    EpochTxnEpochQueue* q = EpochTxnQueueAt(peer, epoch);
    SpinLockAcquire(&q->lock);
    if (q->epoch != epoch) {
        /* 该 epoch 队列被复用：清空重建（上一轮数据必须已消费完，由 physical-logical 深度保证） */
        q->epoch = epoch;
        q->head = 0;
        q->tail = 0;
        q->count = 0;
    }
    if (q->count >= EPOCH_TXN_PER_EPOCH) {
        SpinLockRelease(&q->lock);
        /*
         * 单格被撑满：名额机制下每片固定 24 条写集 + 1 个收尾包 = 25，
         * 正常绝不该走到这里。真到了说明 physical 没有按预期推进
         * （例如某片一直没凑满、或者名额计数器被复用）。
         * 记一条日志便于定位，然后让这一笔事务失败（客户端会重试）。
         */
        EPOCH_DBG_FPRINTF("EPOCH_DBG queue_full peer=%u send_epoch=%lu 超过容量 count=%u cap=%u\n",
            peer, static_cast<unsigned long>(epoch),
            static_cast<unsigned>(q->count),
            static_cast<unsigned>(EPOCH_TXN_PER_EPOCH));
        fflush(stderr);
        return false;
    }
    EpochTxnWire& w = q->wires[q->tail];
    w.len = len;
    errno_t rc = memcpy_s(w.data, sizeof(w.data), data, len);
    securec_check(rc, "\0", "\0");
    q->tail = (q->tail + 1) % EPOCH_TXN_PER_EPOCH;
    q->count++;
    SpinLockRelease(&q->lock);
    g_epochTxnQueue->total_count.fetch_add(1, std::memory_order_relaxed);
    g_epochTxnQueue->peer_count[peer].fetch_add(1, std::memory_order_relaxed);
    return true;
}

bool EpochTxnQueuePop(uint32_t peer, uint64_t epoch, char* data, uint32_t max_len, uint32_t* out_len,
    uint32_t* out_index)
{
    if (g_epochTxnQueue == nullptr || data == nullptr || out_len == nullptr || epoch == 0 ||
        peer >= EpochGetTotalNodes()) {
        return false;
    }
    if (out_index != nullptr) {
        *out_index = 0;
    }
    EpochTxnEpochQueue* q = EpochTxnQueueAt(peer, epoch);
    SpinLockAcquire(&q->lock);
    if (q->epoch != epoch || q->count == 0) {
        SpinLockRelease(&q->lock);
        {
            static uint64_t dbg_pp = 0;
            if ((++dbg_pp % 500) == 0) {
                fprintf(stderr, "EPOCH_DBG q_pop_miss peer=%u epoch=%lu bound=%lu count=%u\n",
                    peer, (unsigned long)epoch, (unsigned long)q->epoch, q->count);
                fflush(stderr);
            }
        }
        return false;
    }
    if (out_index != nullptr) {
        /* 该条在本片内的序号：各 peer 队列顺序一致，因此同一笔的下标相同 */
        *out_index = q->head;
    }
    EpochTxnWire& w = q->wires[q->head];
    if (w.len > max_len) {
        SpinLockRelease(&q->lock);
        return false;
    }
    errno_t rc = memcpy_s(data, max_len, w.data, w.len);
    securec_check(rc, "\0", "\0");
    *out_len = w.len;
    q->head = (q->head + 1) % EPOCH_TXN_PER_EPOCH;
    q->count--;
    SpinLockRelease(&q->lock);
    g_epochTxnQueue->total_count.fetch_sub(1, std::memory_order_relaxed);
    g_epochTxnQueue->peer_count[peer].fetch_sub(1, std::memory_order_relaxed);
    return true;
}


uint32_t EpochTxnQueueCount(void)
{
    if (g_epochTxnQueue == nullptr) {
        return 0;
    }
    return g_epochTxnQueue->total_count.load(std::memory_order_relaxed);
}

uint32_t EpochTxnQueueCountPeer(uint32_t peer)
{
    if (g_epochTxnQueue == nullptr || peer >= EPOCH_MAX_NODES) {
        return 0;
    }
    return g_epochTxnQueue->peer_count[peer].load(std::memory_order_relaxed);
}
