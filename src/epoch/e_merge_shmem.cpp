#include "postgres.h"

#include "epoch/e_merge_shmem.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_txn_wire.h"

#include "neu_concurrency_tools/concurrentqueue.h"

#include <atomic>

/*
 * 每个时间片一条并发队列：生产者是接收线程，消费者是 merge 线程。
 * 队列直接按 epoch % 格数 定位，不再经过共享内存任务表。
 */
static moodycamel::ConcurrentQueue<E_WireTxn> g_merge_queues[EPOCH_TXN_EPOCH_COUNT];

/* merge 游标：下一个要处理的时间片 */
static std::atomic<uint64_t> g_merge_epoch{1};
static std::atomic<int> g_merge_shutdown{0};

void EpochMergeQueueInit(void)
{
    /* 时间片编号从 1 开始 */
    g_merge_epoch.store(1, std::memory_order_release);
}

void EpochMergeQueueReset(void)
{
    g_merge_epoch.store(1, std::memory_order_release);
}

bool EpochMergeQueueEnqueueTxn(const E_WireTxn& txn)
{
    if (txn.epoch == 0) {
        return false;
    }
    return g_merge_queues[EpochSlotOf(txn.epoch)].enqueue(txn);
}

/* 诊断用：该 epoch 的 merge 队列近似长度 */
size_t EpochMergeQueueSize(uint64_t epoch)
{
    if (epoch == 0) {
        return 0;
    }
    return g_merge_queues[EpochSlotOf(epoch)].size_approx();
}

bool EpochMergeQueueTryDequeue(uint64_t epoch, E_WireTxn* out)
{
    if (epoch == 0 || out == nullptr) {
        return false;
    }
    return g_merge_queues[EpochSlotOf(epoch)].try_dequeue(*out);
}

uint64_t GetMergeEpoch(void)
{
    return g_merge_epoch.load(std::memory_order_acquire);
}

void EpochMergeQueueDescribeEpoch(uint64_t epoch, char* out, size_t outlen)
{
    if (out == nullptr || outlen == 0) {
        return;
    }
    snprintf(out, outlen, "epoch=%lu merge_epoch=%lu", static_cast<unsigned long>(epoch),
        static_cast<unsigned long>(g_merge_epoch.load(std::memory_order_acquire)));
}

void EpochMergeQueueSetMergeEpoch(uint64_t epoch)
{
    g_merge_epoch.store(epoch, std::memory_order_release);
}

void EpochMergeQueueRequestShutdown(void)
{
    g_merge_shutdown.store(1, std::memory_order_release);
}

bool EpochMergeQueueShutdownRequested(void)
{
    return g_merge_shutdown.load(std::memory_order_acquire) != 0;
}

bool EpochMergeQueueTryAdvanceMergeEpoch(uint64_t completed_epoch)
{
    if (completed_epoch == 0) {
        return false;
    }
    /*
     * CAS 而不是 fetch_add：
     * 多个 merge 线程可能同时看到 merged == declared（最后一笔刚被其中一个写完），
     * fetch_add 会让游标一次跳多格，把还没处理的时间片整片跳过。
     */
    uint64_t expected = completed_epoch;
    return g_merge_epoch.compare_exchange_strong(expected, completed_epoch + 1,
        std::memory_order_acq_rel, std::memory_order_acquire);
}
