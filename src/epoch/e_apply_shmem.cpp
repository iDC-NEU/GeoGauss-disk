#include "postgres.h"

#include "epoch/e_apply_shmem.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_txn_wire.h"

#include "epoch/e_concurrent_map.h"
#include "neu_concurrency_tools/concurrentqueue.h"

#include <atomic>

/*
 * 每个时间片一条并发队列：生产者是接收/merge 线程，消费者是 apply 线程。
 * 队列按 epoch % 格数 定位。
 */
static moodycamel::ConcurrentQueue<E_WireTxn> g_apply_queues[EPOCH_TXN_EPOCH_COUNT];

/* apply 游标：下一个要写回的时间片 */
static std::atomic<uint64_t> g_apply_epoch{1};
static std::atomic<int> g_apply_shutdown{0};

/*
 * 每个时间片已计过 commit 的 csn 集合。
 * 一笔事务可能有多行、分多条入队，写回时按行处理，
 * 但 commit 统计只能按事务计一次 —— 用这个集合去重。
 */
static EpochConcurrent::U64SetMap g_apply_csn_seen;

bool EpochApplyCsnSeenFirst(uint64_t epoch, uint64_t csn)
{
    if (epoch == 0 || csn == 0) {
        return true;
    }
    return EpochConcurrent::U64SetInsert(g_apply_csn_seen, epoch, csn);
}

void EpochApplyQueueInit(void)
{
    /* 时间片编号从 1 开始 */
    g_apply_epoch.store(1, std::memory_order_release);
}

void EpochApplyQueueReset(void)
{
    g_apply_epoch.store(1, std::memory_order_release);
}

bool EpochApplyQueueEnqueueTxn(uint64_t epoch, const E_WireTxn& txn)
{
    if (epoch == 0) {
        return false;
    }
    return g_apply_queues[EpochSlotOf(epoch)].enqueue(txn);
}

bool EpochApplyQueueTryDequeue(uint64_t epoch, E_WireTxn* out)
{
    if (epoch == 0 || out == nullptr) {
        return false;
    }
    return g_apply_queues[EpochSlotOf(epoch)].try_dequeue(*out);
}

uint64_t EpochApplyQueueGetApplyEpoch(void)
{
    return g_apply_epoch.load(std::memory_order_acquire);
}

void EpochApplyQueueSetApplyEpoch(uint64_t epoch)
{
    g_apply_epoch.store(epoch, std::memory_order_release);
}

bool EpochApplyQueueTryAdvanceApplyEpoch(uint64_t completed_epoch)
{
    if (completed_epoch == 0) {
        return false;
    }
    /*
     * CAS 而不是 fetch_add：多个 apply 线程同时发现队列空了，
     * fetch_add 会让游标一次跳多格，把还没写回的时间片整片跳过。
     */
    uint64_t expected = completed_epoch;
    return g_apply_epoch.compare_exchange_strong(expected, completed_epoch + 1,
        std::memory_order_acq_rel, std::memory_order_acquire);
}

void EpochApplyQueueRequestShutdown(void)
{
    g_apply_shutdown.store(1, std::memory_order_release);
}

bool EpochApplyQueueShutdownRequested(void)
{
    return g_apply_shutdown.load(std::memory_order_acquire) != 0;
}
