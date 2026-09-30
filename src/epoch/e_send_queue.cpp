#include "postgres.h"

#include "epoch/e_send_queue.h"

#include "neu_concurrency_tools/concurrentqueue.h"

#include <atomic>
#include <memory>

namespace {

std::unique_ptr<moodycamel::ConcurrentQueue<EpochSendItem>> g_send_q;
std::atomic<bool> g_send_q_ready{false};

} /* namespace */

void EpochSendQueueInit(void)
{
    if (g_send_q_ready.load(std::memory_order_acquire)) {
        return;
    }
    g_send_q = std::make_unique<moodycamel::ConcurrentQueue<EpochSendItem>>();
    g_send_q_ready.store(true, std::memory_order_release);
}

void EpochSendQueueShutdown(void)
{
    g_send_q_ready.store(false, std::memory_order_release);
    g_send_q.reset();
}

bool EpochSendEnqueue(uint64_t epoch, bool is_epoch_end, const std::string& wire)
{
    if (!g_send_q_ready.load(std::memory_order_acquire) || g_send_q == nullptr) {
        return false;
    }
    EpochSendItem item;
    item.epoch = epoch;
    item.is_epoch_end = is_epoch_end;
    item.wire = wire;
    return g_send_q->enqueue(std::move(item));
}

bool EpochSendTryDequeue(EpochSendItem* out)
{
    if (out == nullptr || !g_send_q_ready.load(std::memory_order_acquire) || g_send_q == nullptr) {
        return false;
    }
    return g_send_q->try_dequeue(*out);
}

size_t EpochSendQueueApproxSize(void)
{
    if (!g_send_q_ready.load(std::memory_order_acquire) || g_send_q == nullptr) {
        return 0;
    }
    return g_send_q->size_approx();
}
