#include "postgres.h"

#include "epoch/e_send_queue.h"

#include "neu_concurrency_tools/concurrentqueue.h"

#include <atomic>
#include <memory>

namespace {

std::unique_ptr<moodycamel::ConcurrentQueue<EpochSendItem>> g_send_q;
std::unique_ptr<moodycamel::ConcurrentQueue<EpochSendItem>> g_eof_q;
std::atomic<bool> g_send_q_ready{false};
std::atomic<uint64_t> g_send_outstanding{0};

} /* namespace */

void EpochSendQueueInit(void)
{
    if (g_send_q_ready.load(std::memory_order_acquire)) {
        return;
    }
    g_send_outstanding.store(0, std::memory_order_relaxed);
    g_send_q = std::make_unique<moodycamel::ConcurrentQueue<EpochSendItem>>();
    g_eof_q = std::make_unique<moodycamel::ConcurrentQueue<EpochSendItem>>();
    g_send_q_ready.store(true, std::memory_order_release);
}

void EpochSendQueueShutdown(void)
{
    g_send_q_ready.store(false, std::memory_order_release);
    g_send_outstanding.store(0, std::memory_order_relaxed);
    g_send_q.reset();
    g_eof_q.reset();
}

bool EpochSendEnqueue(uint64_t epoch, bool is_epoch_end, const std::string& wire)
{
    if (!g_send_q_ready.load(std::memory_order_acquire) || g_send_q == nullptr || g_eof_q == nullptr) {
        return false;
    }
    EpochSendItem item;
    item.epoch = epoch;
    item.is_epoch_end = is_epoch_end;
    item.wire = wire;
    moodycamel::ConcurrentQueue<EpochSendItem>* dest = is_epoch_end ? g_eof_q.get() : g_send_q.get();
    if (!dest->enqueue(std::move(item))) {
        return false;
    }
    if (!is_epoch_end && !wire.empty()) {
        g_send_outstanding.fetch_add(wire.size(), std::memory_order_relaxed);
    }
    return true;
}

uint64_t EpochSendOutstandingBytes(void)
{
    return g_send_outstanding.load(std::memory_order_relaxed);
}

void EpochSendNoteSent(size_t nbytes)
{
    uint64_t cur = g_send_outstanding.load(std::memory_order_relaxed);
    for (;;) {
        uint64_t next = (cur > nbytes) ? (cur - nbytes) : 0;
        if (g_send_outstanding.compare_exchange_weak(cur, next, std::memory_order_relaxed)) {
            break;
        }
    }
}

bool EpochSendTryDequeue(EpochSendItem* out)
{
    if (out == nullptr || !g_send_q_ready.load(std::memory_order_acquire) || g_send_q == nullptr) {
        return false;
    }
    return g_send_q->try_dequeue(*out);
}

bool EpochSendTryDequeueEpochEnd(EpochSendItem* out)
{
    if (out == nullptr || !g_send_q_ready.load(std::memory_order_acquire) || g_eof_q == nullptr) {
        return false;
    }
    return g_eof_q->try_dequeue(*out);
}

size_t EpochSendQueueApproxSize(void)
{
    if (!g_send_q_ready.load(std::memory_order_acquire) || g_send_q == nullptr) {
        return 0;
    }
    return g_send_q->size_approx();
}
