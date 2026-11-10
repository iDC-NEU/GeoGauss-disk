#include "postgres.h"

#include "epoch/e_shmem.h"

#include "epoch/e_config.h"
#include "epoch/e_recv_ledger.h"
#include "epoch/e_statistician.h"

#include "occ/o_global.h"

#include "storage/ipc.h"
#include "storage/shmem.h"

#include <ctime>

static EpochSharedState* g_epochSharedState = nullptr;

Size EpochShmemSize(void)
{
    return sizeof(EpochSharedState);
}

uint64_t EpochGetSendEpochPeer(uint32_t peer)
{
    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr || peer >= EpochGetTotalNodes()) {
        return 1;
    }
    return state->send_epoch_peer[peer].load(std::memory_order_acquire);
}

/*
 * 所有 peer 的最小发送游标。
 * 语义：send_epoch > E 表示 E 这一片已经发给全部对端（任何一个 peer 落后都算没发完）。
 */
uint64_t EpochGetSendEpoch(void)
{
    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr) {
        return 1;
    }
    const uint32_t self = EpochGetNodeId();
    const uint32_t total = EpochGetTotalNodes();
    uint64_t min_ep = UINT64_MAX;
    for (uint32_t p = 0; p < total; ++p) {
        if (p == self) {
            continue;
        }
        const uint64_t v = state->send_epoch_peer[p].load(std::memory_order_acquire);
        if (v < min_ep) {
            min_ep = v;
        }
    }
    return (min_ep == UINT64_MAX) ? 1 : min_ep;
}

void EpochAdvanceSendEpoch(uint32_t peer, uint64_t epoch)
{
    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr || epoch == 0 || peer >= EpochGetTotalNodes()) {
        return;
    }
    /*
     * 推进条件只有一个：该时间片的写集与收尾包【已经成功进入 ZMQ 队列】。
     *
     * 调用点（drain）在 EpochNetPublish 返回成功之后才调这里，而 Publish 在
     * 队列满时会一直重试到成功——所以走到这里就代表“确实交给 ZMQ 了”。
     * 不再查任何账本计数（IsComplete 那套依赖“已声明 + 已发条数相等”，
     * 既慢又与“是否真的入队”不是同一件事）。
     *
     * 直接用 max 语义推进：发送是单线程按 epoch 顺序走的，正常就是 +1；
     * 用 max 只是防止极端乱序下把游标推回去。
     */
    uint64_t cur = state->send_epoch_peer[peer].load(std::memory_order_acquire);
    while (cur <= epoch &&
           !state->send_epoch_peer[peer].compare_exchange_weak(cur, epoch + 1,
               std::memory_order_acq_rel, std::memory_order_acquire)) {
    }
}

/*
 * 记“某个终端离开了某时间片”：它已经分到更新的时间片，说明它不会再往旧片里放事务。
 * 计数达到终端总数，说明整个节点在该时间片上的事务都已入队，此时塞收尾包。
 * 多个终端同时到达终点会重复塞收尾包，接收端只认第一个，不影响正确性。
 */
static void EpochInitCounterBank(EpochCounterBank* bank)
{
    for (uint32_t slot = 0; slot < EPOCH_DEFAULT_CACHE_SLOTS; ++slot) {
        bank->slot_epoch[slot].store(0, std::memory_order_relaxed);
        for (uint32_t node = 0; node < EPOCH_MAX_NODES; ++node) {
            bank->change_set_txn[slot][node].store(0, std::memory_order_relaxed);
            bank->executing_txn[slot][node].store(0, std::memory_order_relaxed);
            bank->local_pack_txn[slot][node].store(0, std::memory_order_relaxed);
            bank->sealed_pack_txn[slot][node].store(0, std::memory_order_relaxed);
            bank->abort_txn[slot][node].store(0, std::memory_order_relaxed);
            bank->write_abort_after_send_txn[slot][node].store(0, std::memory_order_relaxed);
            bank->wire_merge_done[slot][node].store(0, std::memory_order_relaxed);
            bank->merged_txn[slot][node].store(0, std::memory_order_relaxed);
            bank->committed_txn[slot][node].store(0, std::memory_order_relaxed);
            bank->node_send_completed[slot][node].store(false, std::memory_order_relaxed);
            bank->node_receive_completed[slot][node].store(false, std::memory_order_relaxed);
        }
    }
}

void EpochShmemInit(void)
{
    bool found = false;
    g_epochSharedState = static_cast<EpochSharedState*>(
        ShmemInitStruct("Epoch Shared State", EpochShmemSize(), &found));
    if (!found) {
        time_t now = time(nullptr);
        struct tm tm_buf;
        (void)localtime_r(&now, &tm_buf);
        (void)strftime(g_epochSharedState->log_file_stamp,
            sizeof(g_epochSharedState->log_file_stamp),
            "%Y%m%d_%H%M%S",
            &tm_buf);
        g_epochSharedState->next_commit_csn.store(INITIAL_CSN, std::memory_order_relaxed);
        g_epochSharedState->physical_epoch.store(1, std::memory_order_relaxed);
        /* 与 GeoGauss MOTAdaptor::logical_epoch 初值 1 对齐 */
        g_epochSharedState->logical_epoch.store(1, std::memory_order_relaxed);
        for (uint32_t p = 0; p < EPOCH_MAX_NODES; ++p) {
            g_epochSharedState->send_epoch_peer[p].store(1, std::memory_order_relaxed);
        }
        g_epochSharedState->physical_txn_since_advance.store(0, std::memory_order_relaxed);
        EpochInitCounterBank(&g_epochSharedState->counters);
        /* 时间片“离开计数”格子：逐个清空并初始化自旋锁 */
        for (uint32_t i = 0; i < 1024; ++i) {
            g_epochSharedState->depart_slots[i].epoch = 0;
            g_epochSharedState->depart_slots[i].count = 0;
            g_epochSharedState->depart_slots[i].eof_pushed = false;
            SpinLockInit(&g_epochSharedState->depart_slots[i].lock);
        }
        for (uint32_t i = 0; i < EPOCH_PERF_GATE_COUNT; ++i) {
            g_epochSharedState->perf.gate_wait_us[i].store(0, std::memory_order_relaxed);
            g_epochSharedState->perf.gate_wait_count[i].store(0, std::memory_order_relaxed);
            g_epochSharedState->perf.gate_spin_count[i].store(0, std::memory_order_relaxed);
        }
        for (uint32_t i = 0; i < EPOCH_FRONTIER_SLOTS; ++i) {
            g_epochSharedState->backend_read_frontier[i].store(0, std::memory_order_relaxed);
        }
    }
}

EpochSharedState* EpochGetSharedState(void)
{
    return g_epochSharedState;
}

uint64_t EpochGetNextCommitCsn(void)
{
    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr) {
        static std::atomic<uint64_t> fallback_csn{INITIAL_CSN};
        uint64_t current = INVALID_CSN;
        uint64_t next = INVALID_CSN;
        do {
            current = fallback_csn.load(std::memory_order_relaxed);
            next = current + 1;
        } while (!fallback_csn.compare_exchange_strong(current, next, std::memory_order_acq_rel));
        return current;
    }

    uint64_t current = INVALID_CSN;
    uint64_t next = INVALID_CSN;
    do {
        current = state->next_commit_csn.load(std::memory_order_relaxed);
        next = current + 1;
    } while (!state->next_commit_csn.compare_exchange_strong(current, next, std::memory_order_acq_rel));
    return current;
}
