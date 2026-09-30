#include "postgres.h"

#include "epoch/e_shmem.h"

#include "occ/o_global.h"

#include "storage/ipc.h"
#include "storage/shmem.h"

#include <ctime>

static EpochSharedState* g_epochSharedState = nullptr;

Size EpochShmemSize(void)
{
    return sizeof(EpochSharedState);
}

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
            bank->store_should_receive_pack[slot][node].store(0, std::memory_order_relaxed);
            bank->store_received_pack[slot][node].store(0, std::memory_order_relaxed);
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
        g_epochSharedState->physical_txn_since_advance.store(0, std::memory_order_relaxed);
        EpochInitCounterBank(&g_epochSharedState->counters);
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
