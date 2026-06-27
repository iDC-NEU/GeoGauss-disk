#include "postgres.h"

#include "epoch/e_shmem.h"

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
            bank->local_produce_txn[slot][node].store(0, std::memory_order_relaxed);
            bank->local_pack_txn[slot][node].store(0, std::memory_order_relaxed);
            bank->sealed_pack_txn[slot][node].store(0, std::memory_order_relaxed);
            bank->local_record_commit_txn[slot][node].store(0, std::memory_order_relaxed);
            bank->local_record_committed_txn[slot][node].store(0, std::memory_order_relaxed);
            bank->abort_txn[slot][node].store(0, std::memory_order_relaxed);
            bank->read_committed_txn[slot][node].store(0, std::memory_order_relaxed);
            bank->write_abort_after_send_txn[slot][node].store(0, std::memory_order_relaxed);
            bank->store_should_receive_pack[slot][node].store(0, std::memory_order_relaxed);
            bank->store_received_pack[slot][node].store(0, std::memory_order_relaxed);
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
        g_epochSharedState->physical_epoch.store(1, std::memory_order_relaxed);
        /* 与 GeoGauss MOTAdaptor::logical_epoch 初值 1 对齐 */
        g_epochSharedState->logical_epoch.store(1, std::memory_order_relaxed);
        g_epochSharedState->record_committed.store(true, std::memory_order_relaxed);
        g_epochSharedState->remote_record_committed.store(true, std::memory_order_relaxed);
        g_epochSharedState->remote_execed.store(false, std::memory_order_relaxed);
        g_epochSharedState->epoch_advance_started.store(false, std::memory_order_relaxed);
        g_epochSharedState->async_cap_pack_epoch.store(0, std::memory_order_relaxed);
        g_epochSharedState->async_cap_pack_reserved.store(0, std::memory_order_relaxed);
        g_epochSharedState->async_cap_pack_blocked_ms.store(0, std::memory_order_relaxed);
        g_epochSharedState->async_cap_pack_drip_tokens.store(0, std::memory_order_relaxed);
        g_epochSharedState->async_cap_pack_drip_refill_ms.store(0, std::memory_order_relaxed);
        g_epochSharedState->async_commit_rate_tokens.store(0, std::memory_order_relaxed);
        g_epochSharedState->async_commit_rate_last_ms.store(0, std::memory_order_relaxed);
        EpochInitCounterBank(&g_epochSharedState->counters);
        EpochAbortCsnBankInit(&g_epochSharedState->abort_csns);
        for (uint32_t i = 0; i < EPOCH_PERF_GATE_COUNT; ++i) {
            g_epochSharedState->perf.gate_wait_us[i].store(0, std::memory_order_relaxed);
            g_epochSharedState->perf.gate_wait_count[i].store(0, std::memory_order_relaxed);
            g_epochSharedState->perf.gate_spin_count[i].store(0, std::memory_order_relaxed);
        }
    }
}

EpochSharedState* EpochGetSharedState(void)
{
    return g_epochSharedState;
}
