#include "postgres.h"

#include "epoch/e_read_frontier.h"
#include "epoch/e_config.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_shmem.h"
#include "epoch/e_types.h"

#include "knl/knl_session.h"
#include "knl/knl_thread.h"
#include "storage/proc.h"

static __thread uint64_t t_epoch_heap_write_epoch = 0;

static void EpochPublishReadFrontier(uint64_t frontier)
{
    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr || t_thrd.proc == nullptr) {
        return;
    }
    int slot = t_thrd.proc->pgprocno;
    if (slot < 0 || static_cast<uint32_t>(slot) >= EPOCH_FRONTIER_SLOTS) {
        return;
    }
    state->backend_read_frontier[slot].store(frontier, std::memory_order_release);
}

static void EpochClearPublishedReadFrontier(void)
{
    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr || t_thrd.proc == nullptr) {
        return;
    }
    int slot = t_thrd.proc->pgprocno;
    if (slot < 0 || static_cast<uint32_t>(slot) >= EPOCH_FRONTIER_SLOTS) {
        return;
    }
    state->backend_read_frontier[slot].store(0, std::memory_order_release);
}

void EpochPinReadFrontierOnStart(void)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        u_sess->epoch_cxt.read_frontier_pinned = false;
        u_sess->epoch_cxt.read_frontier = 0;
        EpochClearPublishedReadFrontier();
        return;
    }
    u_sess->epoch_cxt.read_frontier = E_NodeManager::GetLogicalEpoch();
    u_sess->epoch_cxt.read_frontier_pinned = true;
    EpochPublishReadFrontier(u_sess->epoch_cxt.read_frontier);
}

void EpochResetReadFrontier(void)
{
    u_sess->epoch_cxt.read_frontier = 0;
    u_sess->epoch_cxt.read_frontier_pinned = false;
    EpochClearPublishedReadFrontier();
}

bool EpochReadFrontierActive(void)
{
    return g_enable_epoch_multimaster && EpochGetTotalNodes() > 1 &&
           u_sess->epoch_cxt.read_frontier_pinned;
}

uint64_t EpochGetReadFrontier(void)
{
    return u_sess->epoch_cxt.read_frontier;
}

uint64_t EpochGetPruneSafeFrontier(void)
{
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t lag_cap = (logical > EPOCH_PRUNE_KEEP_EPOCHS) ? (logical - EPOCH_PRUNE_KEEP_EPOCHS) : 0;

    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr) {
        return lag_cap;
    }

    uint64_t min_pin = UINT64_MAX;
    for (uint32_t i = 0; i < EPOCH_FRONTIER_SLOTS; ++i) {
        uint64_t f = state->backend_read_frontier[i].load(std::memory_order_acquire);
        if (f == 0) {
            continue;
        }
        /* 过期 pin 不参与（卡住事务会否则拖死全局 prune） */
        if (logical > f && (logical - f) > EPOCH_PRUNE_STALE_PIN_EPOCHS) {
            continue;
        }
        if (f < min_pin) {
            min_pin = f;
        }
    }

    /* 无有效 pin：logical-KEEP；有 pin：最老 reader（不超过 lag_cap 的保守上界不需要——越高越能回收） */
    if (min_pin == UINT64_MAX) {
        return lag_cap;
    }
    return min_pin;
}

void EpochHeapSetWriteEpoch(uint64_t epoch)
{
    t_epoch_heap_write_epoch = epoch;
}

uint64_t EpochHeapCommitEpochForWrite(void)
{
    if (t_epoch_heap_write_epoch != 0) {
        return t_epoch_heap_write_epoch;
    }
    if (u_sess->epoch_cxt.epoch_assigned) {
        return u_sess->epoch_cxt.txn_epoch;
    }
    return 0;
}
