#include "postgres.h"

#include "epoch/e_merge_pool.h"
#include "epoch/e_abort_csn.h"
#include "epoch/e_crdt_shmem.h"
#include "epoch/e_config.h"
#include "epoch/e_merge_shmem.h"
#include "epoch/e_merge_profile.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_statistician.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_types.h"

#include "utils/elog.h"

#include "portability/instr_time.h"

static void (*g_merge_task_done_hook)(const EpochMergeDoneSlot* done) = nullptr;

void EpochMergePoolSetTaskDoneHook(void (*hook)(const EpochMergeDoneSlot* done))
{
    g_merge_task_done_hook = hook;
}

void EpochMergePoolInvokeDoneHook(const EpochMergeDoneSlot* done)
{
    if (g_merge_task_done_hook != nullptr && done != nullptr) {
        g_merge_task_done_hook(done);
    }
}

static bool EpochMergePoolUsesParallelWorkers(void)
{
    return g_enable_epoch_multimaster && g_epoch_merge_threads >= 1 && EpochGetTotalNodes() > 1;
}

void EpochMergePoolPackDoneFromOutcome(EpochMergeDoneSlot* done, const E_WireTxn& txn, uint64_t wire_key,
    bool ok, const E_CrdtMergeOutcome& outcome)
{
    done->epoch = txn.epoch;
    done->wire_key = wire_key;
    done->csn = txn.csn;
    done->server_id = txn.server_id;
    done->ok = ok ? 1 : 0;
    done->txn_accepted = outcome.txn_accepted ? 1 : 0;
    done->any_new_merged = 0;
    done->pad = 0;
}

static bool EpochMergePoolExecuteInline(const E_WireTxn& txn, uint64_t wire_key)
{
    EpochMergeProfileBeginWireTxn();
    E_CrdtMergeOutcome outcome = EpochCrdtShmemMergeWireTxn(txn);
    bool ok = !outcome.resource_failure;
    instr_time post_start;
    INSTR_TIME_SET_CURRENT(post_start);
    if (ok) {
        EpochAbortCsnRecordMergeOutcome(txn.epoch, txn, outcome);
        if (outcome.txn_accepted) {
            (void)EpochPipelineTryEnqueueMergeWinnerOrig(txn, wire_key, nullptr, 0);
        }
    } else {
        EpochDiagLogMergeLoserAbortCsn(txn.epoch, txn.csn, txn.server_id, true, false);
    }
    instr_time post_end;
    INSTR_TIME_SET_CURRENT(post_end);
    INSTR_TIME_SUBTRACT(post_end, post_start);
    EpochMergeProfileAddUs(EMP_POST_HOOKS, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(post_end)));
    EpochMergeDoneSlot done;
    EpochMergePoolPackDoneFromOutcome(&done, txn, wire_key, ok, outcome);
    if (g_merge_task_done_hook != nullptr) {
        g_merge_task_done_hook(&done);
    }
    EpochMergeProfileEndWireTxn(txn.epoch, txn.csn, txn.server_id, ok && outcome.txn_accepted);
    return ok;
}

bool EpochMergePoolUsesInlineMerge(void)
{
    return !EpochMergePoolUsesParallelWorkers();
}

void EpochMergePoolInit(void)
{
    EpochAbortCsnShmemInit();
    EpochCrdtShmemInit();
    if (EpochMergePoolUsesParallelWorkers()) {
        EpochMergeQueueShmemInit();
        EpochMergeQueueResetCoordinatorState();
        ereport(LOG,
            (errmsg("epoch merge pool: async merge via %d postmaster backend workers (epoch_merge_threads=%d)",
                g_epoch_merge_threads,
                g_epoch_merge_threads)));
    }
}

void EpochMergePoolShutdown(void)
{
    if (EpochMergePoolUsesParallelWorkers()) {
        EpochMergeQueueRequestShutdown();
    }
}

bool EpochMergePoolPollCompletions(void)
{
    if (EpochMergePoolUsesInlineMerge()) {
        return false;
    }
    bool progress = false;
    EpochMergeDoneSlot done;
    while (EpochMergeQueuePopDone(&done)) {
        progress = true;
        if (g_merge_task_done_hook != nullptr) {
            g_merge_task_done_hook(&done);
        }
    }
    return progress;
}

bool EpochMergePoolHasPendingWork(void)
{
    if (EpochMergePoolUsesInlineMerge()) {
        return false;
    }
    return EpochMergeQueueInflight() > 0;
}

bool EpochMergePoolShouldThrottleDispatchForEpoch(uint64_t epoch)
{
    if (EpochMergePoolUsesInlineMerge()) {
        return false;
    }
    /* 二维格：每 worker 一列 depth，上限 ≈ workers * depth */
    uint32_t limit = EpochMergeQueueGetWorkerCount() * EpochMergeQueueGetDepth();
    if (limit < 1) {
        limit = 1;
    }
    return EpochMergeQueueInflightForEpoch(epoch) >= limit;
}

bool EpochMergePoolShouldThrottleDispatch(void)
{
    if (EpochMergePoolUsesInlineMerge()) {
        return false;
    }
    uint32_t limit = EpochMergeQueueGetWorkerCount() * EpochMergeQueueGetDepth();
    if (limit < 1) {
        limit = 1;
    }
    return EpochMergeQueueInflight() >= limit;
}

bool EpochMergePoolSubmit(const E_WireTxn& txn, uint64_t wire_key, bool is_local)
{
    if (EpochMergePoolUsesInlineMerge()) {
        return EpochMergePoolExecuteInline(txn, wire_key);
    }

    std::string payload;
    if (!E_MergeTaskSerialize(txn, &payload)) {
        return false;
    }

    uint32_t flags = is_local ? EPOCH_MERGE_FLAG_IS_LOCAL : 0;
    uint32_t n_workers = static_cast<uint32_t>(g_epoch_merge_threads < 1 ? 1 : g_epoch_merge_threads);
    if (n_workers > EPOCH_MAX_MERGE_WORKERS) {
        n_workers = EPOCH_MAX_MERGE_WORKERS;
    }
    uint32_t route_worker = E_WireTxnRouteWorker(txn.route_wid, wire_key, n_workers);
    return EpochMergeQueueSubmit(txn.epoch,
        wire_key,
        flags,
        payload.data(),
        static_cast<uint32_t>(payload.size()),
        route_worker,
        n_workers);
}

uint64_t EpochMergePoolGetMergeEpoch(void)
{
    if (EpochMergePoolUsesInlineMerge()) {
        return E_NodeManager::GetLogicalEpoch();
    }
    return EpochMergeQueueGetMergeEpoch();
}

void EpochMergePoolEnsureMergeEpoch(uint64_t floor_epoch)
{
    if (EpochMergePoolUsesInlineMerge() || floor_epoch == 0) {
        return;
    }
    uint64_t cur = EpochMergeQueueGetMergeEpoch();
    if (cur == 0 || cur < floor_epoch) {
        EpochMergeQueueSetMergeEpoch(floor_epoch);
    }
}

bool EpochMergePoolTryAdvanceMergeEpoch(uint64_t completed_epoch)
{
    if (EpochMergePoolUsesInlineMerge()) {
        return false;
    }
    return EpochMergeQueueTryAdvanceMergeEpoch(completed_epoch);
}
