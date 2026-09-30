#include "postgres.h"

#include "epoch/e_apply_pool.h"
#include "epoch/e_abort_csn.h"
#include "epoch/e_apply.h"
#include "epoch/e_apply_shmem.h"
#include "epoch/e_apply_stats.h"
#include "epoch/e_config.h"
#include "epoch/e_crdt_shmem.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_statistician.h"
#include "epoch/e_tip_staging_shmem.h"
#include "epoch/e_pk_staging_shmem.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_types.h"

#include "utils/elog.h"

static void (*g_apply_task_done_hook)(uint64_t epoch, uint64_t wire_key, bool ok) = nullptr;

void EpochApplyPoolSetTaskDoneHook(void (*hook)(uint64_t epoch, uint64_t wire_key, bool ok))
{
    g_apply_task_done_hook = hook;
    EpochApplyQueueSetFinishHook(hook);
}

static bool EpochApplyPoolUsesParallelWorkers(void)
{
    return g_enable_epoch_multimaster && g_epoch_commit_threads >= 1 && EpochGetTotalNodes() > 1;
}

static bool EpochApplyPoolExecuteInline(const EpochApplyPoolTask& task)
{
    if (g_epoch_skip_apply) {
        EpochApplyStatsNoteCommit();
        E_NodeManager::IncCommittedTxnCounters(task.epoch, task.local_node);
        if (g_apply_task_done_hook != nullptr) {
            g_apply_task_done_hook(task.epoch, task.wire_key, true);
        }
        return true;
    }

    /* inline 路径无 claim 门闩：须已 merge_complete 再 Build/落盘 */
    if (!EpochPipelineEpochMergeComplete(task.epoch)) {
        return false;
    }

    if (EpochAbortCsnContains(task.epoch, task.apply_txn.csn, task.apply_txn.server_id)) {
        EpochApplyStatsNoteAbort(OCC_ABORT_REMOTE_MERGE);
        if (g_apply_task_done_hook != nullptr) {
            g_apply_task_done_hook(task.epoch, task.wire_key, true);
        }
        return true;
    }

    /* 赢家原始 wire；不再 BuildApplyTxn 从 CRDT 重建写集 */
    const E_WireTxn* to_apply = &task.apply_txn;
    (void)task.need_crdt_build;

    uint64_t row_count = to_apply->rows.size();
    const char* apply_tag = task.is_local ? "APPLY_LOCAL" : "APPLY_REMOTE";
    EpochRecvDiagLogWireRelSummary(apply_tag,
        task.epoch,
        to_apply->csn,
        to_apply->server_id,
        to_apply->rows);

    bool apply_ok = E_ApplyWireTxn(*to_apply, task.is_local);
    EpochRecvDiagOnApply(task.epoch, row_count, apply_ok);
    if (!apply_ok) {
        if (g_apply_task_done_hook != nullptr) {
            g_apply_task_done_hook(task.epoch, task.wire_key, false);
        }
        return false;
    }
    EpochApplyStatsNoteCommit();
    E_NodeManager::IncCommittedTxnCounters(task.epoch, task.local_node);
    if (g_apply_task_done_hook != nullptr) {
        g_apply_task_done_hook(task.epoch, task.wire_key, true);
    }
    return true;
}

static uint32_t EpochApplyPoolBuildFlags(const EpochApplyPoolTask& task)
{
    uint32_t flags = 0;
    if (task.is_local) {
        flags |= EPOCH_APPLY_FLAG_IS_LOCAL;
    }
    if (task.need_crdt_build) {
        flags |= EPOCH_APPLY_FLAG_NEED_CRDT_BUILD;
    }
    return flags;
}

bool EpochApplyPoolUsesInlineApply(void)
{
    return !EpochApplyPoolUsesParallelWorkers();
}

void EpochApplyPoolInit(void)
{
    if (EpochApplyPoolUsesParallelWorkers()) {
        EpochApplyQueueShmemInit();
        EpochTipStagingShmemInit();
        EpochPkStagingShmemInit();
        EpochApplyQueueResetCoordinatorState();
        ereport(LOG,
            (errmsg("epoch apply pool: async apply via %d postmaster backend workers (epoch_commit_threads=%d)",
                g_epoch_commit_threads,
                g_epoch_commit_threads)));
    }
}

void EpochApplyPoolShutdown(void)
{
    if (EpochApplyPoolUsesParallelWorkers()) {
        EpochApplyQueueRequestShutdown();
    }
}

bool EpochApplyPoolPollCompletions(void)
{
    if (EpochApplyPoolUsesInlineApply()) {
        return false;
    }
    bool progress = false;
    EpochApplyDoneSlot done;
    while (EpochApplyQueuePopDone(&done)) {
        progress = true;
        if (g_apply_task_done_hook != nullptr) {
            g_apply_task_done_hook(done.epoch, done.wire_key, done.ok != 0);
        }
    }
    return progress;
}

bool EpochApplyPoolHasPendingWork(void)
{
    if (EpochApplyPoolUsesInlineApply()) {
        return false;
    }
    return EpochApplyQueueInflight() > 0;
}

bool EpochApplyPoolHasPendingWorkForEpoch(uint64_t epoch)
{
    if (EpochApplyPoolUsesInlineApply()) {
        return false;
    }
    return EpochApplyQueueInflightForEpoch(epoch) > 0;
}

bool EpochApplyPoolShouldThrottleDispatchForEpoch(uint64_t epoch)
{
    if (EpochApplyPoolUsesInlineApply()) {
        return false;
    }
    /*
     * 当前 apply_epoch：上限提到 tasks_per_slot，一次尽量喂满环，避免几十个赢家
     * 被 2*commit_threads（≈10）切成多波 → merge→apply 墙钟被波数拉长。
     * 未来 prefill epoch：配额压到 commit_threads，减轻抢 coordinator / queue_wait 虚高。
     */
    int n = g_epoch_commit_threads;
    if (n < 1) {
        n = 1;
    }
    const uint64_t apply_ep = EpochApplyQueueGetApplyEpoch();
    uint32_t limit;
    if (apply_ep == 0 || epoch == apply_ep) {
        uint32_t tps = EpochApplyQueueGetTasksPerSlot();
        if (tps < 1) {
            tps = 1;
        }
        const uint32_t floor = static_cast<uint32_t>(n) * 2U;
        limit = (tps > floor) ? tps : floor;
    } else {
        limit = static_cast<uint32_t>(n);
    }
    return EpochApplyQueueInflightForEpoch(epoch) >= limit;
}

bool EpochApplyPoolShouldThrottleDispatch(void)
{
    uint64_t ep = EpochApplyQueueGetApplyEpoch();
    if (ep == 0) {
        ep = E_NodeManager::GetLogicalEpoch();
    }
    return EpochApplyPoolShouldThrottleDispatchForEpoch(ep);
}

uint64_t EpochApplyPoolGetApplyEpoch(void)
{
    if (EpochApplyPoolUsesInlineApply()) {
        return E_NodeManager::GetLogicalEpoch();
    }
    return EpochApplyQueueGetApplyEpoch();
}

void EpochApplyPoolEnsureApplyEpoch(uint64_t floor_epoch)
{
    if (EpochApplyPoolUsesInlineApply() || floor_epoch == 0) {
        return;
    }
    uint64_t cur = EpochApplyQueueGetApplyEpoch();
    if (cur == 0 || cur < floor_epoch) {
        EpochApplyQueueSetApplyEpoch(floor_epoch);
    }
}

bool EpochApplyPoolTryAdvanceApplyEpoch(uint64_t completed_epoch)
{
    if (EpochApplyPoolUsesInlineApply()) {
        return false;
    }
    return EpochApplyQueueTryAdvanceApplyEpoch(completed_epoch);
}

bool EpochApplyPoolSubmit(const EpochApplyPoolTask& task)
{
    if (EpochApplyPoolUsesInlineApply()) {
        return EpochApplyPoolExecuteInline(task);
    }

    std::string payload;
    if (!E_WireSerializeTxn(task.apply_txn, &payload)) {
        return false;
    }

    uint32_t n_workers = static_cast<uint32_t>(g_epoch_commit_threads < 1 ? 1 : g_epoch_commit_threads);
    if (n_workers > EPOCH_MAX_APPLY_WORKERS) {
        n_workers = EPOCH_MAX_APPLY_WORKERS;
    }
    uint32_t route_worker = E_WireTxnRouteWorker(task.apply_txn.route_wid, task.wire_key, n_workers);

    return EpochApplyQueueSubmit(task.epoch,
        task.wire_key,
        task.local_node,
        EpochApplyPoolBuildFlags(task),
        payload.data(),
        static_cast<uint32_t>(payload.size()),
        route_worker,
        n_workers);
}

bool EpochApplyPoolSubmitSerialized(uint64_t epoch,
    uint64_t wire_key,
    uint32_t local_node,
    bool is_local,
    const char* payload,
    uint32_t payload_len,
    uint32_t extra_flags)
{
    if (EpochApplyPoolUsesInlineApply()) {
        return false;
    }
    if (payload == nullptr || payload_len == 0) {
        return false;
    }

    uint32_t flags = extra_flags;
    if (is_local) {
        flags |= EPOCH_APPLY_FLAG_IS_LOCAL;
    }
    uint16_t route_wid = 0;
    (void)E_WirePeekRouteWid(payload, payload_len, &route_wid);
    uint32_t n_workers = static_cast<uint32_t>(g_epoch_commit_threads < 1 ? 1 : g_epoch_commit_threads);
    if (n_workers > EPOCH_MAX_APPLY_WORKERS) {
        n_workers = EPOCH_MAX_APPLY_WORKERS;
    }
    uint32_t route_worker = E_WireTxnRouteWorker(route_wid, wire_key, n_workers);
    return EpochApplyQueueSubmit(epoch, wire_key, local_node, flags, payload, payload_len, route_worker, n_workers);
}
