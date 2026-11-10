#include "postgres.h"

#include "epoch/e_exec_track.h"
#include "epoch/e_config.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_statistician.h"

#include "gs_thread.h"
#include "knl/knl_variable.h"
#include "knl/knl_thread.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"

static bool EpochShouldTrackExecuting(void)
{
    if (!g_enable_epoch_multimaster) {
        return false;
    }
    if (IsAutoVacuumWorkerProcess() || AmEpochWorkerProcess()) {
        return false;
    }
    if (t_thrd.role == EPOCHWORKER || t_thrd.role == EPOCHAPPLYWORKER || t_thrd.role == EPOCHMERGEWORKER ||
        t_thrd.role == EPOCHPACKWORKER || t_thrd.role == EPOCHSENDWORKER ||
        t_thrd.role == EPOCHTIPFLUSHER) {
        return false;
    }
    if (t_thrd.role != WORKER && t_thrd.role != THREADPOOL_WORKER) {
        return false;
    }
    if (!IsNormalProcessingMode()) {
        return false;
    }
    if (u_sess->proc_cxt.MyProcPort == nullptr || u_sess->proc_cxt.MyProcPort->database_name == nullptr) {
        return false;
    }
    const char* track_db = (g_epoch_apply_database != nullptr && g_epoch_apply_database[0] != '\0')
        ? g_epoch_apply_database
        : "test";
    if (strcmp(u_sess->proc_cxt.MyProcPort->database_name, track_db) != 0) {
        return false;
    }
    return u_sess->epoch_cxt.epoch_assigned && !u_sess->epoch_cxt.epoch_exec_counted;
}

void EpochMaybeMarkTxnExecuting(void)
{
    EpochEnsureTxnEpochOnAccess(true);
}

void EpochTrackExecutingInc(uint64_t epoch, const char* reason)
{
    if (u_sess->epoch_cxt.epoch_exec_counted) {
        return;
    }
    uint32_t node_id = EpochGetNodeId();
    uint64_t after = E_NodeManager::IncExecutingTxnCounters(epoch, node_id);
    if (after == 0) {
        return;
    }
    u_sess->epoch_cxt.epoch_exec_counted = true;
}

void EpochReleaseTxnExecuting(const char* reason)
{
    if (!u_sess->epoch_cxt.epoch_exec_counted) {
        return;
    }
    uint64_t epoch = u_sess->epoch_cxt.txn_epoch;
    uint32_t node_id = EpochGetNodeId();
    const char* log_reason = reason;
    if (!E_NodeManager::DecExecutingTxnCounters(epoch, node_id)) {
        log_reason = "exec_dec_miss";
    }
    u_sess->epoch_cxt.epoch_exec_counted = false;
}

void EpochMaybeLogTxnProgress(const char* tag, const char* detail)
{
    if (!u_sess->epoch_cxt.epoch_exec_counted || u_sess->epoch_cxt.deferred_commit) {
        return;
    }
}
