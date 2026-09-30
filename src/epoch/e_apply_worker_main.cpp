#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <sys/time.h>
#include <unistd.h>

#include "postmaster/epoch_worker.h"
#include "epoch/e_abort_csn.h"
#include "epoch/e_apply.h"
#include "epoch/e_apply_shmem.h"
#include "epoch/e_apply_stats.h"
#include "epoch/e_apply_profile.h"
#include "epoch/e_config.h"
#include "epoch/e_crdt_shmem.h"
#include "epoch/e_log_path.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_statistician.h"
#include "epoch/e_tip_staging_shmem.h"
#include "epoch/e_pk_staging_shmem.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_types.h"
#include "knl/knl_instance.h"

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "storage/ipc.h"
#include "storage/lock/lwlock.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "gssignal/gs_signal.h"

static void EpochApplyWorkerQuickDie(SIGNAL_ARGS);
static void EpochApplyWorkerSigHup(SIGNAL_ARGS);
static void EpochApplyWorkerShutdown(SIGNAL_ARGS);

static int EpochApplyWorkerResolveId(void)
{
    ThreadId mypid = t_thrd.proc_cxt.MyProcPid;
    for (int retry = 0; retry < 2000; ++retry) {
        for (int i = 0; i < static_cast<int>(EPOCH_MAX_APPLY_WORKERS); ++i) {
            if (g_instance.pid_cxt.EpochApplyWorkerPIDs[i] == mypid) {
                return i;
            }
        }
        pg_usleep(1000L);
    }
    return 0;
}

static uint32_t EpochApplyWorkerNumWorkers(void)
{
    int n = g_epoch_commit_threads;
    if (n < 1) {
        n = 1;
    }
    if (n > static_cast<int>(EPOCH_MAX_APPLY_WORKERS)) {
        n = static_cast<int>(EPOCH_MAX_APPLY_WORKERS);
    }
    return static_cast<uint32_t>(n);
}

static bool EpochApplyWorkerExecuteTask(EpochApplyTaskSlot* task)
{
    if (g_epoch_skip_apply) {
        EpochApplyStatsNoteCommit();
        E_NodeManager::IncCommittedTxnCounters(task->epoch, task->local_node);
        return true;
    }

    EpochApplyProfileBeginWireTxn();
    if (task->submit_us > 0) {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        uint64_t now_us = static_cast<uint64_t>(tv.tv_sec) * 1000000ULL + static_cast<uint64_t>(tv.tv_usec);
        if (now_us >= task->submit_us) {
            EpochApplyProfileAddUs(EAP_QUEUE_WAIT, now_us - task->submit_us);
        }
    }

    E_WireTxn source_txn;
    instr_time deserialize_start;
    INSTR_TIME_SET_CURRENT(deserialize_start);
    bool deserialized = E_WireDeserializeTxn(task->payload, task->payload_len, &source_txn);
    instr_time deserialize_end;
    INSTR_TIME_SET_CURRENT(deserialize_end);
    INSTR_TIME_SUBTRACT(deserialize_end, deserialize_start);
    EpochApplyProfileAddUs(EAP_DESERIALIZE, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(deserialize_end)));

    if (!deserialized) {
        ereport(WARNING,
            (errmsg("epoch apply worker: failed to deserialize wire txn epoch=%lu wire_key=%lu",
                task->epoch,
                task->wire_key)));
        return false;
    }

    /*
     * 入环 payload 已是赢家原始 wire。insert→heap/pk staging，update/delete→tip staging；
     * 不再从 CRDT BuildApplyTxn 重建写集。
     */
    if (EpochAbortCsnContains(task->epoch, source_txn.csn, source_txn.server_id)) {
        /* 输家不应入环；丢弃槽位，勿 IncCommitted（否则 committed>expected 卡死） */
        EpochApplyStatsNoteAbort(OCC_ABORT_REMOTE_MERGE);
        return true;
    }

    uint64_t row_count = source_txn.rows.size();
    const char* apply_tag = (task->flags & EPOCH_APPLY_FLAG_IS_LOCAL) != 0 ? "APPLY_LOCAL" : "APPLY_REMOTE";
    EpochRecvDiagLogWireRelSummary(apply_tag,
        task->epoch,
        source_txn.csn,
        source_txn.server_id,
        source_txn.rows);

    bool apply_ok = E_ApplyWireTxnCore(source_txn, (task->flags & EPOCH_APPLY_FLAG_IS_LOCAL) != 0);
    EpochRecvDiagOnApply(task->epoch, row_count, apply_ok);
    if (!apply_ok) {
        return false;
    }
    EpochApplyStatsNoteCommit();
    E_NodeManager::IncCommittedTxnCounters(task->epoch, task->local_node);
    return true;
}

void EpochApplyWorkerMain(void)
{
    sigjmp_buf local_sigjmp_buf;
    MemoryContext worker_context;
    sigset_t old_sig_mask;

    IsUnderPostmaster = true;
    t_thrd.role = EPOCHAPPLYWORKER;
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    t_thrd.proc_cxt.MyStartTime = time(NULL);
    t_thrd.proc_cxt.MyProgName = "EpochApplyWorker";

    init_ps_display("epoch apply worker process", "", "", "");
    SetProcessingMode(InitProcessing);

    (void)gspqsignal(SIGHUP, EpochApplyWorkerSigHup);
    (void)gspqsignal(SIGINT, EpochApplyWorkerShutdown);
    (void)gspqsignal(SIGTERM, EpochApplyWorkerShutdown);
    (void)gspqsignal(SIGQUIT, EpochApplyWorkerQuickDie);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_DFL);

    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    BaseInit();
#ifndef EXEC_BACKEND
    InitProcess();
#endif
    EpochLogPathInit();
    EpochApplyQueueShmemInit();
    EpochCrdtShmemInit();
    EpochTipStagingShmemInit();
    EpochPkStagingShmemInit();

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    if (!t_thrd.mem_cxt.mask_password_mem_cxt) {
        t_thrd.mem_cxt.mask_password_mem_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt,
            "MaskPasswordCtx",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    }

    worker_context = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "epoch apply worker",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(worker_context);

    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        pthread_sigmask(SIG_SETMASK, &old_sig_mask, NULL);
        t_thrd.log_cxt.error_context_stack = NULL;
        HOLD_INTERRUPTS();
        EmitErrorReport();
        FlushErrorState();
        AbortOutOfAnyTransaction();
        LWLockReleaseAll();
        pgstat_report_waitevent(WAIT_EVENT_END);
        AbortBufferIO();
        UnlockBuffers();
        if (t_thrd.utils_cxt.CurrentResourceOwner != nullptr) {
            ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
        }
        AtEOXact_Buffers(false);
        AtEOXact_SMgr();
        AtEOXact_Files();
        AtEOXact_HashTables(false);
        (void)MemoryContextSwitchTo(worker_context);
        MemoryContextResetAndDeleteChildren(worker_context);
        RESUME_INTERRUPTS();
        pg_usleep(1000000L);
    }

    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;
    SetProcessingMode(NormalProcessing);

    Assert(t_thrd.utils_cxt.CurrentResourceOwner == NULL);
    t_thrd.utils_cxt.CurrentResourceOwner =
        ResourceOwnerCreate(NULL, "epoch apply worker", THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    EpochWorkerInitApplySession();
    E_ApplyWarmup();

    t_thrd.epoch_worker_cxt.apply_worker_id = EpochApplyWorkerResolveId();

    pgstat_report_appname("epoch apply worker");
    pgstat_report_activity(STATE_IDLE, NULL);

    while (!EpochPipelineIsBootstrapped() && !t_thrd.epoch_worker_cxt.shutdown_requested) {
        pg_usleep(1000L);
    }

    ereport(LOG,
        (errmsg("epoch apply worker started (pid=%lu worker_id=%d n_workers=%u)",
            (unsigned long)t_thrd.proc_cxt.MyProcPid,
            t_thrd.epoch_worker_cxt.apply_worker_id,
            EpochApplyWorkerNumWorkers())));

    for (;;) {
        if (t_thrd.epoch_worker_cxt.got_SIGHUP) {
            t_thrd.epoch_worker_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        if (t_thrd.epoch_worker_cxt.shutdown_requested || EpochApplyQueueShutdownRequested()) {
            proc_exit(0);
        }

        EpochApplyTaskSlot* task = nullptr;
        if (!EpochApplyQueueTryClaim(static_cast<uint32_t>(t_thrd.epoch_worker_cxt.apply_worker_id),
                EpochApplyWorkerNumWorkers(),
                &task)) {
            pgstat_report_activity(STATE_IDLE, NULL);
            pg_usleep(200L);
            continue;
        }

        /* 硬门闩：apply(E) 须 merge_complete(E)；未关窗则退回 READY */
        if (!EpochPipelineEpochMergeComplete(task->epoch)) {
            task->state.store(EPOCH_APPLY_SLOT_READY, std::memory_order_release);
            pg_usleep(200L);
            continue;
        }

        pgstat_report_activity(STATE_RUNNING, NULL);
        bool ok = EpochApplyWorkerExecuteTask(task);
        EpochApplyQueueFinishTask(task, ok);
        pgstat_report_activity(STATE_IDLE, NULL);
    }
}

static void EpochApplyWorkerQuickDie(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
    on_exit_reset();
    _exit(2);
}

static void EpochApplyWorkerSigHup(SIGNAL_ARGS)
{
    t_thrd.epoch_worker_cxt.got_SIGHUP = true;
}

static void EpochApplyWorkerShutdown(SIGNAL_ARGS)
{
    t_thrd.epoch_worker_cxt.shutdown_requested = true;
}
