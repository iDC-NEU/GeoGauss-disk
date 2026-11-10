#include "postgres.h"

#include "epoch/e_perf.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <sys/time.h>
#include <unistd.h>

#include "postmaster/epoch_worker.h"
#include "epoch/e_abort_csn.h"
#include "epoch/e_apply.h"
#include "epoch/e_apply_shmem.h"
#include "epoch/e_merge_shmem.h"
#include "epoch/e_apply_stats.h"
#include "epoch/e_apply_profile.h"
#include "epoch/e_config.h"
#include "epoch/e_crdt_shmem.h"
#include "epoch/e_log_path.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_apply_pool.h"
#include "epoch/e_merge_pool.h"
#include "epoch/e_statistician.h"
#include "epoch/e_stuck_trace.h"
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
/*
 * 写回一笔事务：
 *   csn 在 abort 集合里 → 只计 abort，不写回；
 *   否则按 csn 去重后计一次 commit（同一事务的多行只算一笔），再写回。
 */
static bool EpochApplyWorkerExecuteOne(const E_WireTxn& txn)
{
    if (EpochAbortCsnContains(txn.epoch, txn.csn, txn.server_id)) {
        EpochApplyStatsNoteAbortN(1);
        return true;
    }

    if (EpochApplyCsnSeenFirst(txn.epoch, txn.csn)) {
        EpochApplyStatsNoteCommit();
    }

    const bool is_local = (txn.server_id == EpochGetNodeId());
    return E_ApplyWireTxnCore(txn, is_local);
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
    {
        char trace_name[16];
        (void)snprintf(trace_name, sizeof(trace_name), "apply%d", t_thrd.epoch_worker_cxt.apply_worker_id);
        EpochStuckTraceRegister(trace_name);
    }

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

        const uint64_t apply_ep = EpochApplyQueueGetApplyEpoch();
        const uint64_t merge_ep = GetMergeEpoch();

        /* apply 不能追上 merge：merge 还没处理完的时间片，写回数据还不存在 */
        if (apply_ep == 0 || merge_ep == 0 || apply_ep >= merge_ep) {
            pgstat_report_activity(STATE_IDLE, NULL);
            pg_usleep(200L);
            continue;
        }

        E_WireTxn txn;
        if (!EpochApplyQueueTryDequeue(apply_ep, &txn)) {
            /* 该格已取空 → 推进 apply 游标（CAS，多线程只会推进一次） */
            if (EpochApplyQueueTryAdvanceApplyEpoch(apply_ep)) {
                EpochPerfNoteEpochApplied(apply_ep);   /* 结算这一片的“封口→写回完成”延迟 */
            }
            continue;
        }

        pgstat_report_activity(STATE_RUNNING, NULL);
        EpochStuckTraceNote(EST_APPLY_CLAIM, static_cast<uint32_t>(txn.epoch), txn.csn);
        (void)EpochApplyWorkerExecuteOne(txn);
        EpochStuckTraceNote(EST_APPLY_DONE, 1, txn.epoch);
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
