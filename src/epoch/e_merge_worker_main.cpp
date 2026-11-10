#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <sys/time.h>
#include <unistd.h>

#include "postmaster/epoch_worker.h"
#include "epoch/e_apply.h"
#include "epoch/e_apply_shmem.h"
#include "epoch/e_config.h"
#include "epoch/e_abort_csn.h"
#include "epoch/e_crdt_shmem.h"
#include "epoch/e_log_path.h"
#include "epoch/e_merge_pool.h"
#include "epoch/e_merge_shmem.h"
#include "epoch/e_perf.h"
#include "epoch/e_merge_profile.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_pacer.h"
#include "epoch/e_statistician.h"
#include "epoch/e_stuck_trace.h"
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

static void EpochMergeWorkerQuickDie(SIGNAL_ARGS);
static void EpochMergeWorkerSigHup(SIGNAL_ARGS);
static void EpochMergeWorkerShutdown(SIGNAL_ARGS);

static int EpochMergeWorkerResolveId(void)
{
    ThreadId mypid = t_thrd.proc_cxt.MyProcPid;
    for (int retry = 0; retry < 2000; ++retry) {
        for (int i = 0; i < static_cast<int>(EPOCH_MAX_MERGE_WORKERS); ++i) {
            if (g_instance.pid_cxt.EpochMergeWorkerPIDs[i] == mypid) {
                return i;
            }
        }
        pg_usleep(1000L);
    }
    return 0;
}

static uint32_t EpochMergeWorkerNumWorkers(void)
{
    int n = g_epoch_merge_threads;
    if (n < 1) {
        n = 1;
    }
    if (n > static_cast<int>(EPOCH_MAX_MERGE_WORKERS)) {
        n = static_cast<int>(EPOCH_MAX_MERGE_WORKERS);
    }
    return static_cast<uint32_t>(n);
}

/* 当前 worker 正在处理的写集，ERROR 恢复时用来补记（避免 merged 少一笔） */
static thread_local bool g_merge_task_active = false;
static thread_local uint64_t g_merge_task_epoch = 0;
static thread_local uint64_t g_merge_task_wire_key = 0;

/*
 * 合并一笔写集。全部工作就两步：
 *   ① CRDT 合并（冲突裁决、abort 记录、赢家入 apply 都在里面）
 *   ② merged++（merged == declared 时 merge 游标才会前进）
 */
static void EpochMergeWorkerExecuteOne(const E_WireTxn& txn)
{
    g_merge_task_active = true;
    g_merge_task_epoch = txn.epoch;
    g_merge_task_wire_key = txn.csn ^ (static_cast<uint64_t>(txn.server_id) << 48);

    /* ① CRDT 合并（冲突裁决、abort 记录、赢家入 apply 都在里面完成） */
    (void)EpochCrdtShmemMergeWireTxn(txn);

    /* PA：被跳过片里判赢的事务，缓存起来等这片 merge 完再转发给慢节点 */
    EpochPacerPaMaybeBufferMerged(txn);

    /* ② 计一笔已合并；merged == declared 时 merge 游标才会前进 */
    const uint64_t merged_now = E_NodeManager::IncWireMergeDoneCounters(txn.epoch, EpochGetNodeId());
    EPOCH_DBG_FPRINTF("EPOCH_DBG merged_inc epoch=%lu merged=%lu node=%u csn=%lu\n",
        static_cast<unsigned long>(txn.epoch),
        static_cast<unsigned long>(merged_now),
        txn.server_id,
        static_cast<unsigned long>(txn.csn));

    g_merge_task_active = false;
}

void EpochMergeWorkerMain(void)
{
    sigjmp_buf local_sigjmp_buf;
    MemoryContext worker_context;
    sigset_t old_sig_mask;

    IsUnderPostmaster = true;
    t_thrd.role = EPOCHMERGEWORKER;
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    t_thrd.proc_cxt.MyStartTime = time(NULL);
    t_thrd.proc_cxt.MyProgName = "EpochMergeWorker";

    init_ps_display("epoch merge worker process", "", "", "");
    SetProcessingMode(InitProcessing);

    (void)gspqsignal(SIGHUP, EpochMergeWorkerSigHup);
    (void)gspqsignal(SIGINT, EpochMergeWorkerShutdown);
    (void)gspqsignal(SIGTERM, EpochMergeWorkerShutdown);
    (void)gspqsignal(SIGQUIT, EpochMergeWorkerQuickDie);
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
    EpochAbortCsnShmemInit();
    EpochCrdtShmemInit();
    /* merge_complete 后顺带 ApplyQueueSubmit，需 attach apply 环 */

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
        "epoch merge worker",
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
        /* ERROR 恢复：把 ERROR 前正在处理的 task 归还，保证 inflight 一定归零 */
        if (g_merge_task_active) {
            /* 这一笔没合并成功：仍要记一笔 merged，否则该片永远差一条、游标推不动 */
            EpochPipelineRevokeAcceptedWire(g_merge_task_epoch, g_merge_task_wire_key);
            (void)E_NodeManager::IncWireMergeDoneCounters(g_merge_task_epoch, EpochGetNodeId());
            g_merge_task_active = false;
        }
        (void)MemoryContextSwitchTo(worker_context);
        MemoryContextResetAndDeleteChildren(worker_context);
        RESUME_INTERRUPTS();
        pg_usleep(1000000L);
    }

    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;
    SetProcessingMode(NormalProcessing);

    Assert(t_thrd.utils_cxt.CurrentResourceOwner == NULL);
    t_thrd.utils_cxt.CurrentResourceOwner =
        ResourceOwnerCreate(NULL, "epoch merge worker", THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    EpochWorkerInitApplySession();
    E_ApplyWarmup();

    t_thrd.epoch_worker_cxt.merge_worker_id = EpochMergeWorkerResolveId();
    EpochMergeProfileSetWorker(t_thrd.epoch_worker_cxt.merge_worker_id);
    {
        char trace_name[16];
        (void)snprintf(trace_name, sizeof(trace_name), "merge%d", t_thrd.epoch_worker_cxt.merge_worker_id);
        EpochStuckTraceRegister(trace_name);
    }

    pgstat_report_appname("epoch merge worker");
    pgstat_report_activity(STATE_IDLE, NULL);

    while (!EpochPipelineIsBootstrapped() && !t_thrd.epoch_worker_cxt.shutdown_requested) {
        pg_usleep(1000L);
    }

    ereport(LOG,
        (errmsg("epoch merge worker started (pid=%lu worker_id=%d n_workers=%u)",
            (unsigned long)t_thrd.proc_cxt.MyProcPid,
            t_thrd.epoch_worker_cxt.merge_worker_id,
            EpochMergeWorkerNumWorkers())));

    for (;;) {
        if (t_thrd.epoch_worker_cxt.got_SIGHUP) {
            t_thrd.epoch_worker_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        if (t_thrd.epoch_worker_cxt.shutdown_requested || EpochMergeQueueShutdownRequested()) {
            proc_exit(0);
        }

        const uint32_t wid = static_cast<uint32_t>(t_thrd.epoch_worker_cxt.merge_worker_id);
        const uint32_t n_workers = EpochMergeWorkerNumWorkers();

        /* 从 merge_epoch 那一格取一条；取到就合并，然后让 pipeline 检查能否推进游标 */
        const uint64_t epoch = GetMergeEpoch();
        E_WireTxn txn;
        EpochPipelineLogMergeStall();
        if (epoch > 0 && EpochMergeQueueTryDequeue(epoch, &txn)) {
            EpochStuckTraceNote(EST_MERGE_CLAIM, wid, txn.epoch);
            pgstat_report_activity(STATE_RUNNING, NULL);
            EpochMergeWorkerExecuteOne(txn);
            (void)EpochPipelineMergeWorkerPullTick(wid, n_workers);
            pgstat_report_activity(STATE_IDLE, NULL);
            continue;
        }

        bool pulled = EpochPipelineMergeWorkerPullTick(wid, n_workers);
        if (!pulled) {
            pgstat_report_activity(STATE_IDLE, NULL);
            pg_usleep(200L);
        }
    }
}

static void EpochMergeWorkerQuickDie(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
    on_exit_reset();
    _exit(2);
}

static void EpochMergeWorkerSigHup(SIGNAL_ARGS)
{
    t_thrd.epoch_worker_cxt.got_SIGHUP = true;
}

static void EpochMergeWorkerShutdown(SIGNAL_ARGS)
{
    t_thrd.epoch_worker_cxt.shutdown_requested = true;
}
