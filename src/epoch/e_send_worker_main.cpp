#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <unistd.h>

#include "postmaster/epoch_worker.h"
#include "epoch/e_config.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_log_path.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_shmem.h"
#include "epoch/e_stuck_trace.h"
#include "epoch/e_txn_queue.h"
#include "epoch/e_types.h"
#include "knl/knl_instance.h"

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "gssignal/gs_signal.h"

static void EpochSendWorkerQuickDie(SIGNAL_ARGS);
static void EpochSendWorkerSigHup(SIGNAL_ARGS);
static void EpochSendWorkerShutdown(SIGNAL_ARGS);

static int EpochSendWorkerResolveId(void)
{
    ThreadId mypid = t_thrd.proc_cxt.MyProcPid;
    for (int retry = 0; retry < 2000; ++retry) {
        for (int i = 0; i < static_cast<int>(EPOCH_MAX_SEND_WORKERS); ++i) {
            if (g_instance.pid_cxt.EpochPackWorkerPIDs[i] == mypid) {
                return i;
            }
        }
        pg_usleep(1000L);
    }
    return 0;
}

static uint32_t EpochSendWorkerNumWorkers(void)
{
    int n = g_epoch_send_threads;
    if (n < 1) {
        n = 1;
    }
    if (n > static_cast<int>(EPOCH_MAX_SEND_WORKERS)) {
        n = static_cast<int>(EPOCH_MAX_SEND_WORKERS);
    }
    return static_cast<uint32_t>(n);
}

void EpochSendWorkerMain(void)
{
    sigjmp_buf local_sigjmp_buf;
    MemoryContext worker_context;
    sigset_t old_sig_mask;

    IsUnderPostmaster = true;
    t_thrd.role = EPOCHSENDWORKER;
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    t_thrd.proc_cxt.MyStartTime = time(NULL);
    t_thrd.proc_cxt.MyProgName = "EpochSendWorker";

    init_ps_display("epoch send worker process", "", "", "");
    SetProcessingMode(InitProcessing);

    (void)gspqsignal(SIGHUP, EpochSendWorkerSigHup);
    (void)gspqsignal(SIGINT, EpochSendWorkerShutdown);
    (void)gspqsignal(SIGTERM, EpochSendWorkerShutdown);
    (void)gspqsignal(SIGQUIT, EpochSendWorkerQuickDie);
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
    EpochShmemInit();
    EpochTxnQueueShmemInit(EpochGetTxnQueueCapacity());

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    worker_context = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "epoch pack worker",
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
        LWLockReleaseAll();
        pgstat_report_waitevent(WAIT_EVENT_END);
        (void)MemoryContextSwitchTo(worker_context);
        MemoryContextResetAndDeleteChildren(worker_context);
        RESUME_INTERRUPTS();
        pg_usleep(1000000L);
    }

    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;
    SetProcessingMode(NormalProcessing);

    Assert(t_thrd.utils_cxt.CurrentResourceOwner == NULL);
    t_thrd.utils_cxt.CurrentResourceOwner =
        ResourceOwnerCreate(NULL, "epoch pack worker", THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    t_thrd.epoch_worker_cxt.pack_worker_id = EpochSendWorkerResolveId();
    {
        char trace_name[16];
        (void)snprintf(trace_name, sizeof(trace_name), "pack%d", t_thrd.epoch_worker_cxt.pack_worker_id);
        EpochStuckTraceRegister(trace_name);
    }

    pgstat_report_appname("epoch pack worker");
    pgstat_report_activity(STATE_IDLE, NULL);

    /* 等 epoch 协调进程拉起流水线；本进程只负责发送 */
    while (!EpochPipelineIsBootstrapped() && !t_thrd.epoch_worker_cxt.shutdown_requested) {
        pg_usleep(1000L);
    }

    ereport(LOG,
        (errmsg("epoch pack worker started (pid=%lu worker_id=%d n_workers=%u)",
            (unsigned long)t_thrd.proc_cxt.MyProcPid,
            t_thrd.epoch_worker_cxt.pack_worker_id,
            EpochSendWorkerNumWorkers())));

    /* 每个对端一条发送线程：慢对端只阻塞它自己 */
    if (t_thrd.epoch_worker_cxt.pack_worker_id == 0) {
        EpochPipelineStartPeerSendThreads();
    }

    for (;;) {
        if (t_thrd.epoch_worker_cxt.got_SIGHUP) {
            t_thrd.epoch_worker_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        if (t_thrd.epoch_worker_cxt.shutdown_requested) {
            proc_exit(0);
        }

        /* 主线程只做周期维护（网卡采样 + pacer），发送在各 peer 线程里 */
        (void)EpochPipelineSendWorkerTick(0, EpochSendWorkerNumWorkers());
        pgstat_report_activity(STATE_IDLE, NULL);
        pg_usleep(1000L);
    }
}

static void EpochSendWorkerQuickDie(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
    on_exit_reset();
    _exit(2);
}

static void EpochSendWorkerSigHup(SIGNAL_ARGS)
{
    t_thrd.epoch_worker_cxt.got_SIGHUP = true;
}

static void EpochSendWorkerShutdown(SIGNAL_ARGS)
{
    t_thrd.epoch_worker_cxt.shutdown_requested = true;
}
