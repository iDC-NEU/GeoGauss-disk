#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <unistd.h>

#include "postmaster/epoch_worker.h"
#include "epoch/e_config.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_log_path.h"
#include "epoch/e_net.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_send_queue.h"
#include "epoch/e_txn_queue.h"
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
    EpochTxnQueueShmemInit(EpochGetTxnQueueCapacity());
    EpochSendQueueInit();
    /* 等 epochcoord BootstrapOwner 完成 ZMQ 绑定 */

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    worker_context = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "epoch send worker",
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
        ResourceOwnerCreate(NULL, "epoch send worker", THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    pgstat_report_appname("epoch send worker");
    pgstat_report_activity(STATE_IDLE, NULL);

    while (!EpochNetIsReady() && !t_thrd.epoch_worker_cxt.shutdown_requested) {
        pg_usleep(10000L);
    }

    ereport(LOG, (errmsg("epoch send worker started (pid=%lu)", (unsigned long)t_thrd.proc_cxt.MyProcPid)));

    for (;;) {
        if (t_thrd.epoch_worker_cxt.got_SIGHUP) {
            t_thrd.epoch_worker_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        if (t_thrd.epoch_worker_cxt.shutdown_requested) {
            proc_exit(0);
        }

        if (!EpochPipelineSendWorkerTick()) {
            pgstat_report_activity(STATE_IDLE, NULL);
            pg_usleep(200L);
            continue;
        }
        pgstat_report_activity(STATE_RUNNING, NULL);
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
