#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <unistd.h>

#include "postmaster/epoch_worker.h"
#include "epoch/e_apply.h"
#include "epoch/e_apply_pool.h"
#include "epoch/e_apply_shmem.h"
#include "epoch/e_config.h"
#include "epoch/e_merge_pool.h"
#include "epoch/e_log_path.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_statistician.h"
#include "epoch/e_stuck_trace.h"
#include "epoch/e_perf.h"
#include "epoch/e_epoch_lifecycle.h"

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/lock/lwlock.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "occ/o_staging.h"
#include "gssignal/gs_signal.h"


/*
 * GeoGauss: MOT epoch 线程入口 MOT::CreateSessionContext() 绑定 store。
 * 磁盘表等价路径：logical apply worker（主备逻辑复制）的 InitApplyWorker + replica 角色。
 * 参见 replication/logical/worker.cpp ApplyWorkerMain()。
 */
void EpochWorkerInitApplySession(void)
{
    ResourceOwner worker_resowner = t_thrd.utils_cxt.CurrentResourceOwner;
    const char* dbname = (g_epoch_apply_database != nullptr && g_epoch_apply_database[0] != '\0')
        ? g_epoch_apply_database
        : "test";
    const char* username = (g_epoch_apply_user != nullptr && g_epoch_apply_user[0] != '\0')
        ? g_epoch_apply_user
        : "hcj2003";
    char resolvedDbname[NAMEDATALEN] = {0};

    /* 与 ApplyWorkerMain 相同：以 replica 角色 apply，避免触发本地 trigger */
    SetConfigOption("session_replication_role", "replica", PGC_SUSET, PGC_S_OVERRIDE);
    SetConfigOption("search_path", "", PGC_SUSET, PGC_S_OVERRIDE);

    /*
     * 传库名/用户名而非 OID：InitApplyWorker 内 InitSysCache 完成后才访问 pg_catalog。
     * 若在 InitApplyWorker 之前调用 get_database_oid，会因 phase2 未初始化触发断言。
     */
    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(dbname, InvalidOid, username, InvalidOid);
    t_thrd.proc_cxt.PostInit->InitApplyWorker();
    t_thrd.proc_cxt.PostInit->GetDatabaseName(resolvedDbname);

    MemoryContext oldctx = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    if (u_sess->proc_cxt.MyProcPort->database_name != nullptr) {
        pfree_ext(u_sess->proc_cxt.MyProcPort->database_name);
    }
    if (u_sess->proc_cxt.MyProcPort->user_name != nullptr) {
        pfree_ext(u_sess->proc_cxt.MyProcPort->user_name);
    }
    u_sess->proc_cxt.MyProcPort->database_name = pstrdup(resolvedDbname);
    u_sess->proc_cxt.MyProcPort->user_name = pstrdup(username);
    (void)MemoryContextSwitchTo(oldctx);

    /*
     * InitApplyWorker 会 commit 初始化事务并把 CurrentResourceOwner 置 NULL。
     * merge/epoch worker 的 heap bootstrap 读路径依赖 CurrentResourceOwner 管理
     * TupleDesc 引用，但不走 backend 的 StartTransactionCommand。
     */
    if (worker_resowner != nullptr) {
        t_thrd.utils_cxt.CurrentResourceOwner = worker_resowner;
    }
}

static constexpr long EPOCH_WORKER_PIPELINE_IDLE_US = 200;

static void EpochCoordQuickDie(SIGNAL_ARGS);
static void EpochCoordSigHup(SIGNAL_ARGS);
static void EpochCoordShutdown(SIGNAL_ARGS);

static void EpochCoordSigHup(SIGNAL_ARGS)
{
    t_thrd.epoch_worker_cxt.got_SIGHUP = true;
}

static void EpochCoordShutdown(SIGNAL_ARGS)
{
    t_thrd.epoch_worker_cxt.shutdown_requested = true;
}

static void EpochCoordQuickDie(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
    on_exit_reset();
    _exit(2);
}

/*
 * epoch 协调进程（EPOCHWORKER 角色）。
 *
 * 职责只有一件：在具备 backend 会话上下文的进程里拉起 epoch 流水线的
 * recv / merge / apply / logical / physical / state 六个线程，然后常驻待命。
 *
 * 为什么必须是进程而不是 postmaster 里的线程：这些子线程要写堆表、要访问
 * u_sess，必须继承 InitProcessAndShareMemory() 建立的会话环境。
 *
 * 它不参与发送，也不参与合并 —— 数据面全部由各自的工作线程承担。
 */
void EpochCoordWorkerMain(void)
{
    sigjmp_buf local_sigjmp_buf;
    MemoryContext worker_context;
    sigset_t old_sig_mask;

    IsUnderPostmaster = true;
    t_thrd.role = EPOCHWORKER;
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    t_thrd.proc_cxt.MyStartTime = time(NULL);
    t_thrd.proc_cxt.MyProgName = "EpochCoordWorker";

    init_ps_display("epoch coord worker process", "", "", "");
    SetProcessingMode(InitProcessing);

    (void)gspqsignal(SIGHUP, EpochCoordSigHup);
    (void)gspqsignal(SIGINT, EpochCoordShutdown);
    (void)gspqsignal(SIGTERM, EpochCoordShutdown);
    (void)gspqsignal(SIGQUIT, EpochCoordQuickDie);
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

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    worker_context = AllocSetContextCreate(t_thrd.top_mem_cxt,
                                           "epoch coord worker",
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
            ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner,
                RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
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

    t_thrd.utils_cxt.CurrentResourceOwner =
        ResourceOwnerCreate(NULL, "epoch coord worker", THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    EpochWorkerInitApplySession();

    pgstat_report_appname("epoch coord worker");
    pgstat_report_activity(STATE_IDLE, NULL);

    /* 拉起流水线；之后只待命 */
    EpochPipelineBootstrapOwner();

    for (;;) {
        if (t_thrd.epoch_worker_cxt.got_SIGHUP) {
            t_thrd.epoch_worker_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        if (t_thrd.epoch_worker_cxt.shutdown_requested) {
            EpochPipelineShutdown();
            proc_exit(0);
        }
        pgstat_report_activity(STATE_IDLE, NULL);
        pg_usleep(EPOCH_WORKER_PIPELINE_IDLE_US);
    }
}
