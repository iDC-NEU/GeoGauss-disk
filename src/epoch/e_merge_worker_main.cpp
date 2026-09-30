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
#include "epoch/e_merge_profile.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_statistician.h"
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

static bool EpochMergeWorkerExecuteTask(EpochMergeTaskSlot* task)
{
    EpochMergeProfileBeginWireTxn();
    if (task->submit_us > 0) {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        uint64_t now_us = static_cast<uint64_t>(tv.tv_sec) * 1000000ULL + static_cast<uint64_t>(tv.tv_usec);
        if (now_us >= task->submit_us) {
            EpochMergeProfileAddUs(EMP_QUEUE_WAIT, now_us - task->submit_us);
        }
    }

    E_WireTxn txn;
    instr_time deserialize_start;
    INSTR_TIME_SET_CURRENT(deserialize_start);
    bool deserialized = E_MergeTaskDeserialize(task->payload, task->payload_len, &txn);
    instr_time deserialize_end;
    INSTR_TIME_SET_CURRENT(deserialize_end);
    INSTR_TIME_SUBTRACT(deserialize_end, deserialize_start);
    EpochMergeProfileAddUs(EMP_DESERIALIZE, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(deserialize_end)));

    if (!deserialized) {
        ereport(WARNING,
            (errmsg("epoch merge worker: failed to deserialize wire txn epoch=%lu wire_key=%lu",
                task->epoch,
                task->wire_key)));
        EpochMergeDoneSlot done;
        done.epoch = task->epoch;
        done.wire_key = task->wire_key;
        done.csn = 0;
        done.server_id = 0;
        done.ok = 0;
        done.txn_accepted = 0;
        done.any_new_merged = 0;
        done.pad = 0;
        instr_time finish_start;
        INSTR_TIME_SET_CURRENT(finish_start);
        EpochMergeQueueFinishTask(task, &done);
        EpochMergePoolInvokeDoneHook(&done);
        instr_time finish_end;
        INSTR_TIME_SET_CURRENT(finish_end);
        INSTR_TIME_SUBTRACT(finish_end, finish_start);
        EpochMergeProfileAddUs(EMP_QUEUE_FINISH, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(finish_end)));
        EpochMergeProfileEndWireTxn(task->epoch, 0, 0, false);
        return false;
    }

    E_CrdtMergeOutcome outcome = EpochCrdtShmemMergeWireTxn(txn);
    bool ok = !outcome.resource_failure;
    instr_time post_start;
    INSTR_TIME_SET_CURRENT(post_start);
    if (ok) {
        EpochAbortCsnRecordMergeOutcome(txn.epoch, txn, outcome);
        /* 赢家入 apply 环：仅在 Finish→done hook（OnAsyncMergeTaskDone），此处不投 */
    } else {
        EpochDiagLogMergeLoserAbortCsn(txn.epoch, txn.csn, txn.server_id, true, false);
    }
    instr_time post_end;
    INSTR_TIME_SET_CURRENT(post_end);
    INSTR_TIME_SUBTRACT(post_end, post_start);
    EpochMergeProfileAddUs(EMP_POST_HOOKS, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(post_end)));

    EpochMergeDoneSlot done;
    EpochMergePoolPackDoneFromOutcome(&done, txn, task->wire_key, ok, outcome);

    instr_time finish_start;
    INSTR_TIME_SET_CURRENT(finish_start);
    EpochMergeQueueFinishTask(task, &done);
    EpochMergePoolInvokeDoneHook(&done);
    instr_time finish_end;
    INSTR_TIME_SET_CURRENT(finish_end);
    INSTR_TIME_SUBTRACT(finish_end, finish_start);
    EpochMergeProfileAddUs(EMP_QUEUE_FINISH, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(finish_end)));
    EpochMergeProfileEndWireTxn(txn.epoch, txn.csn, txn.server_id, ok && outcome.txn_accepted);
    return true;
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
    EpochMergeQueueShmemInit();
    /* merge_complete 后顺带 ApplyQueueSubmit，需 attach apply 环 */
    EpochApplyQueueShmemInit();

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

        /* Claim 优先：对着 merge_epoch 执行；PullTick 只推进游标/Drain/关窗 */
        EpochMergeTaskSlot* task = nullptr;
        if (EpochMergeQueueTryClaim(wid, n_workers, &task)) {
            pgstat_report_activity(STATE_RUNNING, NULL);
            (void)EpochMergeWorkerExecuteTask(task);
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
