#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <unistd.h>

#include "postmaster/epoch_worker.h"
#include "epoch/e_config.h"
#include "epoch/e_log_path.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_tip_staging_shmem.h"

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "gssignal/gs_signal.h"
#include "portability/instr_time.h"

#include "occ/o_staging.h"
#include "knl/knl_instance.h"
#include "epoch/e_types.h"

static void EpochTipFlushWorkerQuickDie(SIGNAL_ARGS);
static void EpochTipFlushWorkerSigHup(SIGNAL_ARGS);
static void EpochTipFlushWorkerShutdown(SIGNAL_ARGS);

static constexpr long EPOCH_TIP_FLUSH_IDLE_LOW_US = 5000L;   /* 5ms when below high */
static constexpr long EPOCH_TIP_FLUSH_IDLE_HIGH_US = 500L;  /* 0.5ms when pressing occupancy */
static constexpr long EPOCH_TIP_FLUSH_IDLE_CRIT_US = 0L;    /* spin when critical / urgent */

static int EpochTipFlushWorkerResolveId(void)
{
    ThreadId mypid = t_thrd.proc_cxt.MyProcPid;
    for (int retry = 0; retry < 2000; ++retry) {
        for (int i = 0; i < static_cast<int>(EPOCH_MAX_TIP_FLUSH_WORKERS); ++i) {
            if (g_instance.pid_cxt.EpochTipFlushWorkerPIDs[i] == mypid) {
                return i;
            }
        }
        pg_usleep(1000L);
    }
    return -1;
}

void EpochTipFlushWorkerMain(void)
{
    sigjmp_buf local_sigjmp_buf;
    MemoryContext worker_context;
    sigset_t old_sig_mask;

    IsUnderPostmaster = true;
    t_thrd.role = EPOCHTIPFLUSHER;
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    t_thrd.proc_cxt.MyStartTime = time(NULL);
    t_thrd.proc_cxt.MyProgName = "EpochTipFlushWorker";

    init_ps_display("epoch tip flush worker process", "", "", "");
    SetProcessingMode(InitProcessing);

    (void)gspqsignal(SIGHUP, EpochTipFlushWorkerSigHup);
    (void)gspqsignal(SIGINT, EpochTipFlushWorkerShutdown);
    (void)gspqsignal(SIGTERM, EpochTipFlushWorkerShutdown);
    (void)gspqsignal(SIGQUIT, EpochTipFlushWorkerQuickDie);
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
    EpochTipStagingShmemInit();

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
        "epoch tip flush worker",
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
        ResourceOwnerCreate(NULL, "epoch tip flush worker", THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    EpochWorkerInitApplySession();

    pgstat_report_appname("epoch tip flush worker");
    pgstat_report_activity(STATE_IDLE, NULL);

    while (!EpochPipelineIsBootstrapped() && !t_thrd.epoch_worker_cxt.shutdown_requested) {
        pg_usleep(1000L);
    }

    int worker_id = EpochTipFlushWorkerResolveId();
    ereport(LOG,
        (errmsg("epoch tip flush worker started (id=%d pid=%lu tip_staging=%d force_flush=%d drop_on_fail=%d)",
            worker_id,
            (unsigned long)t_thrd.proc_cxt.MyProcPid,
            g_epoch_local_tip_staging ? 1 : 0,
            /* mirror e_tip_staging_shmem.cpp EPOCH_TIP_FLUSH_FORCE_ENABLED / DROP_ON_FAIL */
            0,
            1)));

    uint64_t last_report_us = 0;
    EpochTipFlushProfile last_prof{};
    for (;;) {
        if (t_thrd.epoch_worker_cxt.got_SIGHUP) {
            t_thrd.epoch_worker_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        if (t_thrd.epoch_worker_cxt.shutdown_requested) {
            proc_exit(0);
        }

        if (!g_epoch_local_tip_staging) {
            pg_usleep(EPOCH_TIP_FLUSH_IDLE_LOW_US);
            continue;
        }

        pgstat_report_activity(STATE_RUNNING, NULL);
        size_t flushed = EpochTipStagingFlusherTick();
        pgstat_report_activity(STATE_IDLE, NULL);

        uint32_t used = EpochTipStagingTotalUsed();
        uint32_t high = (EPOCH_TIP_STAGING_NUM_ENTRIES * EPOCH_TIP_FLUSH_HIGH_PCT) / 100U;
        uint32_t critical = (EPOCH_TIP_STAGING_NUM_ENTRIES * EPOCH_TIP_FLUSH_CRITICAL_PCT) / 100U;
        bool urgent = EpochTipStagingFlushUrgent();

        instr_time now;
        INSTR_TIME_SET_CURRENT(now);
        uint64_t now_us = static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(now));
        if (last_report_us == 0 || now_us - last_report_us >= 5000000ULL) {
            uint64_t inplace = 0;
            uint64_t force = 0;
            uint64_t deleted = 0;
            uint64_t fail = 0;
            uint64_t pages = 0;
            uint64_t shard_full = 0;
            EpochTipStagingGetFlushCounters(&inplace, &force, &deleted, &fail, &pages, &shard_full);
            EpochTipFlushProfile prof{};
            EpochTipStagingGetFlushProfile(&prof);
            uint64_t d_tick = prof.tick_us - last_prof.tick_us;
            uint64_t d_pick = prof.pick_us - last_prof.pick_us;
            uint64_t d_find = prof.find_page_us - last_prof.find_page_us;
            uint64_t d_collect = prof.collect_us - last_prof.collect_us;
            uint64_t d_open = prof.open_us - last_prof.open_us;
            uint64_t d_inplace = prof.inplace_us - last_prof.inplace_us;
            uint64_t d_force = prof.force_us - last_prof.force_us;
            uint64_t d_delete = prof.delete_us - last_prof.delete_us;
            uint64_t d_xact = prof.xact_us - last_prof.xact_us;
            uint64_t d_evict = prof.dirty_evict - last_prof.dirty_evict;
            uint64_t d_evict_us = prof.dirty_evict_us - last_prof.dirty_evict_us;
            uint64_t d_touch_full = prof.dirty_touch_full - last_prof.dirty_touch_full;
            uint64_t d_touch_fail = prof.dirty_touch_fail - last_prof.dirty_touch_fail;
            uint64_t d_miss = prof.find_miss - last_prof.find_miss;
            uint64_t d_itry = prof.inplace_try - last_prof.inplace_try;
            uint64_t d_ftry = prof.force_try - last_prof.force_try;
            uint64_t d_tips = prof.tips_done - last_prof.tips_done;
            double denom = d_tick > 0 ? (double)d_tick : 1.0;
            ereport(LOG,
                (errmsg(
                    "epoch tip flush[%d]: used=%u/%u flushed_tick=%zu urgent=%d inplace=%lu force=%lu del=%lu fail=%lu pages=%lu shard_full=%lu",
                    worker_id,
                    used,
                    EPOCH_TIP_STAGING_NUM_ENTRIES,
                    flushed,
                    urgent ? 1 : 0,
                    (unsigned long)inplace,
                    (unsigned long)force,
                    (unsigned long)deleted,
                    (unsigned long)fail,
                    (unsigned long)pages,
                    (unsigned long)shard_full)));
            ereport(LOG,
                (errmsg(
                    "epoch tip flush prof[%d]: tips=%lu tick_ms=%.1f pick=%.0f%% find=%.0f%% collect=%.0f%%(evict=%lu miss=%lu) open=%.0f%% inplace=%.0f%%(n=%lu avg=%.0fus) force=%.0f%%(n=%lu avg=%.0fus) del=%.0f%% xact=%.0f%%",
                    worker_id,
                    (unsigned long)d_tips,
                    d_tick / 1000.0,
                    100.0 * d_pick / denom,
                    100.0 * d_find / denom,
                    100.0 * d_collect / denom,
                    (unsigned long)d_evict,
                    (unsigned long)d_miss,
                    100.0 * d_open / denom,
                    100.0 * d_inplace / denom,
                    (unsigned long)d_itry,
                    d_itry > 0 ? (double)d_inplace / (double)d_itry : 0.0,
                    100.0 * d_force / denom,
                    (unsigned long)d_ftry,
                    d_ftry > 0 ? (double)d_force / (double)d_ftry : 0.0,
                    100.0 * d_delete / denom,
                    100.0 * d_xact / denom)));
            /* DirtyTouch writer-path cost — compare 33 vs 50 terminals. */
            ereport(LOG,
                (errmsg(
                    "epoch tip dirty[%d]: touch_full=%lu evict=%lu evict_ms=%.1f (avg=%.0fus) touch_fail=%lu",
                    worker_id,
                    (unsigned long)d_touch_full,
                    (unsigned long)d_evict,
                    d_evict_us / 1000.0,
                    d_evict > 0 ? (double)d_evict_us / (double)d_evict : 0.0,
                    (unsigned long)d_touch_fail)));
            last_prof = prof;
            last_report_us = now_us;
        }

        long sleep_us = EPOCH_TIP_FLUSH_IDLE_LOW_US;
        if (urgent || used >= critical) {
            sleep_us = (flushed > 0) ? 0L : 100L;
        } else if (used >= high) {
            sleep_us = EPOCH_TIP_FLUSH_IDLE_HIGH_US;
        } else if (flushed > 0) {
            sleep_us = EPOCH_TIP_FLUSH_IDLE_HIGH_US;
        }
        if (sleep_us > 0) {
            pg_usleep(sleep_us);
        }
    }
}

static void EpochTipFlushWorkerQuickDie(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
    on_exit_reset();
    _exit(2);
}

static void EpochTipFlushWorkerSigHup(SIGNAL_ARGS)
{
    t_thrd.epoch_worker_cxt.got_SIGHUP = true;
}

static void EpochTipFlushWorkerShutdown(SIGNAL_ARGS)
{
    t_thrd.epoch_worker_cxt.shutdown_requested = true;
}
