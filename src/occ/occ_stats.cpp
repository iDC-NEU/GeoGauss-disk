//
// OCC 全局统计计数器（多 backend 线程共享，进程内累加）。
//

#include "occ/occ_stats.h"

#include "utils/elog.h"

#include <atomic>

static std::atomic<uint64_t> g_occStatCommits{0};
static std::atomic<uint64_t> g_occStatAbortPre{0};
static std::atomic<uint64_t> g_occStatAbortLockVer{0};
static std::atomic<uint64_t> g_occStatAbortLockTo{0};

void O_OccStatsRecordCommit()
{
    g_occStatCommits.fetch_add(1, std::memory_order_relaxed);
}

void O_OccStatsLogSummary()
{
    ereport(LOG,
            (errmsg("OCC_STATS commits=%llu abort_pre_validation=%llu "
                    "abort_lock_version=%llu abort_lock_timeout=%llu",
                    (unsigned long long)g_occStatCommits.load(std::memory_order_relaxed),
                    (unsigned long long)g_occStatAbortPre.load(std::memory_order_relaxed),
                    (unsigned long long)g_occStatAbortLockVer.load(std::memory_order_relaxed),
                    (unsigned long long)g_occStatAbortLockTo.load(std::memory_order_relaxed))));
}

void O_OccStatsRecordAbort(OccAbortReason reason)
{
    switch (reason) {
        case OCC_ABORT_PRE_VALIDATION:
            g_occStatAbortPre.fetch_add(1, std::memory_order_relaxed);
            break;
        case OCC_ABORT_LOCK_VERSION:
            g_occStatAbortLockVer.fetch_add(1, std::memory_order_relaxed);
            break;
        case OCC_ABORT_LOCK_TIMEOUT:
            g_occStatAbortLockTo.fetch_add(1, std::memory_order_relaxed);
            break;
        default:
            break;
    }
    O_OccStatsLogSummary();
}
