//
// OCC 全局统计计数器（多 backend 线程共享，进程内累加）。
//

#include "occ/occ_stats.h"

#include "utils/elog.h"

#include <atomic>

bool g_occ_enable_stats_log = false;

static std::atomic<uint64_t> g_occStatCommits{0};
static std::atomic<uint64_t> g_occStatAbortPre{0};
static std::atomic<uint64_t> g_occStatAbortLockVer{0};
static std::atomic<uint64_t> g_occStatAbortLockTo{0};

static std::atomic<uint64_t> g_occStatPreFailRelation{0};
static std::atomic<uint64_t> g_occStatPreFailCsn{0};
static std::atomic<uint64_t> g_occStatPreFailNotLatest{0};
static std::atomic<uint64_t> g_occStatPreFailInsert{0};
static std::atomic<uint64_t> g_occStatPreFailUnknown{0};

static constexpr uint64_t OCC_STATS_LOG_INTERVAL = 1024;

void O_OccStatsRecordCommit()
{
    g_occStatCommits.fetch_add(1, std::memory_order_relaxed);
}

void O_OccStatsLogSummary()
{
    ereport(LOG,
            (errmsg("OCC_STATS commits=%llu abort_pre_validation=%llu "
                    "abort_lock_version=%llu abort_lock_timeout=%llu "
                    "pre_fail_relation=%llu pre_fail_csn=%llu "
                    "pre_fail_not_latest=%llu pre_fail_insert=%llu pre_fail_unknown=%llu",
                    (unsigned long long)g_occStatCommits.load(std::memory_order_relaxed),
                    (unsigned long long)g_occStatAbortPre.load(std::memory_order_relaxed),
                    (unsigned long long)g_occStatAbortLockVer.load(std::memory_order_relaxed),
                    (unsigned long long)g_occStatAbortLockTo.load(std::memory_order_relaxed),
                    (unsigned long long)g_occStatPreFailRelation.load(std::memory_order_relaxed),
                    (unsigned long long)g_occStatPreFailCsn.load(std::memory_order_relaxed),
                    (unsigned long long)g_occStatPreFailNotLatest.load(std::memory_order_relaxed),
                    (unsigned long long)g_occStatPreFailInsert.load(std::memory_order_relaxed),
                    (unsigned long long)g_occStatPreFailUnknown.load(std::memory_order_relaxed))));
}

void O_OccStatsRecordPreAbortDetail(OccPreAbortFailReason reason)
{
    switch (reason) {
        case OCC_PRE_FAIL_RELATION:
            g_occStatPreFailRelation.fetch_add(1, std::memory_order_relaxed);
            break;
        case OCC_PRE_FAIL_CSN:
            g_occStatPreFailCsn.fetch_add(1, std::memory_order_relaxed);
            break;
        case OCC_PRE_FAIL_NOT_LATEST:
            g_occStatPreFailNotLatest.fetch_add(1, std::memory_order_relaxed);
            break;
        case OCC_PRE_FAIL_INSERT:
            g_occStatPreFailInsert.fetch_add(1, std::memory_order_relaxed);
            break;
        default:
            g_occStatPreFailUnknown.fetch_add(1, std::memory_order_relaxed);
            break;
    }
}

void O_OccStatsRecordAbort(OccAbortReason reason)
{
    uint64_t totalAborts = 0;

    switch (reason) {
        case OCC_ABORT_PRE_VALIDATION:
            totalAborts = g_occStatAbortPre.fetch_add(1, std::memory_order_relaxed) + 1;
            break;
        case OCC_ABORT_LOCK_VERSION:
            totalAborts = g_occStatAbortLockVer.fetch_add(1, std::memory_order_relaxed) + 1;
            break;
        case OCC_ABORT_LOCK_TIMEOUT:
            totalAborts = g_occStatAbortLockTo.fetch_add(1, std::memory_order_relaxed) + 1;
            break;
        default:
            break;
    }

    if (g_occ_enable_stats_log) {
        O_OccStatsLogSummary();
    } else if (totalAborts > 0 && (totalAborts % OCC_STATS_LOG_INTERVAL) == 0) {
        O_OccStatsLogSummary();
    }
}
