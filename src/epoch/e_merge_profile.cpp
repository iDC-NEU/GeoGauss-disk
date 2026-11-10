#include "postgres.h"

#include "epoch/e_merge_profile.h"
#include "epoch/e_config.h"
#include "epoch/e_log_path.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_stuck_trace.h"
#include "epoch/e_types.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <sys/time.h>

static const char* EpochMergeProfilePhaseName(EpochMergeProfilePhase phase)
{
    switch (phase) {
        case EMP_DESERIALIZE:
            return "deserialize";
        case EMP_PREPARE_ROWS:
            return "prepare_rows";
        case EMP_PK_HEAP_OPEN:
            return "pk_heap_open";
        case EMP_PK_TUPLE_DECODE:
            return "pk_tuple_decode";
        case EMP_PK_INDEX_ATTRS:
            return "pk_index_attrs";
        case EMP_ENSURE_SLOT:
            return "ensure_slot";
        case EMP_COLLECT_SHARDS:
            return "collect_shards";
        case EMP_SHARD_ACQUIRE:
            return "shard_acquire";
        case EMP_PREVIEW:
            return "preview";
        case EMP_MERGE_ROWS:
            return "merge_rows";
        case EMP_SHARD_RELEASE:
            return "shard_release";
        case EMP_POST_HOOKS:
            return "post_hooks";
        case EMP_QUEUE_WAIT:
            return "queue_wait";
        case EMP_QUEUE_FINISH:
            return "queue_finish";
        default:
            return "?";
    }
}

struct EpochMergeProfileSample {
    uint64_t us[EMP_PHASE_COUNT];
    uint64_t row_count;
    uint64_t shard_count;
};

static thread_local bool g_merge_profile_active = false;
static thread_local EpochMergeProfileSample g_merge_profile_sample;

static std::atomic<uint64_t> g_merge_profile_agg_txn_count{0};
static std::atomic<uint64_t> g_merge_profile_agg_row_count{0};
static std::atomic<uint64_t> g_merge_profile_agg_shard_count{0};
static std::atomic<uint64_t> g_merge_profile_agg_us[EMP_PHASE_COUNT];
static constexpr uint64_t EPOCH_MERGE_PROFILE_AGG_INTERVAL = 50;

bool g_epoch_merge_profile = false;

static bool EpochMergeProfileEnabled(void)
{
    return g_epoch_merge_profile;
}

static void EpochMergeProfileAppendLine(const char* line)
{
    if (line == nullptr) {
        return;
    }
    char path[MAXPGPATH];
    if (!EpochFormatLogPath(path, sizeof(path), "epoch_merge_profile")) {
        return;
    }

    FILE* fp = fopen(path, "a");
    if (fp == nullptr) {
        return;
    }

    time_t now = time(nullptr);
    struct tm tm_buf;
    (void)localtime_r(&now, &tm_buf);
    char ts[32];
    (void)strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_buf);
    fprintf(fp, "[%s] node=%u %s\n", ts, EpochGetNodeId(), line);
    (void)fclose(fp);
}

static void EpochMergeProfileMaybeEmitAggregate(uint64_t txn_count)
{
    if (txn_count == 0 || txn_count % EPOCH_MERGE_PROFILE_AGG_INTERVAL != 0) {
        return;
    }

    uint64_t row_count = g_merge_profile_agg_row_count.load(std::memory_order_relaxed);
    uint64_t shard_count = g_merge_profile_agg_shard_count.load(std::memory_order_relaxed);
    uint64_t phase_us[EMP_PHASE_COUNT];
    uint64_t total_us = 0;
    for (int i = 0; i < EMP_PHASE_COUNT; ++i) {
        phase_us[i] = g_merge_profile_agg_us[i].load(std::memory_order_relaxed);
        total_us += phase_us[i];
    }
    if (total_us == 0) {
        return;
    }

    char buf[1536];
    int offset = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "MERGE_PROFILE_AGG txns=%lu rows=%lu shards=%lu total_us=%lu",
        txn_count,
        row_count,
        shard_count,
        total_us);
    securec_check_ss(offset, "\0", "\0");

    for (int i = 0; i < EMP_PHASE_COUNT; ++i) {
        if (phase_us[i] == 0) {
            continue;
        }
        uint64_t pct = (phase_us[i] * 100ULL) / total_us;
        int rc = snprintf_s(buf + offset,
            sizeof(buf) - static_cast<size_t>(offset),
            sizeof(buf) - static_cast<size_t>(offset) - 1,
            " %s_us=%lu(%lu%%)",
            EpochMergeProfilePhaseName(static_cast<EpochMergeProfilePhase>(i)),
            phase_us[i],
            pct);
        securec_check_ss(rc, "\0", "\0");
        offset += rc;
        if (offset >= static_cast<int>(sizeof(buf)) - 1) {
            break;
        }
    }
    EpochMergeProfileAppendLine(buf);
}

void EpochMergeProfileBeginWireTxn(void)
{
    if (!EpochMergeProfileEnabled()) {
        g_merge_profile_active = false;
        return;
    }
    g_merge_profile_active = true;
    memset(&g_merge_profile_sample, 0, sizeof(g_merge_profile_sample));
}

void EpochMergeProfileAddUs(EpochMergeProfilePhase phase, uint64_t us)
{
    if (!g_merge_profile_active || phase < 0 || phase >= EMP_PHASE_COUNT) {
        return;
    }
    g_merge_profile_sample.us[phase] += us;
}

void EpochMergeProfileSetRowCount(uint64_t rows)
{
    if (!g_merge_profile_active) {
        return;
    }
    g_merge_profile_sample.row_count = rows;
}

void EpochMergeProfileSetShardCount(uint64_t shards)
{
    if (!g_merge_profile_active) {
        return;
    }
    g_merge_profile_sample.shard_count = shards;
}

void EpochMergeProfileEndWireTxn(uint64_t epoch, uint64_t csn, uint32_t server_id, bool ok)
{
    if (!g_merge_profile_active) {
        return;
    }
    g_merge_profile_active = false;

    uint64_t total_us = 0;
    for (int i = 0; i < EMP_PHASE_COUNT; ++i) {
        total_us += g_merge_profile_sample.us[i];
    }

    char buf[1536];
    int offset = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "MERGE_PROFILE_TXN epoch=%lu csn=%lu sid=%u ok=%d rows=%lu shards=%lu total_us=%lu",
        epoch,
        csn,
        server_id,
        ok ? 1 : 0,
        g_merge_profile_sample.row_count,
        g_merge_profile_sample.shard_count,
        total_us);
    securec_check_ss(offset, "\0", "\0");

    for (int i = 0; i < EMP_PHASE_COUNT; ++i) {
        uint64_t phase_us = g_merge_profile_sample.us[i];
        if (phase_us == 0) {
            continue;
        }
        int rc = snprintf_s(buf + offset,
            sizeof(buf) - static_cast<size_t>(offset),
            sizeof(buf) - static_cast<size_t>(offset) - 1,
            " %s_us=%lu",
            EpochMergeProfilePhaseName(static_cast<EpochMergeProfilePhase>(i)),
            phase_us);
        securec_check_ss(rc, "\0", "\0");
        offset += rc;
        if (offset >= static_cast<int>(sizeof(buf)) - 1) {
            break;
        }
    }
    EpochMergeProfileAppendLine(buf);

    uint64_t txn_count = g_merge_profile_agg_txn_count.fetch_add(1, std::memory_order_relaxed) + 1;
    g_merge_profile_agg_row_count.fetch_add(g_merge_profile_sample.row_count, std::memory_order_relaxed);
    g_merge_profile_agg_shard_count.fetch_add(g_merge_profile_sample.shard_count, std::memory_order_relaxed);
    for (int i = 0; i < EMP_PHASE_COUNT; ++i) {
        g_merge_profile_agg_us[i].fetch_add(g_merge_profile_sample.us[i], std::memory_order_relaxed);
    }
    EpochMergeProfileMaybeEmitAggregate(txn_count);
}

/*
 * ---------------- 卡死定位：merge worker 当前 task / 阶段 ----------------
 * 所有 epoch 线程同进程，用进程内原子量即可；不强求 profile 开关。
 */
struct EpochMergeWorkerProgress {
    std::atomic<int> active;
    std::atomic<int> phase;
    std::atomic<uint64_t> epoch;
    std::atomic<uint64_t> wire_key;
    std::atomic<uint64_t> task_start_us;
    std::atomic<uint64_t> phase_start_us;
};

static EpochMergeWorkerProgress g_merge_progress[EPOCH_MAX_MERGE_WORKERS];
static thread_local int g_merge_worker_id = -1;
static std::atomic<uint64_t> g_merge_stuck_last_report_us{0};

static uint64_t EpochMergeProfileWallUs(void)
{
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000ULL + static_cast<uint64_t>(tv.tv_usec);
}

void EpochMergeProfileSetWorker(int worker_id)
{
    if (worker_id < 0 || worker_id >= static_cast<int>(EPOCH_MAX_MERGE_WORKERS)) {
        return;
    }
    g_merge_worker_id = worker_id;
    g_merge_progress[worker_id].phase.store(-1, std::memory_order_relaxed);
    g_merge_progress[worker_id].active.store(0, std::memory_order_relaxed);
}

void EpochMergeProfileNoteTask(uint64_t epoch, uint64_t wire_key)
{
    if (g_merge_worker_id < 0) {
        return;
    }
    EpochMergeWorkerProgress& p = g_merge_progress[g_merge_worker_id];
    uint64_t now = EpochMergeProfileWallUs();
    p.epoch.store(epoch, std::memory_order_relaxed);
    p.wire_key.store(wire_key, std::memory_order_relaxed);
    p.task_start_us.store(now, std::memory_order_relaxed);
    p.phase_start_us.store(now, std::memory_order_relaxed);
    p.phase.store(static_cast<int>(EMP_DESERIALIZE), std::memory_order_relaxed);
    p.active.store(1, std::memory_order_release);
}

void EpochMergeProfileNotePhase(EpochMergeProfilePhase phase)
{
    if (g_merge_worker_id < 0) {
        return;
    }
    EpochMergeWorkerProgress& p = g_merge_progress[g_merge_worker_id];
    p.phase_start_us.store(EpochMergeProfileWallUs(), std::memory_order_relaxed);
    p.phase.store(static_cast<int>(phase), std::memory_order_release);
}

void EpochMergeProfileNoteTxnDone(void)
{
    if (g_merge_worker_id < 0) {
        return;
    }
    EpochMergeWorkerProgress& p = g_merge_progress[g_merge_worker_id];
    p.phase.store(-1, std::memory_order_relaxed);
    p.active.store(0, std::memory_order_release);
}

void EpochMergeProfileMaybeReportStuck(void)
{
    uint64_t now = EpochMergeProfileWallUs();
    uint64_t last = g_merge_stuck_last_report_us.load(std::memory_order_relaxed);
    if (now < last || now - last < 5000000ULL) {
        return;
    }
    if (!g_merge_stuck_last_report_us.compare_exchange_strong(last, now, std::memory_order_relaxed)) {
        return;
    }

    bool any_stuck = false;
    for (uint32_t w = 0; w < EPOCH_MAX_MERGE_WORKERS; ++w) {
        EpochMergeWorkerProgress& p = g_merge_progress[w];
        if (p.active.load(std::memory_order_acquire) == 0) {
            continue;
        }
        int phase = p.phase.load(std::memory_order_relaxed);
        uint64_t task_start = p.task_start_us.load(std::memory_order_relaxed);
        uint64_t phase_start = p.phase_start_us.load(std::memory_order_relaxed);
        uint64_t task_ms = (now > task_start) ? (now - task_start) / 1000ULL : 0;
        if (task_ms < 2000ULL) {
            continue;
        }
        uint64_t phase_ms = (now > phase_start) ? (now - phase_start) / 1000ULL : 0;
        const char* phase_name = (phase >= 0 && phase < EMP_PHASE_COUNT)
            ? EpochMergeProfilePhaseName(static_cast<EpochMergeProfilePhase>(phase))
            : "idle";
        fprintf(stderr,
            "MERGE_STUCK node=%u wid=%u epoch=%lu wire_key=%lu phase=%s task_ms=%lu phase_ms=%lu\n",
            EpochGetNodeId(),
            w,
            static_cast<unsigned long>(p.epoch.load(std::memory_order_relaxed)),
            static_cast<unsigned long>(p.wire_key.load(std::memory_order_relaxed)),
            phase_name,
            static_cast<unsigned long>(task_ms),
            static_cast<unsigned long>(phase_ms));
        fflush(stderr);
        any_stuck = true;
    }
    if (any_stuck) {
        /* 一次落盘全线程飞行记录，等价「逐行打印」现场 */
        EpochStuckTraceDump("merge_task_stuck", false);
    }
}
