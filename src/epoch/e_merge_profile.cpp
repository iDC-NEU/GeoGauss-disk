#include "postgres.h"

#include "epoch/e_merge_profile.h"
#include "epoch/e_config.h"
#include "epoch/e_log_path.h"
#include "epoch/e_node_manager.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <ctime>

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
