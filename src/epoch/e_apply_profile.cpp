#include "postgres.h"

#include "epoch/e_apply_profile.h"
#include "epoch/e_config.h"
#include "epoch/e_log_path.h"
#include "epoch/e_node_manager.h"

#include <atomic>
#include <cstdio>
#include <ctime>
#include <cstring>

static const char* EpochApplyProfilePhaseName(EpochApplyProfilePhase phase)
{
    switch (phase) {
        case EAP_QUEUE_WAIT:
            return "queue_wait";
        case EAP_DESERIALIZE:
            return "deserialize";
        case EAP_SORT:
            return "sort";
        case EAP_TXN_BEGIN:
            return "txn_begin";
        case EAP_TXN_COMMIT:
            return "txn_commit";
        case EAP_HEAP_OPEN:
            return "heap_open";
        case EAP_TUPLE_DECODE:
            return "tuple_decode";
        case EAP_ESTATE_NEW:
            return "estate_new";
        case EAP_FIND_TUPLE:
            return "find_tuple";
        case EAP_FIND_INDEX_SCAN:
            return "find_index_scan";
        case EAP_FIND_CHAIN_WALK:
            return "find_chain_walk";
        case EAP_HEAP_INSERT:
            return "heap_insert";
        case EAP_HEAP_UPDATE:
            return "heap_update";
        case EAP_HEAP_DELETE:
            return "heap_delete";
        case EAP_CLEANUP:
            return "cleanup";
        default:
            return "?";
    }
}

struct EpochApplyProfileSample {
    uint64_t us[EAP_PHASE_COUNT];
    uint64_t row_count;
    int max_chain_depth;
    Oid max_chain_depth_relid;
};

static thread_local bool g_apply_profile_active = false;
static thread_local EpochApplyProfileSample g_apply_profile_sample;

/* HCJ ADDBY: apply profile 聚合计数改用 atomic，避免 apply worker 争用 mutex */
static std::atomic<uint64_t> g_apply_profile_agg_txn_count{0};
static std::atomic<uint64_t> g_apply_profile_agg_row_count{0};
static std::atomic<uint64_t> g_apply_profile_agg_us[EAP_PHASE_COUNT];
static std::atomic<uint64_t> g_apply_abort_local{0};
static std::atomic<uint64_t> g_apply_abort_remote{0};
/* INSERT 四岔路 × local/remote */
static std::atomic<uint64_t> g_apply_insert_path_local[EAP_INSERT_PATH_COUNT];
static std::atomic<uint64_t> g_apply_insert_path_remote[EAP_INSERT_PATH_COUNT];
static constexpr uint64_t EPOCH_APPLY_PROFILE_AGG_INTERVAL = 50;

bool g_epoch_apply_profile = false;

static const char* EpochApplyInsertPathName(EpochApplyInsertPath path)
{
    switch (path) {
        case EAP_INSERT_STAGING_HIT:
            return "insert_staging_hit";
        case EAP_INSERT_FIND_HIT:
            return "insert_find_hit";
        case EAP_INSERT_DO:
            return "insert_do";
        case EAP_INSERT_UNIQUE_CATCH:
            return "insert_unique_catch";
        default:
            return "insert_unknown";
    }
}

static bool EpochApplyProfileEnabled(void)
{
    return g_epoch_apply_profile && !g_epoch_skip_apply;
}

static void EpochApplyProfileAppendLine(const char* line)
{
    if (line == nullptr) {
        return;
    }
    char path[MAXPGPATH];
    if (!EpochFormatLogPath(path, sizeof(path), "epoch_apply_profile")) {
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

static void EpochApplyProfileMaybeEmitAggregate(uint64_t txn_count)
{
    if (txn_count == 0 || txn_count % EPOCH_APPLY_PROFILE_AGG_INTERVAL != 0) {
        return;
    }

    uint64_t row_count = g_apply_profile_agg_row_count.load(std::memory_order_relaxed);
    uint64_t phase_us[EAP_PHASE_COUNT];
    uint64_t total_us = 0;
    for (int i = 0; i < EAP_PHASE_COUNT; ++i) {
        phase_us[i] = g_apply_profile_agg_us[i].load(std::memory_order_relaxed);
        total_us += phase_us[i];
    }
    if (total_us == 0) {
        return;
    }

    const uint64_t abort_local = g_apply_abort_local.load(std::memory_order_relaxed);
    const uint64_t abort_remote = g_apply_abort_remote.load(std::memory_order_relaxed);
    uint64_t ins_local[EAP_INSERT_PATH_COUNT];
    uint64_t ins_remote[EAP_INSERT_PATH_COUNT];
    uint64_t pk_conflict_local = 0;
    uint64_t pk_conflict_remote = 0;
    for (int i = 0; i < EAP_INSERT_PATH_COUNT; ++i) {
        ins_local[i] = g_apply_insert_path_local[i].load(std::memory_order_relaxed);
        ins_remote[i] = g_apply_insert_path_remote[i].load(std::memory_order_relaxed);
        if (i != EAP_INSERT_DO) {
            pk_conflict_local += ins_local[i];
            pk_conflict_remote += ins_remote[i];
        }
    }

    char buf[1536];
    int offset = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "APPLY_PROFILE_AGG txns=%lu rows=%lu total_us=%lu apply_abort_local=%lu apply_abort_remote=%lu "
        "insert_pk_conflict_local=%lu insert_pk_conflict_remote=%lu "
        "insert_staging_hit_local=%lu insert_staging_hit_remote=%lu "
        "insert_find_hit_local=%lu insert_find_hit_remote=%lu "
        "insert_do_local=%lu insert_do_remote=%lu "
        "insert_unique_catch_local=%lu insert_unique_catch_remote=%lu",
        txn_count,
        row_count,
        total_us,
        abort_local,
        abort_remote,
        pk_conflict_local,
        pk_conflict_remote,
        ins_local[EAP_INSERT_STAGING_HIT],
        ins_remote[EAP_INSERT_STAGING_HIT],
        ins_local[EAP_INSERT_FIND_HIT],
        ins_remote[EAP_INSERT_FIND_HIT],
        ins_local[EAP_INSERT_DO],
        ins_remote[EAP_INSERT_DO],
        ins_local[EAP_INSERT_UNIQUE_CATCH],
        ins_remote[EAP_INSERT_UNIQUE_CATCH]);
    securec_check_ss(offset, "\0", "\0");

    for (int i = 0; i < EAP_PHASE_COUNT; ++i) {
        if (phase_us[i] == 0) {
            continue;
        }
        uint64_t pct = (phase_us[i] * 100ULL) / total_us;
        int rc = snprintf_s(buf + offset,
            sizeof(buf) - static_cast<size_t>(offset),
            sizeof(buf) - static_cast<size_t>(offset) - 1,
            " %s_us=%lu(%lu%%)",
            EpochApplyProfilePhaseName(static_cast<EpochApplyProfilePhase>(i)),
            phase_us[i],
            pct);
        securec_check_ss(rc, "\0", "\0");
        offset += rc;
        if (offset >= static_cast<int>(sizeof(buf)) - 1) {
            break;
        }
    }
    EpochApplyProfileAppendLine(buf);
}

void EpochApplyProfileNoteApplyAbort(bool is_local, Oid relid, EpochOpType op)
{
    const uint64_t n = is_local ? (g_apply_abort_local.fetch_add(1, std::memory_order_relaxed) + 1)
                                : (g_apply_abort_remote.fetch_add(1, std::memory_order_relaxed) + 1);
    if (EpochApplyProfileEnabled()) {
        char buf[256];
        int rc = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            "APPLY_ABORT is_local=%d rel=%u op=%u local_total=%lu remote_total=%lu",
            is_local ? 1 : 0,
            relid,
            static_cast<unsigned>(op),
            g_apply_abort_local.load(std::memory_order_relaxed),
            g_apply_abort_remote.load(std::memory_order_relaxed));
        securec_check_ss(rc, "\0", "\0");
        EpochApplyProfileAppendLine(buf);
    }
    if (n == 1 || (n % 100) == 0) {
        ereport(LOG,
            (errmsg("epoch apply_abort is_local=%d rel=%u op=%u count=%lu (row skipped, txn treated applied)",
                is_local ? 1 : 0,
                relid,
                static_cast<unsigned>(op),
                n)));
    }
}

void EpochApplyProfileNoteInsertPath(EpochApplyInsertPath path, bool is_local, Oid relid)
{
    if (static_cast<int>(path) < 0 || path >= EAP_INSERT_PATH_COUNT) {
        return;
    }
    std::atomic<uint64_t>* counter =
        is_local ? &g_apply_insert_path_local[path] : &g_apply_insert_path_remote[path];
    const uint64_t n = counter->fetch_add(1, std::memory_order_relaxed) + 1;

    /* 高频 hit 不分条落盘，只在 AGG 看；UNIQUE CATCH / 真插入偶发打 LOG */
    if (path == EAP_INSERT_UNIQUE_CATCH || path == EAP_INSERT_DO) {
        if (n == 1 || (n % 1000) == 0) {
            ereport(LOG,
                (errmsg("epoch apply %s is_local=%d rel=%u count=%lu",
                    EpochApplyInsertPathName(path),
                    is_local ? 1 : 0,
                    relid,
                    n)));
        }
    }
    (void)relid;
}

void EpochApplyProfileNoteInsertPkConflict(bool is_local, Oid relid)
{
    /* 兼容旧调用点：仅表示 heap UNIQUE CATCH */
    EpochApplyProfileNoteInsertPath(EAP_INSERT_UNIQUE_CATCH, is_local, relid);
}

void EpochApplyProfileBeginWireTxn(void)
{
    if (!EpochApplyProfileEnabled()) {
        g_apply_profile_active = false;
        return;
    }
    g_apply_profile_active = true;
    memset(&g_apply_profile_sample, 0, sizeof(g_apply_profile_sample));
}

void EpochApplyProfileAddUs(EpochApplyProfilePhase phase, uint64_t us)
{
    if (!g_apply_profile_active || phase < 0 || phase >= EAP_PHASE_COUNT) {
        return;
    }
    g_apply_profile_sample.us[phase] += us;
}

void EpochApplyProfileSetRowCount(uint64_t rows)
{
    if (!g_apply_profile_active) {
        return;
    }
    g_apply_profile_sample.row_count = rows;
}

void EpochApplyProfileRecordFind(Oid relid, uint64_t index_scan_us, uint64_t chain_walk_us, int chain_depth)
{
    if (!g_apply_profile_active) {
        return;
    }
    if (index_scan_us > 0) {
        EpochApplyProfileAddUs(EAP_FIND_INDEX_SCAN, index_scan_us);
        EpochApplyProfileAddUs(EAP_FIND_TUPLE, index_scan_us);
    }
    if (chain_walk_us > 0) {
        EpochApplyProfileAddUs(EAP_FIND_CHAIN_WALK, chain_walk_us);
        EpochApplyProfileAddUs(EAP_FIND_TUPLE, chain_walk_us);
    }
    if (chain_depth > g_apply_profile_sample.max_chain_depth) {
        g_apply_profile_sample.max_chain_depth = chain_depth;
        g_apply_profile_sample.max_chain_depth_relid = relid;
    }
    if (chain_depth >= EPOCH_APPLY_CHAIN_DEPTH_LOG_THRESHOLD) {
        char buf[256];
        int rc = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            "APPLY_CHAIN_DEPTH rel=%u depth=%d index_scan_us=%lu chain_walk_us=%lu",
            relid,
            chain_depth,
            index_scan_us,
            chain_walk_us);
        securec_check_ss(rc, "\0", "\0");
        EpochApplyProfileAppendLine(buf);
    }
}

void EpochApplyProfileEndWireTxn(uint64_t epoch, uint64_t csn, uint32_t server_id, bool ok)
{
    if (!g_apply_profile_active) {
        return;
    }
    g_apply_profile_active = false;

    uint64_t total_us = 0;
    for (int i = 0; i < EAP_PHASE_COUNT; ++i) {
        total_us += g_apply_profile_sample.us[i];
    }

    char buf[1024];
    int offset = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "APPLY_PROFILE_TXN epoch=%lu csn=%lu sid=%u ok=%d rows=%lu total_us=%lu",
        epoch,
        csn,
        server_id,
        ok ? 1 : 0,
        g_apply_profile_sample.row_count,
        total_us);
    securec_check_ss(offset, "\0", "\0");

    for (int i = 0; i < EAP_PHASE_COUNT; ++i) {
        uint64_t phase_us = g_apply_profile_sample.us[i];
        if (phase_us == 0) {
            continue;
        }
        int rc = snprintf_s(buf + offset,
            sizeof(buf) - static_cast<size_t>(offset),
            sizeof(buf) - static_cast<size_t>(offset) - 1,
            " %s_us=%lu",
            EpochApplyProfilePhaseName(static_cast<EpochApplyProfilePhase>(i)),
            phase_us);
        securec_check_ss(rc, "\0", "\0");
        offset += rc;
        if (offset >= static_cast<int>(sizeof(buf)) - 1) {
            break;
        }
    }
    if (g_apply_profile_sample.max_chain_depth > 0) {
        int rc = snprintf_s(buf + offset,
            sizeof(buf) - static_cast<size_t>(offset),
            sizeof(buf) - static_cast<size_t>(offset) - 1,
            " chain_depth_max=%d chain_depth_rel=%u",
            g_apply_profile_sample.max_chain_depth,
            g_apply_profile_sample.max_chain_depth_relid);
        securec_check_ss(rc, "\0", "\0");
    }
    EpochApplyProfileAppendLine(buf);

    uint64_t txn_count = g_apply_profile_agg_txn_count.fetch_add(1, std::memory_order_relaxed) + 1;
    g_apply_profile_agg_row_count.fetch_add(g_apply_profile_sample.row_count, std::memory_order_relaxed);
    for (int i = 0; i < EAP_PHASE_COUNT; ++i) {
        g_apply_profile_agg_us[i].fetch_add(g_apply_profile_sample.us[i], std::memory_order_relaxed);
    }
    EpochApplyProfileMaybeEmitAggregate(txn_count);
}
