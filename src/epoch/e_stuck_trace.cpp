#include "postgres.h"

#include "epoch/e_stuck_trace.h"
#include "epoch/e_apply_shmem.h"
#include "epoch/e_config.h"
#include "epoch/e_log_path.h"
#include "epoch/e_merge_pool.h"
#include "epoch/e_merge_shmem.h"
#include "epoch/e_node_manager.h"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <sys/syscall.h>
#include <sys/time.h>
#include <unistd.h>
#include <vector>

static constexpr uint32_t EST_RING = 128;
static constexpr uint32_t EST_MAX_THREADS = 256;

struct EpochStuckTraceEntry {
    uint64_t us;
    uint32_t ev;
    uint32_t a;
    uint64_t b;
};

struct EpochStuckTraceSlot {
    std::atomic<uint32_t> used;
    std::atomic<uint32_t> head;
    pid_t tid;
    char name[24];
    EpochStuckTraceEntry ring[EST_RING];
};

static EpochStuckTraceSlot g_stuck_slots[EST_MAX_THREADS];
static std::atomic<uint32_t> g_stuck_slot_count{0};
static std::atomic<uint64_t> g_stuck_last_dump_us{0};
static std::atomic<uint64_t> g_stuck_last_logical{0};
static std::atomic<uint64_t> g_stuck_logical_change_us{0};
static thread_local int g_stuck_slot = -1;

static const char* const g_est_names[] = {
    "NONE",
    "MERGE_CLAIM",
    "MERGE_DESERIALIZE",
    "MERGE_DESERIALIZE_FAIL",
    "MERGE_CRDT_ENTER",
    "CRDT_ENSURE_SLOT",
    "CRDT_COLLECT_SHARDS",
    "CRDT_ACQUIRE_BEGIN",
    "CRDT_ACQUIRE_SHARD",
    "CRDT_ACQUIRE_DONE",
    "CRDT_PREVIEW_ROW",
    "CRDT_MERGE_ROW",
    "CRDT_RELEASE_BEGIN",
    "CRDT_RELEASE_DONE",
    "MERGE_CRDT_DONE",
    "MERGE_FINISH",
    "MERGE_DONE_HOOK_ENTER",
    "MERGE_DONE_HOOK_ENQUEUE",
    "MERGE_DONE_HOOK_EXIT",
    "MERGE_TXN_DONE",
    "SUBMIT_ENTER",
    "SUBMIT_OK",
    "SUBMIT_FAIL_REBIND",
    "SUBMIT_FAIL_CELLS_FULL",
    "SUBMIT_RETRY",
    "RECV_TXN",
    "RECV_EPOCH_END",
    "DRAIN_LOCAL_EPOCH",
    "SEND_EPOCH_END",
    "MERGE_CURSOR_ADVANCE",
    "MERGE_CURSOR_FORCE_ADVANCE",
    "LOGICAL_STALL",
    "APPLY_CLAIM",
    "APPLY_COMMIT",
    "APPLY_DONE",
};

static_assert(sizeof(g_est_names) / sizeof(g_est_names[0]) == static_cast<size_t>(EST_EV_COUNT),
    "EpochStuckTrace event name table out of sync with enum");

static const char* EpochStuckTraceEventName(uint32_t ev)
{
    if (ev >= static_cast<uint32_t>(EST_EV_COUNT)) {
        return "?";
    }
    return g_est_names[ev];
}

static uint64_t EpochStuckTraceWallUs(void)
{
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000ULL + static_cast<uint64_t>(tv.tv_usec);
}

void EpochStuckTraceRegister(const char* thread_name)
{
    if (g_stuck_slot >= 0) {
        return;
    }
    uint32_t idx = g_stuck_slot_count.fetch_add(1, std::memory_order_acq_rel);
    if (idx >= EST_MAX_THREADS) {
        g_stuck_slot = -1;
        return;
    }
    EpochStuckTraceSlot& s = g_stuck_slots[idx];
    s.tid = static_cast<pid_t>(syscall(SYS_gettid));
    s.head.store(0, std::memory_order_relaxed);
    s.name[0] = '\0';
    if (thread_name != nullptr) {
        (void)snprintf(s.name, sizeof(s.name), "%s", thread_name);
    }
    s.used.store(1, std::memory_order_release);
    g_stuck_slot = static_cast<int>(idx);
}

void EpochStuckTraceNote(uint32_t event, uint32_t a, uint64_t b)
{
    if (g_stuck_slot < 0) {
        EpochStuckTraceRegister("auto");
        if (g_stuck_slot < 0) {
            return;
        }
    }
    EpochStuckTraceSlot& s = g_stuck_slots[g_stuck_slot];
    uint32_t idx = s.head.fetch_add(1, std::memory_order_acq_rel);
    EpochStuckTraceEntry& e = s.ring[idx % EST_RING];
    e.us = EpochStuckTraceWallUs();
    e.a = a;
    e.b = b;
    e.ev = event;
}

struct EpochStuckTraceFlat {
    uint64_t us;
    uint32_t slot;
    uint32_t ev;
    uint32_t a;
    uint64_t b;
};

static bool EpochStuckTraceCmp(const EpochStuckTraceFlat& l, const EpochStuckTraceFlat& r)
{
    return l.us < r.us;
}

void EpochStuckTraceDump(const char* reason, bool force)
{
    uint64_t now = EpochStuckTraceWallUs();
    uint64_t last = g_stuck_last_dump_us.load(std::memory_order_relaxed);
    if (!force && now > last && now - last < 3000000ULL) {
        return;
    }
    uint64_t expect = last;
    if (!g_stuck_last_dump_us.compare_exchange_strong(expect, now, std::memory_order_acq_rel)) {
        if (!force) {
            return;
        }
    }

    char path[MAXPGPATH];
    if (!EpochFormatLogPath(path, sizeof(path), "epoch_stuck_trace")) {
        return;
    }
    FILE* fp = fopen(path, "a");
    if (fp == nullptr) {
        return;
    }

    time_t t = time(nullptr);
    struct tm tm_buf;
    (void)localtime_r(&t, &tm_buf);
    char ts[32];
    (void)strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_buf);

    const uint32_t slots = g_stuck_slot_count.load(std::memory_order_acquire);
    fprintf(fp,
        "[%s] STUCK_TRACE reason=%s node=%u pid=%ld threads=%u logical=%lu physical=%lu merge_epoch=%lu "
        "merge_inflight=%u apply_inflight=%u rej_rebind=%lu cells_full=%lu cleared_ready=%lu\n",
        ts,
        (reason != nullptr) ? reason : "-",
        EpochGetNodeId(),
        static_cast<long>(getpid()),
        (slots > EST_MAX_THREADS) ? EST_MAX_THREADS : slots,
        static_cast<unsigned long>(E_NodeManager::GetLogicalEpoch()),
        static_cast<unsigned long>(E_NodeManager::GetPhysicalEpoch()),
        static_cast<unsigned long>(GetMergeEpoch()),
        0,
        0,
        static_cast<unsigned long>(0),
        static_cast<unsigned long>(0),
        static_cast<unsigned long>(0));

    /* merge 行状态：merge_epoch / merge_epoch+1 / logical 三行 */
    {
        uint64_t me = GetMergeEpoch();
        uint64_t logical = E_NodeManager::GetLogicalEpoch();
        uint64_t probes[3] = {me, me + 1, logical};
        for (int i = 0; i < 3; ++i) {
            if (probes[i] == 0) {
                continue;
            }
            char detail[1024];
            EpochMergeQueueDescribeEpoch(probes[i], detail, sizeof(detail));
            fprintf(fp, "  MERGE_ROW %s\n", detail);
        }
    }

    std::vector<EpochStuckTraceFlat> flat;
    flat.reserve(2048);
    for (uint32_t i = 0; i < slots && i < EST_MAX_THREADS; ++i) {
        EpochStuckTraceSlot& s = g_stuck_slots[i];
        if (s.used.load(std::memory_order_acquire) == 0) {
            continue;
        }
        uint32_t head = s.head.load(std::memory_order_acquire);
        uint32_t n = (head < EST_RING) ? head : EST_RING;
        for (uint32_t k = 0; k < n; ++k) {
            const EpochStuckTraceEntry& e = s.ring[(head - n + k) % EST_RING];
            EpochStuckTraceFlat f;
            f.us = e.us;
            f.slot = i;
            f.ev = e.ev;
            f.a = e.a;
            f.b = e.b;
            flat.push_back(f);
        }
    }
    std::sort(flat.begin(), flat.end(), EpochStuckTraceCmp);

    uint32_t current = UINT32_MAX;
    for (const EpochStuckTraceFlat& f : flat) {
        if (f.slot != current) {
            current = f.slot;
            EpochStuckTraceSlot& s = g_stuck_slots[current];
            fprintf(fp, "  --- thread[%u] %s tid=%ld ---\n", current, s.name, static_cast<long>(s.tid));
        }
        int64_t delta_us = (f.us <= now) ? -static_cast<int64_t>(now - f.us) : 0;
        fprintf(fp,
            "  %+.3fs %-26s a=%u b=%lu\n",
            static_cast<double>(delta_us) / 1000000.0,
            EpochStuckTraceEventName(f.ev),
            f.a,
            static_cast<unsigned long>(f.b));
    }
    fprintf(fp, "\n");
    (void)fclose(fp);
    fflush(stderr);
}

void EpochStuckTraceLogicalWatchdog(uint64_t logical_epoch)
{
    uint64_t now = EpochStuckTraceWallUs();
    uint64_t prev = g_stuck_last_logical.load(std::memory_order_relaxed);
    if (prev != logical_epoch || g_stuck_logical_change_us.load(std::memory_order_relaxed) == 0) {
        g_stuck_last_logical.store(logical_epoch, std::memory_order_relaxed);
        g_stuck_logical_change_us.store(now, std::memory_order_relaxed);
        return;
    }
    uint64_t since = g_stuck_logical_change_us.load(std::memory_order_relaxed);
    if (now < since || now - since < 3000000ULL) {
        return;
    }
    /* logical 3s 未推进：只在新的一段时间块里 dump（由 dump 内部 3s 限流） */
    char reason[96];
    (void)snprintf(reason, sizeof(reason), "logical_stall epoch=%lu stuck_ms=%lu",
        static_cast<unsigned long>(logical_epoch),
        static_cast<unsigned long>((now - since) / 1000ULL));
    EpochStuckTraceDump(reason, false);
    /* 推进时间戳，避免每跳都尝试 */
    g_stuck_logical_change_us.store(now, std::memory_order_relaxed);
}
