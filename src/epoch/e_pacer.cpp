#include "postgres.h"

#include "miscadmin.h"

#include "epoch/e_pacer.h"

#include "epoch/e_abort_csn.h"
#include "epoch/e_config.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_net.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_perf.h"
#include "epoch/e_recv_ledger.h"
#include "epoch/e_shmem.h"
#include "epoch/e_txn_queue.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_types.h"

#include "knl/knl_instance.h"
#include "knl/knl_variable.h"

#include "neu_concurrency_tools/concurrentqueue.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <deque>
#include <fcntl.h>
#include <mutex>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

/* Pacer：DBR 在发送出队点改期，PA 在 merge 游标处转发。 */

static int64_t PacerMonoUs(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<int64_t>(ts.tv_sec) * 1000000 + static_cast<int64_t>(ts.tv_nsec / 1000);
}

static bool PacerSlowListContains(uint32_t node)
{
    const char* s = g_epoch_dyn_net_slow_nodes;
    if (s == nullptr || s[0] == '\0') {
        return false;
    }
    uint32_t cur = 0;
    bool in_num = false;
    for (const char* p = s;; ++p) {
        if (*p >= '0' && *p <= '9') {
            cur = cur * 10 + static_cast<uint32_t>(*p - '0');
            in_num = true;
            continue;
        }
        if (in_num && cur == node) {
            return true;
        }
        cur = 0;
        in_num = false;
        if (*p == '\0') {
            break;
        }
    }
    return false;
}

bool EpochPacerIsSlowSelf(void)
{
    return PacerSlowListContains(EpochGetNodeId());
}

static uint32_t PacerSlowNodeId(void)
{
    const uint32_t total = EpochGetTotalNodes();
    for (uint32_t p = 0; p < total; ++p) {
        if (PacerSlowListContains(p)) {
            return p;
        }
    }
    return UINT32_MAX;
}

static uint32_t PacerFirstHealthyNode(void)
{
    const uint32_t total = EpochGetTotalNodes();
    for (uint32_t p = 0; p < total; ++p) {
        if (!PacerSlowListContains(p)) {
            return p;
        }
    }
    return UINT32_MAX;
}

bool EpochPacerDbrEnabled(void)
{
    return g_enable_epoch_multimaster && EpochGetTotalNodes() > 1 && g_epoch_dyn_net_catchup &&
           EpochPacerIsSlowSelf();
}

bool EpochPacerPaEnabled(void)
{
    /* 至少一个慢节点，且至少一个可用的健康节点充当 proxy */
    return g_enable_epoch_multimaster && EpochGetTotalNodes() > 1 && g_epoch_dyn_net_proxy &&
           PacerSlowNodeId() != UINT32_MAX && PacerFirstHealthyNode() != UINT32_MAX;
}

/* 只发给健康节点：决策请求只需要到达 proxy。 */
static bool PacerPublishToHealthy(const std::string& wire)
{
    bool ok = true;
    const uint32_t self = EpochGetNodeId();
    const uint32_t total = EpochGetTotalNodes();
    for (uint32_t p = 0; p < total; ++p) {
        if (p == self || PacerSlowListContains(p)) {
            continue;
        }
        if (!EpochNetPublishTo(p, wire)) {
            ok = false;
        }
    }
    return ok;
}

/* 发给所有对端：路由决策必须让慢节点也收到，否则它会直发给另一个慢节点。 */
static bool PacerPublishToAll(const std::string& wire)
{
    bool ok = true;
    const uint32_t self = EpochGetNodeId();
    const uint32_t total = EpochGetTotalNodes();
    for (uint32_t p = 0; p < total; ++p) {
        if (p == self) {
            continue;
        }
        if (!EpochNetPublishTo(p, wire)) {
            ok = false;
        }
    }
    return ok;
}

/* ---------------- DBR ---------------- */

static std::atomic<int64_t> g_epoch_interval_us{10000};
static std::atomic<int64_t> g_pressure_since_us{0};
static uint64_t g_last_phys_epoch = 0;
static int64_t g_last_phys_change_us = 0;

static int64_t PacerEpochIntervalUs(void)
{
    return g_epoch_interval_us.load(std::memory_order_relaxed);
}

static void PacerSampleEpochInterval(void)
{
    const uint64_t phys = E_NodeManager::GetPhysicalEpoch();
    const int64_t now = PacerMonoUs();
    if (g_last_phys_epoch == 0) {
        g_last_phys_epoch = phys;
        g_last_phys_change_us = now;
        return;
    }
    if (phys == g_last_phys_epoch) {
        return;
    }
    const uint64_t delta = phys > g_last_phys_epoch ? (phys - g_last_phys_epoch) : 1;
    const int64_t dt = now - g_last_phys_change_us;
    if (dt > 0) {
        int64_t per = dt / static_cast<int64_t>(delta);
        if (per < 1000) {
            per = 1000;
        }
        const int64_t cur = g_epoch_interval_us.load(std::memory_order_relaxed);
        g_epoch_interval_us.store((cur * 7 + per) / 8, std::memory_order_relaxed);
    }
    g_last_phys_epoch = phys;
    g_last_phys_change_us = now;
}

static bool PacerSenderPressure(void)
{
    if (EpochNetUplinkCongested()) {
        return true;
    }
    const uint32_t peers = EpochGetTotalNodes() > 1 ? (EpochGetTotalNodes() - 1) : 1;
    const uint32_t tolerance = static_cast<uint32_t>(g_epoch_dyn_net_queue_tolerance > 0
        ? g_epoch_dyn_net_queue_tolerance : 20) * peers;
    return EpochTxnQueueCount() >= tolerance;
}

static bool PacerSenderPressurePersistent(void)
{
    if (!PacerSenderPressure()) {
        g_pressure_since_us.store(0, std::memory_order_relaxed);
        return false;
    }
    const int64_t now = PacerMonoUs();
    const int64_t since = g_pressure_since_us.load(std::memory_order_relaxed);
    if (since == 0) {
        g_pressure_since_us.store(now, std::memory_order_relaxed);
        return false;
    }
    return (now - since) >= PacerEpochIntervalUs();
}

/* e_t = max{e+1, ê_peer + ceil(RTT/T_epoch) + K}；返回 0 表示本片不适合改期。 */
static uint64_t PacerTargetEpoch(uint64_t epoch)
{
    const uint64_t observed = EpochRecvGetEpoch();
    const int64_t rtt_us = static_cast<int64_t>(g_epoch_dyn_net_rtt_ms > 0
        ? g_epoch_dyn_net_rtt_ms : 100) * 1000;
    const int64_t t_epoch_us = PacerEpochIntervalUs();
    const int64_t slots = (rtt_us + t_epoch_us - 1) / t_epoch_us;
    const int64_t k = g_epoch_dyn_net_catchup_k > 0 ? g_epoch_dyn_net_catchup_k : 3;

    uint64_t target = observed + static_cast<uint64_t>(slots + k);
    if (target <= epoch) {
        target = epoch + 1;
    }
    const uint64_t phys = E_NodeManager::GetPhysicalEpoch();
    if (target < phys + 2) {
        target = phys + 2;
    }
    /* 最多前移 32 片；若这样仍追不上 physical，就不能改期（目标格已封口） */
    if (target > epoch + 32) {
        target = epoch + 32;
    }
    if (target < phys + 2 || target <= epoch) {
        return 0;
    }
    return target;
}

static std::atomic<uint64_t> g_admit_slot[EPOCH_TXN_EPOCH_COUNT];
static std::atomic<uint64_t> g_defer_slot[EPOCH_TXN_EPOCH_COUNT];
static constexpr uint32_t PACER_DEFER_PER_EPOCH = 32;

/*
 * 一次重排把区间 [start, start+delta) 内的输入整体后移 delta：
 * 区间内第 e 片的输入落到 e+delta，片间相对间隔保持不变。
 * floor 记录已经分配到的最大片号，新的重排不会退回到它之前。
 */
static uint64_t g_dbr_window_start = 0;
static uint64_t g_dbr_window_delta = 0;
static uint64_t g_dbr_floor = 0;

/* 所有目的端里最早还有待发输入的片。 */
static uint64_t PacerEarliestPending(void)
{
    return EpochGetSendEpoch();
}

/* 调用方持有 g_decide_mu。 */
static uint64_t PacerDeferTargetLocked(uint64_t epoch)
{
    const uint64_t phys = E_NodeManager::GetPhysicalEpoch();

    /* 同一轮重排：区间内的片整体后移 delta */
    if (g_dbr_window_delta != 0 && epoch >= g_dbr_window_start &&
        epoch < g_dbr_window_start + g_dbr_window_delta) {
        const uint64_t shifted = epoch + g_dbr_window_delta;
        if (shifted >= phys + 2 && shifted <= phys + 64) {
            return shifted;
        }
    }

    /*
     * 窗口从“最早还有待发输入的片”起算，而不是从先弹出线程所在片起算：
     * 否则同一笔写集在不同目的端的副本可能被移进不同片。
     */
    uint64_t start = PacerEarliestPending();
    if (start == 0 || start > epoch || epoch > start + 32) {
        start = epoch;
    }

    uint64_t target = PacerTargetEpoch(start);
    if (target == 0) {
        return 0;
    }
    if (target < g_dbr_floor) {
        target = g_dbr_floor;
    }
    g_dbr_window_start = start;
    g_dbr_window_delta = target - start;
    if (g_dbr_floor < target) {
        g_dbr_floor = target;
    }

    const uint64_t shifted = epoch + g_dbr_window_delta;
    if (shifted > phys + 64) {
        return 0;               /* 目标太远，直接发送 */
    }
    return shifted;
}

/* 槽位存 (epoch << 32 | used)，换片自动重置。 */
static bool PacerSlotReserve(std::atomic<uint64_t>* table, uint64_t epoch, uint32_t limit)
{
    std::atomic<uint64_t>& slot = table[EpochSlotOf(epoch)];
    uint64_t cur = slot.load(std::memory_order_acquire);
    for (;;) {
        const uint64_t bound = cur >> 32;
        uint32_t used = static_cast<uint32_t>(cur & 0xffffffffu);
        if (bound != epoch) {
            used = 0;
        }
        if (used >= limit) {
            return false;
        }
        const uint64_t want = (epoch << 32) | static_cast<uint64_t>(used + 1);
        if (slot.compare_exchange_weak(cur, want, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return true;
        }
    }
}

enum {
    PACER_DEC_SEND = 1,
    PACER_DEC_DEFER = 2,
};

struct PacerDecision {
    uint8_t state;
    uint64_t decided_epoch;
    uint64_t target_epoch;
};

static std::mutex g_decide_mu;
static std::unordered_map<uint64_t, PacerDecision> g_decide;

static inline uint64_t PacerDecideKey(uint64_t csn, uint32_t server_id)
{
    return csn ^ (static_cast<uint64_t>(server_id) << 48);
}

int EpochPacerDecideItem(uint64_t epoch, uint64_t csn, uint32_t server_id, bool is_eof,
    uint64_t* out_target)
{
    if (out_target != nullptr) {
        *out_target = 0;
    }
    if (is_eof || !EpochPacerDbrEnabled()) {
        return 1;
    }

    const uint64_t key = PacerDecideKey(csn, server_id);
    std::lock_guard<std::mutex> lk(g_decide_mu);

    auto it = g_decide.find(key);
    if (it == g_decide.end()) {
        PacerDecision d;
        d.state = PACER_DEC_SEND;
        d.decided_epoch = epoch;
        d.target_epoch = 0;

        const uint32_t budget = g_epoch_dbr_admit_per_epoch > 0
            ? static_cast<uint32_t>(g_epoch_dbr_admit_per_epoch) : 8;
        const bool admitted = PacerSlotReserve(g_admit_slot, epoch, budget);
        if (!admitted && PacerSenderPressurePersistent()) {
            const uint64_t target = PacerDeferTargetLocked(epoch);
            if (target != 0 && PacerSlotReserve(g_defer_slot, target, PACER_DEFER_PER_EPOCH)) {
                d.state = PACER_DEC_DEFER;
                d.target_epoch = target;
            }
        }
        g_decide.emplace(key, d);
        if (d.state == PACER_DEC_DEFER) {
            if (out_target != nullptr) {
                *out_target = d.target_epoch;
            }
            return 0;
        }
        return 1;
    }

    const PacerDecision& d = it->second;
    /* 只有“还在原片”的副本跟着改期；已改期的副本按目标片照常发送。 */
    if (d.state == PACER_DEC_DEFER && epoch == d.decided_epoch) {
        if (out_target != nullptr) {
            *out_target = d.target_epoch;
        }
        return 0;
    }
    return 1;
}

void EpochPacerSweepDecisions(uint64_t up_to_epoch)
{
    if (up_to_epoch < 64) {
        return;
    }
    const uint64_t floor = up_to_epoch - 64;
    std::lock_guard<std::mutex> lk(g_decide_mu);
    for (auto it = g_decide.begin(); it != g_decide.end();) {
        if (it->second.decided_epoch < floor && it->second.target_epoch < floor) {
            it = g_decide.erase(it);
        } else {
            ++it;
        }
    }
}

static std::atomic<uint64_t> g_pacer_deferred{0};
static std::atomic<uint64_t> g_merge_retry{0};

void EpochPacerNoteMergeRetry(void)
{
    g_merge_retry.fetch_add(1, std::memory_order_relaxed);
}

uint64_t EpochPacerMergeRetryCount(void)
{
    return g_merge_retry.load(std::memory_order_relaxed);
}

void EpochPacerNoteDeferred(void)
{
    g_pacer_deferred.fetch_add(1, std::memory_order_relaxed);
}

uint64_t EpochPacerDeferredCount(void)
{
    return g_pacer_deferred.load(std::memory_order_relaxed);
}

/* ---------------- PA ---------------- */

static std::atomic<uint64_t> g_pa_session{0};          /* 本节点（慢节点）的会话 id */
static std::atomic<uint64_t> g_pa_session_exit{0};
static std::atomic<uint32_t> g_pa_proxy{UINT32_MAX};
static std::atomic<bool> g_pa_reannounce{false};

/* 每个慢节点一位；值是该节点声明的首个未应用 epoch。 */
static std::atomic<uint32_t> g_pa_congested_mask{0};
static std::atomic<uint64_t> g_pa_slow_first[EPOCH_MAX_NODES];

static bool PacerPaAnyCongested(void)
{
    return g_pa_congested_mask.load(std::memory_order_acquire) != 0;
}

static uint64_t PacerPaSessionFirst(void)
{
    const uint32_t mask = g_pa_congested_mask.load(std::memory_order_acquire);
    const uint32_t total = EpochGetTotalNodes();
    uint64_t first = 0;
    for (uint32_t n = 0; n < total && n < EPOCH_MAX_NODES; ++n) {
        if ((mask & (1u << n)) == 0) {
            continue;
        }
        const uint64_t f = g_pa_slow_first[n].load(std::memory_order_acquire);
        if (f != 0 && (first == 0 || f < first)) {
            first = f;
        }
    }
    return first;
}

static void PacerPaSetSelfCongested(bool congested, uint64_t first)
{
    const uint32_t self = EpochGetNodeId();
    if (self >= EPOCH_MAX_NODES) {
        return;
    }
    if (congested) {
        if (first != 0) {
            g_pa_slow_first[self].store(first, std::memory_order_release);
        }
        g_pa_congested_mask.fetch_or(1u << self, std::memory_order_acq_rel);
    } else {
        g_pa_congested_mask.fetch_and(~(1u << self), std::memory_order_acq_rel);
    }
}

static bool PacerPaSelfCongested(void)
{
    const uint32_t self = EpochGetNodeId();
    if (self >= EPOCH_MAX_NODES) {
        return false;
    }
    return (g_pa_congested_mask.load(std::memory_order_acquire) & (1u << self)) != 0;
}

constexpr uint8_t EPOCH_PA_DIRECT = 1;
constexpr uint8_t EPOCH_PA_SKIP = 2;

static std::atomic<uint64_t> g_pa_epoch_mode[EPOCH_TXN_EPOCH_COUNT];
static std::atomic<uint64_t> g_pa_peer_decision[EPOCH_TXN_EPOCH_COUNT];

struct EpochPacerPaPending {
    E_WireTxn txn;
};

static moodycamel::ConcurrentQueue<EpochPacerPaPending> g_pa_fwd[EPOCH_TXN_EPOCH_COUNT];

/* 转发报文由发送线程投递：merge 线程不能阻塞在慢节点的链路上。 */
struct EpochPacerPaOut {
    uint32_t target;
    std::string wire;
};

static std::mutex g_pa_out_mu;
static std::deque<EpochPacerPaOut> g_pa_out_q;

static std::atomic<uint64_t> g_pa_out_dropped{0};

/* 投递成功才出队，失败留到下一个 tick，保证片内顺序。 */
static void PacerPaDrainOutbox(void)
{
    static int64_t last_log_us = 0;
    std::lock_guard<std::mutex> lk(g_pa_out_mu);
    while (!g_pa_out_q.empty()) {
        EpochPacerPaOut& msg = g_pa_out_q.front();
        /* 对端断开时不要走 PublishTo 的广播回退：会把转发报文发给所有节点 */
        if (!EpochPeerIsConnected(msg.target)) {
            break;
        }
        if (!EpochNetPublishTo(msg.target, msg.wire)) {
            break;
        }
        g_pa_out_q.pop_front();
    }

    /* 慢节点长时间不可投递时限制内存；丢的是最早的转发报文，计数可见 */
    while (g_pa_out_q.size() > 8192) {
        g_pa_out_q.pop_front();
        g_pa_out_dropped.fetch_add(1, std::memory_order_relaxed);
    }

    const int64_t now = PacerMonoUs();
    if (g_pa_out_q.size() > 1024 && now - last_log_us > 5000000) {
        last_log_us = now;
        EPOCH_DBG_FPRINTF("EPOCH_DBG pa_outbox backlog=%zu dropped=%lu\n",
            g_pa_out_q.size(), static_cast<unsigned long>(g_pa_out_dropped.load()));
        fflush(stderr);
    }
}

static void PacerPaEnqueueOut(uint32_t target, std::string* wire)
{
    EpochPacerPaOut msg;
    msg.target = target;
    msg.wire = std::move(*wire);
    std::lock_guard<std::mutex> lk(g_pa_out_mu);
    g_pa_out_q.push_back(std::move(msg));
}

/* 会话记录的落盘格式；只在状态翻转时写一次。 */
static constexpr uint32_t EPOCH_PA_STATE_MAGIC = 0x45434150u;   /* "PACE" */
static constexpr uint32_t EPOCH_PA_STATE_VERSION = 1;

struct EpochPaStateRecord {
    uint32_t magic;
    uint32_t version;
    uint64_t session;
    uint64_t first_epoch;
    uint64_t exit_epoch;
    uint32_t proxy;
    uint32_t congested;
    uint64_t checksum;
};

static uint64_t PacerStateChecksum(const EpochPaStateRecord& rec)
{
    const unsigned char* p = reinterpret_cast<const unsigned char*>(&rec);
    const size_t n = offsetof(EpochPaStateRecord, checksum);
    uint64_t h = 1469598103934665603ULL;
    for (size_t i = 0; i < n; ++i) {
        h ^= p[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static const char* PacerDataDir(void)
{
    if (t_thrd.proc_cxt.DataDir != nullptr && t_thrd.proc_cxt.DataDir[0] != '\0') {
        return t_thrd.proc_cxt.DataDir;
    }
    return nullptr;
}

static bool PacerStatePath(char* path, size_t size)
{
    const char* dir = PacerDataDir();
    if (dir == nullptr) {
        return false;
    }
    int rc = snprintf_s(path, size, size - 1, "%s/epoch_pa_session.state", dir);
    securec_check_ss(rc, "\0", "\0");
    return true;
}

static void PacerPaPersist(void)
{
    char path[1024];
    if (!PacerStatePath(path, sizeof(path))) {
        return;
    }
    char tmp[1024];
    int rc = snprintf_s(tmp, sizeof(tmp), sizeof(tmp) - 1, "%s.tmp", path);
    securec_check_ss(rc, "\0", "\0");

    EpochPaStateRecord rec;
    memset_s(&rec, sizeof(rec), 0, sizeof(rec));
    rec.magic = EPOCH_PA_STATE_MAGIC;
    rec.version = EPOCH_PA_STATE_VERSION;
    rec.session = g_pa_session.load(std::memory_order_acquire);
    rec.first_epoch = PacerPaSessionFirst();
    rec.exit_epoch = g_pa_session_exit.load(std::memory_order_acquire);
    rec.proxy = g_pa_proxy.load(std::memory_order_acquire);
    rec.congested = PacerPaSelfCongested() ? 1 : 0;
    rec.checksum = PacerStateChecksum(rec);

    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) {
        return;
    }
    const ssize_t n = write(fd, &rec, sizeof(rec));
    if (n == static_cast<ssize_t>(sizeof(rec))) {
        (void)fsync(fd);
    }
    close(fd);

    if (rename(tmp, path) != 0) {
        return;
    }
    int dfd = open(PacerDataDir(), O_RDONLY);
    if (dfd >= 0) {
        (void)fsync(dfd);
        close(dfd);
    }
}

void EpochPacerPaRestoreState(void)
{
    if (!EpochPacerPaEnabled()) {
        return;
    }
    char path[1024];
    if (!PacerStatePath(path, sizeof(path))) {
        return;
    }
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        return;
    }
    EpochPaStateRecord rec;
    const ssize_t n = read(fd, &rec, sizeof(rec));
    close(fd);
    if (n != static_cast<ssize_t>(sizeof(rec))) {
        return;
    }
    if (rec.magic != EPOCH_PA_STATE_MAGIC || rec.version != EPOCH_PA_STATE_VERSION) {
        return;
    }
    if (rec.checksum != PacerStateChecksum(rec)) {
        return;
    }

    g_pa_session.store(rec.session, std::memory_order_release);
    g_pa_session_exit.store(rec.exit_epoch, std::memory_order_release);
    if (rec.proxy < EpochGetTotalNodes() && !PacerSlowListContains(rec.proxy)) {
        g_pa_proxy.store(rec.proxy, std::memory_order_release);
    }
    if (rec.congested != 0) {
        PacerPaSetSelfCongested(true, rec.first_epoch);
        g_pa_reannounce.store(true, std::memory_order_release);
    }
}

/*
 * proxy 取“最小的健康且连通的节点”。所有副本用同一规则独立算出，
 * 不需要协商；某个节点失效时下一个 id 自动接替，也不会出现两个决策源。
 */
static uint32_t PacerPinnedProxy(void)
{
    const uint32_t self = EpochGetNodeId();
    const uint32_t total = EpochGetTotalNodes();
    for (uint32_t p = 0; p < total; ++p) {
        if (p == self || PacerSlowListContains(p) || !EpochPeerIsConnected(p)) {
            continue;
        }
        g_pa_proxy.store(p, std::memory_order_release);
        return p;
    }
    return UINT32_MAX;
}

uint32_t EpochPacerPaProxyNodeId(void)
{
    return PacerPinnedProxy();
}

static uint64_t PacerReadRxBytes(void)
{
    FILE* fp = fopen("/proc/net/dev", "r");
    if (fp == nullptr) {
        return 0;
    }
    char line[512];
    uint64_t total = 0;
    if (fgets(line, sizeof(line), fp) == nullptr || fgets(line, sizeof(line), fp) == nullptr) {
        fclose(fp);
        return 0;
    }
    while (fgets(line, sizeof(line), fp) != nullptr) {
        char* colon = strchr(line, ':');
        if (colon == nullptr) {
            continue;
        }
        const char* name = line;
        while (*name == ' ') {
            ++name;
        }
        if (strncmp(name, "lo:", 3) == 0) {
            continue;
        }
        unsigned long long rx_bytes = 0;
        if (sscanf(colon + 1, "%llu", &rx_bytes) == 1) {
            total += rx_bytes;
        }
    }
    fclose(fp);
    return total;
}

static bool PacerReceiverPressure(void)
{
    bool congested = false;

    if (g_epoch_dyn_net_download_bps > 0) {
        static uint64_t last_rx = 0;
        static int64_t last_us = 0;
        static bool inited = false;

        const int64_t now = PacerMonoUs();
        const uint64_t rx = PacerReadRxBytes();
        if (!inited || last_us == 0 || rx < last_rx) {
            inited = true;
            last_us = now;
            last_rx = rx;
        } else if (now - last_us >= 100000) {
            const double bps = static_cast<double>(rx - last_rx) * 8.0 /
                               (static_cast<double>(now - last_us) / 1000000.0);
            last_us = now;
            last_rx = rx;

            int pct = g_epoch_dbr_threshold_pct;
            if (pct < 1) {
                pct = 1;
            } else if (pct > 100) {
                pct = 100;
            }
            congested = (bps >= g_epoch_dyn_net_download_bps *
                (static_cast<double>(pct) / 100.0));
        }
    }

    const uint64_t recv_ep = EpochRecvGetEpoch();
    const uint64_t phys = E_NodeManager::GetPhysicalEpoch();
    const uint64_t lag_limit = static_cast<uint64_t>(g_epoch_dyn_net_lag_tolerance > 0
        ? g_epoch_dyn_net_lag_tolerance : 5);
    if (phys > recv_ep && (phys - recv_ep) >= lag_limit) {
        congested = true;
    }
    const uint32_t backlog_limit = static_cast<uint32_t>(g_epoch_dyn_net_queue_tolerance > 0
        ? g_epoch_dyn_net_queue_tolerance : 20) * 8;
    if (EpochIncomingQueueSize(recv_ep) >= backlog_limit) {
        congested = true;
    }
    return congested;
}

static void PacerPaPublishSession(bool congested, uint32_t proxy, uint64_t first,
    uint64_t session)
{
    E_WireTxn ctl;
    ctl.epoch = first;
    ctl.server_id = EpochGetNodeId();
    ctl.txn_state = EPOCH_TXN_EMPTY;
    ctl.csn = congested ? EPOCH_WIRE_PA_STATE_ON_CSN : EPOCH_WIRE_PA_STATE_OFF_CSN;
    ctl.route_wid = static_cast<uint16_t>(proxy);
    ctl.origin_start_us = session;
    ctl.claim_only = false;
    ctl.rows.clear();

    std::string wire;
    if (E_WireSerializeTxn(ctl, &wire)) {
        /* 慢节点也要收到，否则它学不到 proxy 的选择，会一直等不到路由决策 */
        (void)PacerPublishToAll(wire);
    }
}

static void PacerPaSlowSideTick(void)
{
    static bool last_sent = false;
    static bool last_sent_valid = false;
    static bool proxy_announced = false;
    static int64_t last_publish_us = 0;

    if (!last_sent_valid) {
        last_sent = PacerPaSelfCongested();
        last_sent_valid = true;
    }

    const uint64_t recv_ep = EpochRecvGetEpoch();
    const uint64_t first = recv_ep == 0 ? 1 : recv_ep;

    const uint32_t proxy = PacerPinnedProxy();
    if (proxy == UINT32_MAX || proxy == EpochGetNodeId()) {
        return;
    }

    /* 最小的慢节点宣告一次 proxy，其余节点只是收到同一个值 */
    if (!proxy_announced) {
        proxy_announced = true;
        if (EpochGetNodeId() == PacerSlowNodeId()) {
            PacerPaPublishSession(false, proxy, first, 0);
        }
    }

    if (g_pa_reannounce.exchange(false, std::memory_order_acq_rel)) {
        const uint64_t session = (static_cast<uint64_t>(EpochGetNodeId() + 1) << 56) |
            (static_cast<uint64_t>(PacerMonoUs()) & 0x00ffffffffffffffULL);
        g_pa_session.store(session, std::memory_order_release);
        g_pa_session_exit.store(0, std::memory_order_release);
        PacerPaSetSelfCongested(true, first);
        PacerPaPersist();
        PacerPaPublishSession(true, proxy, first, session);
        last_publish_us = PacerMonoUs();
        last_sent = true;
        return;
    }

    const bool congested = PacerReceiverPressure();
    if (congested == last_sent) {
        /* 会话进行中定期重播，防止控制消息丢失后对端一直等 */
        if (congested && PacerMonoUs() - last_publish_us >= 500000) {
            PacerPaSetSelfCongested(true, first);
            PacerPaPublishSession(true, proxy, first,
                g_pa_session.load(std::memory_order_acquire));
            last_publish_us = PacerMonoUs();
        }
        return;
    }
    last_sent = congested;

    if (!congested && g_pa_session.load(std::memory_order_acquire) == 0) {
        return;
    }

    PacerPaSetSelfCongested(congested, first);

    uint64_t session = g_pa_session.load(std::memory_order_acquire);
    if (congested || session == 0) {
        session = (static_cast<uint64_t>(EpochGetNodeId() + 1) << 56) |
            (static_cast<uint64_t>(PacerMonoUs()) & 0x00ffffffffffffffULL);
        g_pa_session.store(session, std::memory_order_release);
        g_pa_session_exit.store(0, std::memory_order_release);
    } else {
        g_pa_session_exit.store(E_NodeManager::GetPhysicalEpoch() + 1,
            std::memory_order_release);
    }

    /* 先落盘再广播 */
    PacerPaPersist();
    PacerPaPublishSession(congested, proxy, first, session);
    last_publish_us = PacerMonoUs();
}

void EpochPacerPaNoteSlowState(const E_WireTxn& ctl)
{
    if (!EpochPacerPaEnabled()) {
        return;
    }
    const uint32_t from = ctl.server_id;
    if (from >= EpochGetTotalNodes() || from >= EPOCH_MAX_NODES ||
        !PacerSlowListContains(from)) {
        return;
    }
    const bool congested = (ctl.csn == EPOCH_WIRE_PA_STATE_ON_CSN);
    const uint64_t first = ctl.epoch;
    if (first == 0) {
        return;
    }

    const uint64_t known_first = g_pa_slow_first[from].load(std::memory_order_acquire);
    if (known_first != 0 && first + 1 < known_first) {
        return;
    }
    if (first != known_first) {
        g_pa_slow_first[from].store(first, std::memory_order_release);
    }

    if (congested) {
        g_pa_congested_mask.fetch_or(1u << from, std::memory_order_acq_rel);
        g_pa_session_exit.store(0, std::memory_order_release);
    } else {
        const uint32_t prev = g_pa_congested_mask.fetch_and(~(1u << from),
            std::memory_order_acq_rel);
        if ((prev & ~(1u << from)) == 0) {
            /* 最后一个慢节点退出：此后的片恢复直发 */
            g_pa_session_exit.store(E_NodeManager::GetPhysicalEpoch() + 1,
                std::memory_order_release);
        }
    }

    /* 各慢节点算出的 proxy 相同；这里只做记录，不参与裁决 */
    const uint32_t proxy = ctl.route_wid;
    if (proxy < EpochGetTotalNodes() && !PacerSlowListContains(proxy)) {
        g_pa_proxy.store(proxy, std::memory_order_release);
    }

    EPOCH_DBG_FPRINTF("EPOCH_DBG pa_session node=%u from=%u congested=%d e_f=%lu\n",
        EpochGetNodeId(), from, congested ? 1 : 0, static_cast<unsigned long>(first));
    fflush(stderr);
}

static bool PacerPaEpochInSession(uint64_t epoch)
{
    if (!PacerPaAnyCongested()) {
        return false;
    }
    const uint64_t first = PacerPaSessionFirst();
    if (first == 0 || epoch < first) {
        return false;
    }
    const uint64_t exit_epoch = g_pa_session_exit.load(std::memory_order_acquire);
    return exit_epoch == 0 || epoch < exit_epoch;
}

static bool PacerPaEpochSkipped(uint64_t epoch)
{
    std::atomic<uint64_t>& slot = g_pa_epoch_mode[EpochSlotOf(epoch)];
    uint64_t cur = slot.load(std::memory_order_acquire);
    if ((cur >> 8) == epoch) {
        return ((cur & 0xFF) == EPOCH_PA_SKIP);
    }
    const bool skip = PacerPaEpochInSession(epoch);
    const uint64_t want = (epoch << 8) | (skip ? EPOCH_PA_SKIP : EPOCH_PA_DIRECT);
    if (slot.compare_exchange_strong(cur, want, std::memory_order_acq_rel)) {
        return skip;
    }
    return ((cur >> 8) == epoch) && ((cur & 0xFF) == EPOCH_PA_SKIP);
}

static bool PacerPaBroadcastDecision(uint64_t epoch, bool skip)
{
    E_WireTxn ctl;
    ctl.epoch = epoch;
    ctl.server_id = EpochGetNodeId();
    ctl.txn_state = EPOCH_TXN_EMPTY;
    ctl.csn = skip ? EPOCH_WIRE_PA_EPOCH_SKIP_CSN : EPOCH_WIRE_PA_EPOCH_DIRECT_CSN;
    ctl.claim_only = false;
    ctl.rows.clear();

    std::string wire;
    if (!E_WireSerializeTxn(ctl, &wire)) {
        return false;
    }
    return PacerPublishToAll(wire);
}

static bool PacerPaProxyDecide(uint64_t epoch)
{
    const bool skip = PacerPaEpochSkipped(epoch);
    static std::atomic<uint64_t> broadcast_done[EPOCH_TXN_EPOCH_COUNT];
    std::atomic<uint64_t>& mark = broadcast_done[EpochSlotOf(epoch)];
    uint64_t cur = mark.load(std::memory_order_acquire);
    if ((cur >> 8) != epoch &&
        mark.compare_exchange_strong(cur, (epoch << 8) | 1, std::memory_order_acq_rel)) {
        if (!PacerPaBroadcastDecision(epoch, skip)) {
            mark.store(0, std::memory_order_release);   /* 发送失败，允许重试 */
        }
    }
    return skip;
}

void EpochPacerPaNoteEpochDecision(uint64_t epoch, bool skip, uint32_t from_node)
{
    if (epoch == 0 || from_node != EpochPacerPaProxyNodeId()) {
        return;
    }
    const uint64_t v = (epoch << 8) | (skip ? EPOCH_PA_SKIP : EPOCH_PA_DIRECT);
    g_pa_peer_decision[EpochSlotOf(epoch)].store(v, std::memory_order_release);
}

static std::atomic<uint64_t> g_pa_decide_req[EPOCH_TXN_EPOCH_COUNT];
static std::atomic<uint32_t> g_pa_decide_pending{0};

void EpochPacerPaNoteDecideRequest(uint64_t epoch)
{
    if (!EpochPacerPaEnabled() || epoch == 0) {
        return;
    }
    if (EpochGetNodeId() != EpochPacerPaProxyNodeId()) {
        return;
    }
    const uint64_t v = (epoch << 8) | 1;
    if (g_pa_decide_req[EpochSlotOf(epoch)].exchange(v, std::memory_order_acq_rel) != v) {
        g_pa_decide_pending.fetch_add(1, std::memory_order_relaxed);
    }
}

static void PacerPaServeDecideRequests(void)
{
    if (g_pa_decide_pending.load(std::memory_order_relaxed) == 0) {
        return;
    }
    if (EpochGetNodeId() != EpochPacerPaProxyNodeId()) {
        return;
    }
    for (uint32_t slot = 0; slot < EPOCH_TXN_EPOCH_COUNT; ++slot) {
        const uint64_t v = g_pa_decide_req[slot].exchange(0, std::memory_order_acq_rel);
        const uint64_t epoch = v >> 8;
        if (epoch == 0) {
            continue;
        }
        g_pa_decide_pending.fetch_sub(1, std::memory_order_relaxed);
        (void)PacerPaProxyDecide(epoch);
    }
    /* 覆盖写可能让计数偏大；调用方每 5ms 会重发请求，归零即可自愈 */
    g_pa_decide_pending.store(0, std::memory_order_relaxed);
}

static void PacerPaPublishDecideRequest(uint64_t epoch)
{
    E_WireTxn ctl;
    ctl.epoch = epoch;
    ctl.server_id = EpochGetNodeId();
    ctl.txn_state = EPOCH_TXN_EMPTY;
    ctl.csn = EPOCH_WIRE_PA_DECIDE_REQ_CSN;
    ctl.claim_only = false;
    ctl.rows.clear();

    std::string wire;
    if (E_WireSerializeTxn(ctl, &wire)) {
        (void)PacerPublishToHealthy(wire);
    }
}

bool EpochPacerPaShouldSkipPeer(uint32_t peer, uint64_t epoch)
{
    if (!EpochPacerPaEnabled() || !PacerSlowListContains(peer)) {
        return false;
    }
    if (EpochGetNodeId() == EpochPacerPaProxyNodeId()) {
        return PacerPaProxyDecide(epoch);
    }

    std::atomic<uint64_t>& slot = g_pa_peer_decision[EpochSlotOf(epoch)];
    int spins = 0;
    for (;;) {
        const uint64_t cur = slot.load(std::memory_order_acquire);
        if ((cur >> 8) == epoch) {
            return ((cur & 0xFF) == EPOCH_PA_SKIP);
        }
        if ((++spins % 25) == 0) {
            PacerPaPublishDecideRequest(epoch);
        }
        CHECK_FOR_INTERRUPTS();
        pg_usleep(200);
    }
}

void EpochPacerPaMaybeBufferMerged(const E_WireTxn& txn)
{
    if (!EpochPacerPaEnabled() || EpochGetNodeId() != EpochPacerPaProxyNodeId()) {
        return;
    }
    if (txn.epoch == 0 || txn.claim_only || E_WireTxnIsEpochEnd(txn)) {
        return;
    }
    if (!PacerPaEpochSkipped(txn.epoch)) {
        return;
    }
    bool writable = false;
    for (const E_WireRow& row : txn.rows) {
        if (row.op_type != EPOCH_OP_READ) {
            writable = true;
            break;
        }
    }
    if (!writable) {
        return;
    }

    EpochPacerPaPending item;
    item.txn = txn;
    g_pa_fwd[EpochSlotOf(txn.epoch)].enqueue(std::move(item));
}

void EpochPacerPaResendEpoch(uint64_t epoch)
{
    if (!EpochPacerPaEnabled() || EpochGetNodeId() != EpochPacerPaProxyNodeId()) {
        return;
    }
    if (!PacerPaEpochSkipped(epoch)) {
        return;
    }

    std::vector<EpochPacerPaPending> items;
    EpochPacerPaPending item;
    auto& q = g_pa_fwd[EpochSlotOf(epoch)];
    while (q.try_dequeue(item)) {
        items.push_back(std::move(item));
    }

    /* 每笔先定形态：候选保留整笔，影子只留冲突声明 */
    std::vector<E_WireTxn> out_txns;
    out_txns.reserve(items.size());
    uint32_t candidates = 0;
    uint32_t shadows = 0;
    for (const EpochPacerPaPending& it : items) {
        const E_WireTxn& txn = it.txn;
        E_WireTxn out = txn;
        if (EpochAbortCsnContains(epoch, txn.csn, txn.server_id)) {
            out.claim_only = true;
            for (E_WireRow& row : out.rows) {
                row.tuple_data.clear();
            }
            ++shadows;
        } else {
            ++candidates;
        }
        out_txns.push_back(std::move(out));
    }

    const uint32_t total = EpochGetTotalNodes();
    uint32_t targets = 0;
    for (uint32_t slow = 0; slow < total; ++slow) {
        if (!PacerSlowListContains(slow)) {
            continue;
        }
        ++targets;

        /* 数据：该慢节点自己的写集它本来就有，其余写集都要转给它 */
        for (const E_WireTxn& txn : out_txns) {
            if (txn.server_id == slow) {
                continue;
            }
            std::string wire;
            if (E_WireSerializeTxn(txn, &wire)) {
                PacerPaEnqueueOut(slow, &wire);
            }
        }

        /* EOF：该片被整体抑制，代理替所有其它节点补发 */
        for (uint32_t p = 0; p < total; ++p) {
            if (p == slow) {
                continue;
            }
            E_WireTxn eof_wire;
            eof_wire.epoch = epoch;
            eof_wire.server_id = p;
            eof_wire.txn_state = EPOCH_TXN_EMPTY;
            eof_wire.csn = 0;
            eof_wire.claim_only = false;
            eof_wire.rows.clear();

            std::string payload;
            if (E_WireSerializeTxn(eof_wire, &payload)) {
                PacerPaEnqueueOut(slow, &payload);
            }
        }
    }

    EPOCH_DBG_FPRINTF("EPOCH_DBG pa_resend epoch=%lu candidates=%u shadows=%u targets=%u\n",
        static_cast<unsigned long>(epoch), candidates, shadows, targets);
    fflush(stderr);
}

void EpochPacerPackTick(void)
{
    PacerSampleEpochInterval();
    if (!EpochPacerPaEnabled()) {
        return;
    }
    PacerPaServeDecideRequests();
    PacerPaDrainOutbox();
    if (EpochPacerIsSlowSelf()) {
        PacerPaSlowSideTick();
    }
}
