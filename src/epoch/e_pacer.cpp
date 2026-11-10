#include "postgres.h"

#include "miscadmin.h"

#include "epoch/e_pacer.h"

#include "epoch/e_abort_csn.h"
#include "epoch/e_config.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_crdt_merge_logic.h"
#include "epoch/e_net.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_perf.h"
#include "epoch/e_statistician.h"
#include "epoch/e_txn_wire.h"

#include "neu_concurrency_tools/concurrentqueue.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <atomic>
#include <unordered_map>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

/*
 * ============================================================================
 * Pacer：DBR（发送端拥塞丢弃）+ PA（慢节点下行代理转发）
 * ============================================================================
 *
 * DBR 只做一件事，在发送线程里逐条判断：
 *   非收尾包且在对应 peer 方向上拥塞  → 丢弃（不入 ZMQ）
 *   收尾包                            → 必须发送
 * 判据是本机上行是否已经跑满（EpochNetUplinkCongested：/proc/net/dev 采样
 * 到的发送速率 >= 配置上行带宽的 90%）。不看 ZMQ 队列——加闸门后队列基本不满。
 *
 * 丢弃是安全的：对端的 declared 计数在收到 EOF 时冻结为"实际收到的条数"
 * （见 e_recv_ledger.cpp: EpochRecvNoteEof），所以少发的写集不会被期待。
 *
 * PA 部分保留完整实现，供后续三副本下行受限的实验使用。
 */

static bool g_pacer_logged = false;

bool EpochPacerPaEnabled(void)
{
    return g_enable_epoch_multimaster && EpochGetTotalNodes() > 1 && g_epoch_dyn_net_proxy;
}

bool EpochPacerDbrEnabled(void)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1 || !g_epoch_dyn_net_catchup) {
        return false;
    }
    /*
     * 只有慢节点才启用 DBR。
     * 快节点的上行没有受限，本来就不会拥塞；若也允许丢弃，只会在瞬时抖动时
     * 白白丢数据，还把"谁该丢"的语义搞乱。
     */
    return EpochPacerIsSlowSelf();
}

bool EpochPacerIsSlowSelf(void)
{
    const char* s = g_epoch_dyn_net_slow_nodes;
    if (s == nullptr || s[0] == '\0') {
        return false;
    }
    const uint32_t self = EpochGetNodeId();
    int cur = 0;
    bool in_num = false;
    for (const char* p = s;; ++p) {
        if (*p >= '0' && *p <= '9') {
            cur = cur * 10 + (*p - '0');
            in_num = true;
            continue;
        }
        if (in_num && static_cast<uint32_t>(cur) == self) {
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

static bool EpochPacerIdInSlowList(uint32_t node)
{
    const char* s = g_epoch_dyn_net_slow_nodes;
    if (s == nullptr || s[0] == '\0') {
        return false;
    }
    int cur = 0;
    bool in_num = false;
    for (const char* p = s;; ++p) {
        if (*p >= '0' && *p <= '9') {
            cur = cur * 10 + (*p - '0');
            in_num = true;
            continue;
        }
        if (in_num && static_cast<uint32_t>(cur) == node) {
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

/*
 * 最小 DBR 判据：现在能不能发。
 *
 * 判据是"本机上行是否已经跑满"（EpochNetUplinkCongested，网卡速率 >=
 * 配置上行带宽的 90%），不再看 ZMQ 队列——加了快照闸门后 backend 被 apply
 * 拖住，ZMQ 队列基本不会满，POLLOUT 永远可写，DBR 就成了死代码。
 */
bool EpochPacerShouldDefer(void)
{
    if (!EpochPacerDbrEnabled()) {
        return false;
    }
    return EpochNetUplinkCongested();
}

/*
 * 每条写集的统一决策表。
 *
 * 一笔写集在每个 peer 队列里各有一份，会分别被两条发送线程 pop 到。
 * 若各自独立判断拥塞，就可能"一份发、一份丢"。这里用 CAS 保证：
 *   第一条到达的线程做决定（并负责自环入队），其余线程读它的结论。
 *
 * 单元存放 (epoch << 8) | flag，flag: 1 = 发送，2 = 丢弃。
 * 带上 epoch 就自动规避了环形复用（旧 epoch 的值不会被误读）。
 */
/*
 * DBR 决策表：**每笔事务一条记录**，key = csn ^ (server_id << 48)（全局唯一）。
 *
 * 为什么不用 (epoch, index) 当键：
 *   1) 各 peer 队列的下标不保证对齐（延后重投、环形复用都会让 head 错位）
 *   2) 延后次数必须是"整笔"的：不能一个 peer 延后、另一个发送
 * 所以把"结论"和"次数"放在同一条记录上，所有线程读同一份。
 */
static constexpr uint64_t EPOCH_DBR_DEFER_AHEAD = 4;
static constexpr uint32_t EPOCH_DBR_MAX_DEFER = 3;
static constexpr uint8_t EPOCH_DEC_UNDECIDED = 0;
static constexpr uint8_t EPOCH_DEC_SEND = 1;
static constexpr uint8_t EPOCH_DEC_DEFER = 2;
static constexpr uint8_t EPOCH_DEC_FORCED = 3;   /* 延后次数用尽，强制发送 */
static constexpr uint8_t EPOCH_DEC_DROP = 4;     /* 直接丢弃（drop 模式） */

/* 拥塞时是丢弃还是延后：由 epoch_dbr_mode 决定 */
static bool EpochPacerDbrDropMode(void)
{
    const char* m = g_epoch_dbr_mode;
    return (m != nullptr && m[0] == 'd');    /* "drop" */
}

struct EpochPacerDecide {
    uint8_t state{EPOCH_DEC_UNDECIDED};
    uint8_t count{0};
    uint64_t last_epoch{0};
};

static std::mutex g_decide_mu;
static std::unordered_map<uint64_t, EpochPacerDecide> g_decide;

static inline uint64_t EpochPacerDecideKey(uint64_t csn, uint32_t server_id)
{
    return csn ^ (static_cast<uint64_t>(server_id) << 48);
}

/* 定期清理：已经很旧的决策记录直接删掉，避免无限增长 */
void EpochPacerSweepDecisions(uint64_t up_to_epoch)
{
    if (up_to_epoch < 64) {
        return;
    }
    const uint64_t floor = up_to_epoch - 64;
    std::lock_guard<std::mutex> lk(g_decide_mu);
    for (auto it = g_decide.begin(); it != g_decide.end();) {
        if (it->second.last_epoch < floor) {
            it = g_decide.erase(it);
        } else {
            ++it;
        }
    }
}

/* 目标片：当前片 + AHEAD，且至少比 physical 大 2（保证目标片还没封口） */
uint64_t EpochPacerDeferTarget(uint64_t epoch)
{
    uint64_t target = epoch + EPOCH_DBR_DEFER_AHEAD;
    const uint64_t phys = E_NodeManager::GetPhysicalEpoch();
    if (target < phys + 2) {
        target = phys + 2;
    }
    return target;
}

/* 兼容旧接口：目标片由内部计算，调用方不再需要 DeferDone */
void EpochPacerDeferDone(uint64_t csn, uint32_t server_id)
{
    (void)csn;
    (void)server_id;
}

/*
 * 自环入队的固定负责人（最小的对端 id）。
 *
 * 注意：这个"固定线程"只用来保证同片写入顺序，不代表它的速度可以慢——
 * 因此自环最好由独立的 self 线程承担（见 e_pipeline 的 self drain）。
 */
uint32_t EpochPacerSelfLoopPeer(void)
{
    const uint32_t self = EpochGetNodeId();
    const uint32_t total = EpochGetTotalNodes();
    for (uint32_t p = 0; p < total; ++p) {
        if (p != self) {
            return p;
        }
    }
    return UINT32_MAX;
}

/*
 * 单笔事务的统一决策：返回 1 = 发送、0 = 延后。
 *
 * 所有 peer 线程 + self 线程读同一张表（key = csn ^ server），因此
 * "这笔到底发不发"是整笔一致的；延后次数也是整笔共享的。
 */
int EpochPacerDecideItem(uint64_t epoch, uint64_t csn, uint32_t server_id, bool is_eof)
{
    if (is_eof) {
        return 1;                       /* 收尾包永远发送 */
    }

    const uint64_t key = EpochPacerDecideKey(csn, server_id);
    std::lock_guard<std::mutex> lk(g_decide_mu);

    auto it = g_decide.find(key);
    if (it == g_decide.end()) {
        const bool congested = EpochPacerShouldDefer();
        EpochPacerDecide d;
        d.count = 0;
        d.last_epoch = epoch;
        if (!congested) {
            d.state = EPOCH_DEC_SEND;
        } else if (EpochPacerDbrDropMode()) {
            d.state = EPOCH_DEC_DROP;      /* 丢弃：整笔都不发、也不自环 */
        } else {
            d.state = EPOCH_DEC_DEFER;
            d.count = 1;
        }
        g_decide.emplace(key, d);
        return (d.state == EPOCH_DEC_DROP) ? 2 : (d.state == EPOCH_DEC_DEFER ? 0 : 1);
    }

    EpochPacerDecide& d = it->second;
    d.last_epoch = epoch;

    if (d.state == EPOCH_DEC_FORCED) {
        return 1;                       /* 次数用尽：一律发送 */
    }
    if (d.state == EPOCH_DEC_DROP) {
        return 2;                       /* 整笔丢弃（所有 peer 一致） */
    }
    if (d.state == EPOCH_DEC_DEFER) {
        if (d.count < EPOCH_DBR_MAX_DEFER) {
            ++d.count;
            return 0;                   /* 再延后一次 */
        }
        d.state = EPOCH_DEC_FORCED;
        return 1;
    }
    return 1;
}

static std::atomic<uint64_t> g_pacer_dropped{0};

/* merge 入队失败后的重试次数（背压指标，正常应为 0） */
static std::atomic<uint64_t> g_merge_retry{0};

void EpochPacerNoteMergeRetry(void)
{
    g_merge_retry.fetch_add(1, std::memory_order_relaxed);
}

uint64_t EpochPacerMergeRetryCount(void)
{
    return g_merge_retry.load(std::memory_order_relaxed);
}

void EpochPacerNoteDropped(void)
{
    g_pacer_dropped.fetch_add(1, std::memory_order_relaxed);
}

uint64_t EpochPacerDroppedCount(void)
{
    return g_pacer_dropped.load(std::memory_order_relaxed);
}

void EpochPacerPackTick(void)
{
    if (EpochPacerPaEnabled() && EpochPacerIsSlowSelf()) {
        EpochPacerPaSlowSideTick();      /* 慢节点：采样下行速率并广播状态 */
    }
}

/* 单调时钟（微秒），用于 PA 采样 */
static int64_t EpochPacerMonoUs(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<int64_t>(ts.tv_sec) * 1000000 + static_cast<int64_t>(ts.tv_nsec / 1000);
}

/* ============================================================================
 * PA（最小版）：慢节点下行受限时的代理转发
 *
 *   慢节点：采样自己的"入向"速率，接近下行上限时把状态广播给 proxy
 *   proxy ：收到状态后，对每个 send epoch 做一次决策
 *             · 拥塞   → 本片不直接发给慢节点（改由 merge 之后统一转发）
 *             · 不拥塞 → 正常直发
 *   proxy merge：对"被跳过"的片，收集 CRDT 判定为赢家的事务，该片 merge
 *            完成时一次性发给慢节点，并补一个 EOF。
 * ==========================================================================*/

/* proxy 侧：最近一次收到的慢节点下行拥塞状态 */
static std::atomic<bool> g_pa_slow_congested{false};

/*
 * proxy 侧：每个 epoch 对慢节点的发送方式
 *   0 = 未决定，1 = 直发（未拥塞），2 = 跳过（改由 merge 后转发）
 * 高 8 位存 epoch，规避环形复用误读。
 */
static std::atomic<uint64_t> g_pa_epoch_mode[EPOCH_TXN_EPOCH_COUNT];

/*
 * proxy 侧：被跳过片的待转发写集（按 epoch 分格的并发队列，无锁）
 *
 * 只缓存原始 wire + 身份，不做"是不是赢家"的判定——
 * 判定必须等到该片 merge 全部结束（abort 集合此时才完整）再做。
 */
struct EpochPacerPaPending {
    uint64_t csn = 0;
    uint32_t server_id = 0;
    std::string wire;
};

static moodycamel::ConcurrentQueue<EpochPacerPaPending> g_pa_fwd[EPOCH_TXN_EPOCH_COUNT];
static std::atomic<bool> g_pa_logged{false};

constexpr uint8_t EPOCH_PA_DIRECT = 1;
constexpr uint8_t EPOCH_PA_SKIP = 2;

/* 慢节点 id（slow_nodes 名单里的第一个）；没有则 UINT32_MAX */
static uint32_t EpochPacerPaSlowNodeId(void)
{
    const uint32_t total = EpochGetTotalNodes();
    for (uint32_t p = 0; p < total; ++p) {
        if (EpochPacerIdInSlowList(p)) {
            return p;
        }
    }
    return UINT32_MAX;
}

/* proxy id（第一台不在慢名单里的节点）；没有则 UINT32_MAX */
uint32_t EpochPacerPaProxyNodeId(void)
{
    const uint32_t total = EpochGetTotalNodes();
    for (uint32_t p = 0; p < total; ++p) {
        if (!EpochPacerIdInSlowList(p)) {
            return p;
        }
    }
    return UINT32_MAX;
}

/* proxy 侧：收到慢节点广播的状态 */
void EpochPacerPaNoteSlowState(bool congested)
{
    const bool old = g_pa_slow_congested.exchange(congested, std::memory_order_acq_rel);
    if (old != congested) {
        EPOCH_DBG_FPRINTF("EPOCH_DBG pa_slow_state congested=%d\n", congested ? 1 : 0);
        fflush(stderr);
    }
}

/* 慢节点侧：读入向累计字节（所有非 lo 网卡） */
static uint64_t EpochPacerPaReadRxBytes(void)
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

/* 慢节点侧：采样入向速率，状态翻转时单播给 proxy */
void EpochPacerPaSlowSideTick(void)
{
    const double cap = g_epoch_dyn_net_download_bps;
    if (cap <= 0) {
        return;
    }

    static uint64_t last_rx = 0;
    static int64_t last_us = 0;
    static bool inited = false;
    static bool last_sent = false;
    static bool last_sent_valid = false;

    const int64_t now = EpochPacerMonoUs();
    const uint64_t rx = EpochPacerPaReadRxBytes();
    if (!inited || last_us == 0 || rx < last_rx) {
        inited = true;
        last_us = now;
        last_rx = rx;
        return;
    }
    if (now - last_us < 100000) {          /* 100ms 采样窗口 */
        return;
    }
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
    const bool congested = (bps >= cap * (static_cast<double>(pct) / 100.0));

    if (last_sent_valid && last_sent == congested) {
        return;                            /* 状态没变就不发 */
    }
    last_sent = congested;
    last_sent_valid = true;

    const uint32_t proxy = EpochPacerPaProxyNodeId();
    if (proxy == UINT32_MAX || proxy == EpochGetNodeId()) {
        return;
    }

    E_WireTxn ctl;
    ctl.epoch = 1;                         /* 控制消息，接收端按 csn 识别 */
    ctl.server_id = EpochGetNodeId();
    ctl.txn_state = EPOCH_TXN_EMPTY;
    ctl.csn = congested ? EPOCH_WIRE_PA_STATE_ON_CSN : EPOCH_WIRE_PA_STATE_OFF_CSN;
    ctl.rows.clear();

    std::string wire;
    if (E_WireSerializeTxn(ctl, &wire)) {
        /*
     * 广播给所有对端：不只 proxy，其它快节点也要知道，
     * 否则它们会继续直发给慢节点，造成重复投递。
     */
    (void)EpochNetPublish(wire);
        if (!g_pa_logged.exchange(true)) {
            ereport(LOG, (errmsg("epoch pacer: PA slow-side reports to node %u", proxy)));
        }
    }
}

static bool EpochPacerPaEpochSkipped(uint64_t epoch);

/* 非代理节点：代理广播的每片决策（epoch<<8 | flag，0 表示未知） */
static std::atomic<uint64_t> g_pa_peer_decision[EPOCH_TXN_EPOCH_COUNT];

void EpochPacerPaNoteEpochDecision(uint64_t epoch, bool skip)
{
    if (epoch == 0) {
        return;
    }
    const uint64_t v = (epoch << 8) | (skip ? EPOCH_PA_SKIP : EPOCH_PA_DIRECT);
    g_pa_peer_decision[EpochSlotOf(epoch)].store(v, std::memory_order_release);
}

/* 代理决定完一片后，广播给其它快节点（每片一次） */
static void EpochPacerPaBroadcastDecision(uint64_t epoch, bool skip)
{
    E_WireTxn ctl;
    ctl.epoch = epoch;
    ctl.server_id = EpochGetNodeId();
    ctl.txn_state = EPOCH_TXN_EMPTY;
    ctl.csn = skip ? EPOCH_WIRE_PA_EPOCH_SKIP_CSN : EPOCH_WIRE_PA_EPOCH_DIRECT_CSN;
    ctl.rows.clear();

    std::string wire;
    if (E_WireSerializeTxn(ctl, &wire)) {
        (void)EpochNetPublish(wire);
    }
}

/* proxy 发送端：这一片要不要跳过慢节点（首次遇到该片时按当时状态定） */
bool EpochPacerPaShouldSkipPeer(uint32_t peer, uint64_t epoch)
{
    if (!EpochPacerPaEnabled()) {
        return false;
    }
    if (peer != EpochPacerPaSlowNodeId()) {
        return false;
    }

    /* 代理：本地拍板，并广播给其它快节点 */
    if (EpochGetNodeId() == EpochPacerPaProxyNodeId()) {
        const bool skip = EpochPacerPaEpochSkipped(epoch);
        static std::atomic<uint64_t> broadcast_done[EPOCH_TXN_EPOCH_COUNT];
        std::atomic<uint64_t>& mark = broadcast_done[EpochSlotOf(epoch)];
        uint64_t cur = mark.load(std::memory_order_acquire);
        if ((cur >> 8) != epoch &&
            mark.compare_exchange_strong(cur, (epoch << 8) | 1, std::memory_order_acq_rel)) {
            EpochPacerPaBroadcastDecision(epoch, skip);
        }
        return skip;
    }

    /*
     * 其它快节点：无条件等代理对这片给出决策。
     * 代理在"第一次碰到该片"时就会广播，而代理的发送线程不依赖本节点，
     * 所以这里一定会被唤醒；不设超时是为了保证结论完全一致
     * （超时兜底会引入"一片之内两个节点结论不同"的窗口）。
     */
    std::atomic<uint64_t>& slot = g_pa_peer_decision[EpochSlotOf(epoch)];
    for (;;) {
        const uint64_t cur = slot.load(std::memory_order_acquire);
        if ((cur >> 8) == epoch) {
            return ((cur & 0xFF) == EPOCH_PA_SKIP);
        }
        CHECK_FOR_INTERRUPTS();
        pg_usleep(200);
    }
}

/*
 * 该片是否"跳过慢节点"。
 *
 * 未决定时按当前拥塞状态就地决定（CAS），这样发送线程与 merge 线程
 * 无论谁先到达，都会得到同一结论——否则 merge 若跑在 send 前面，
 * 会误判为"没跳过"而不缓存赢家，慢节点就永远收不齐这一片。
 */
static bool EpochPacerPaEpochSkipped(uint64_t epoch)
{
    std::atomic<uint64_t>& slot = g_pa_epoch_mode[EpochSlotOf(epoch)];
    uint64_t cur = slot.load(std::memory_order_acquire);
    if ((cur >> 8) == epoch) {
        return ((cur & 0xFF) == EPOCH_PA_SKIP);
    }

    const bool congested = g_pa_slow_congested.load(std::memory_order_relaxed);
    const uint64_t want = (epoch << 8) | (congested ? EPOCH_PA_SKIP : EPOCH_PA_DIRECT);
    if (slot.compare_exchange_strong(cur, want, std::memory_order_acq_rel)) {
        return congested;
    }
    return ((cur >> 8) == epoch) && ((cur & 0xFF) == EPOCH_PA_SKIP);
}

/* proxy merge 侧：每合并完一笔，若是"被跳过片"的赢家就缓存 */
void EpochPacerPaMaybeBufferMerged(const E_WireTxn& txn)
{
    if (!EpochPacerPaEnabled() || EpochGetNodeId() != EpochPacerPaProxyNodeId()) {
        return;
    }
    if (txn.epoch == 0 || !EpochPacerPaEpochSkipped(txn.epoch)) {
        return;
    }
    /* 收尾包不缓存：由 Resend 统一补 */
    if (E_WireTxnIsEpochEnd(txn)) {
        return;
    }

    /*
     * 慢节点自己的事务它本来就有（自环入队），再转发回去会变成重复投递：
     * 第二份同 csn 同 server_id 在合并时必然判输，白白抬高 abort。
     */
    if (txn.server_id == EpochPacerPaSlowNodeId()) {
        return;
    }

    EpochPacerPaPending item;
    item.csn = txn.csn;
    item.server_id = txn.server_id;
    if (!E_WireSerializeTxn(txn, &item.wire)) {
        return;
    }
    g_pa_fwd[EpochSlotOf(txn.epoch)].enqueue(std::move(item));
}

/* proxy merge 侧：该片 merge 完成时，把缓存的赢家 + EOF 一次性发给慢节点 */
void EpochPacerPaResendEpoch(uint64_t epoch)
{
    if (!EpochPacerPaEnabled() || EpochGetNodeId() != EpochPacerPaProxyNodeId()) {
        return;
    }
    if (!EpochPacerPaEpochSkipped(epoch)) {
        return;
    }

    const uint32_t slow = EpochPacerPaSlowNodeId();
    if (slow == UINT32_MAX || slow == EpochGetNodeId()) {
        return;
    }

    /*
     * 到这一刻该片的 merge 已全部完成（merge 游标刚推进），
     * abort 集合才完整，可以准确判断哪些事务真正输了。
     */
    std::vector<EpochPacerPaPending> items;
    EpochPacerPaPending item;
    auto& q = g_pa_fwd[EpochSlotOf(epoch)];
    while (q.try_dequeue(item)) {
        items.push_back(std::move(item));
    }

    uint32_t forwarded = 0;
    uint32_t skipped = 0;
    for (const EpochPacerPaPending& it : items) {
        if (EpochAbortCsnContains(epoch, it.csn, it.server_id)) {
            ++skipped;                     /* 判输：不转发 */
            continue;
        }
        (void)EpochNetPublishTo(slow, it.wire);
        ++forwarded;
    }

    /*
     * 补 EOF：除了本端，还要替"被抑制的其它快节点"各补一个（server_id 用它们的 id）。
     *
     * 慢节点要求每个节点都有 EOF 才会推进 recv；被跳过的片上 node2 一条数据都没发，
     * 它的 declared 自然是 0，所以由代理代发一个同身份的 EOF 即可。
     * 顺序：先转发数据，再发这些 EOF（同一连接内有序）。
     */
    const uint32_t total = EpochGetTotalNodes();
    std::string eof_payload;
    for (uint32_t p = 0; p < total; ++p) {
        if (p == slow) {
            continue;
        }
        E_WireTxn eof_wire;
        eof_wire.epoch = epoch;
        eof_wire.server_id = p;                 /* 以被代发节点的身份 */
        eof_wire.txn_state = EPOCH_TXN_EMPTY;
        eof_wire.csn = 0;
        eof_wire.rows.clear();

        eof_payload.clear();
        if (E_WireSerializeTxn(eof_wire, &eof_payload)) {
            (void)EpochNetPublishTo(slow, eof_payload);
        }
    }

    EPOCH_DBG_FPRINTF("EPOCH_DBG pa_resend epoch=%lu forwarded=%u skipped=%u to_node=%u\n",
        static_cast<unsigned long>(epoch), forwarded, skipped, slow);
    fflush(stderr);
}
