#include "postgres.h"

#include "epoch/e_pacer.h"

#include "epoch/e_config.h"
#include "epoch/e_merge_pool.h"
#include "epoch/e_net.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_send_queue.h"
#include "epoch/e_statistician.h"
#include "epoch/e_txn_wire.h"

#include <atomic>
#include <ctime>
#include <map>
#include <mutex>
#include <vector>

static constexpr int EPOCH_PACER_PERSIST_TICKS = 5;
static constexpr int64_t EPOCH_PACER_PERSIST_US = 5000;
/* 测试还在提交时不把后移写集放回。提交停一段时间且缓冲已空，再按目的 epoch 发出。 */
static constexpr int64_t EPOCH_PACER_IDLE_RELEASE_US = 5000000;
static constexpr int EPOCH_PACER_MAX_MOVES = 4;
static constexpr uint64_t EPOCH_PACER_MAX_DELTA = 64;
/* 协助窗口要盖住接下来这一段 epoch，并在用完之前续上，避免退回对慢节点的广播。 */
static constexpr uint64_t EPOCH_PACER_PA_WINDOW = 64;
static constexpr uint64_t EPOCH_PACER_PA_REFRESH_LEAD = 16;
static constexpr size_t EPOCH_PACER_MAX_MOVE_PER_TICK = 4096;

struct EpochPacerPending {
    std::string wire;
    int moves = 0;
};

struct EpochPacerSession {
    bool active = false;
    uint32_t slow_id = 0;
    uint32_t proxy_id = 0;
    uint32_t session_id = 0;
    uint64_t epoch_from = 0;
    uint64_t epoch_to = 0;
};

static std::mutex g_pacer_mu;
/* 刚扣住、计数仍在原 epoch 上。关窗前会改记走。 */
static std::map<uint64_t, std::vector<EpochPacerPending>> g_pacer_pending;
/* 已经改记到目的 epoch，计数在目的 epoch 上。轮到该 epoch 关窗时再决定发送或继续后移。 */
static std::map<uint64_t, std::vector<EpochPacerPending>> g_pacer_scheduled;
static int64_t g_pacer_kernel_since_us = 0;
static std::atomic<bool> g_pacer_latched{false};
static std::atomic<uint64_t> g_pacer_floor{0};
static std::atomic<int64_t> g_pacer_last_new_us{0};

static int64_t EpochPacerMonoUs(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<int64_t>(ts.tv_sec) * 1000000 + static_cast<int64_t>(ts.tv_nsec) / 1000;
}

static bool EpochPacerEpochSealed(uint64_t epoch)
{
    uint32_t self = EpochGetNodeId();
    return E_NodeManager::GetSealedPackTxnCounters(epoch, self) > 0 ||
           E_NodeManager::IsNodeSendCompleted(epoch, self);
}

static bool EpochPacerIdInSlowList(uint32_t node);
static uint64_t EpochPacerComputeTarget(uint64_t base);

static bool EpochPacerIsSlowSelf(void)
{
    return EpochPacerIdInSlowList(EpochGetNodeId());
}

static void EpochPacerNoteNewTxn(void)
{
    g_pacer_last_new_us.store(EpochPacerMonoUs(), std::memory_order_release);
}

/* 实验的 epoch 周期是 max(10ms, RTT)，不用写死的 10ms。 */
static int EpochPacerEpochMs(void)
{
    int rtt = g_epoch_dyn_net_rtt_ms > 0 ? g_epoch_dyn_net_rtt_ms : 100;
    return rtt < 10 ? 10 : rtt;
}

/*
 * 只有慢节点会 latch。连续 5ms unsent 偏高才置位。
 * 置位后，测试还在产生新事务时保持，避免一停发 unsent 掉下来就把积压倒回当前 epoch。
 * 新事务停了且缓冲已空，视为链路不再被测试灌满，交给关窗把目的 epoch 的写集发出去。
 */
static bool EpochPacerCongestionLatched(void)
{
    if (!EpochPacerIsSlowSelf()) {
        return false;
    }
    bool kernel = EpochNetUplinkCongested();
    int64_t now = EpochPacerMonoUs();
    if (kernel) {
        if (g_pacer_kernel_since_us == 0) {
            g_pacer_kernel_since_us = now;
        }
        if (now - g_pacer_kernel_since_us >= EPOCH_PACER_PERSIST_US) {
            g_pacer_latched.store(true, std::memory_order_release);
        }
        return g_pacer_latched.load(std::memory_order_acquire);
    }
    g_pacer_kernel_since_us = 0;
    if (!g_pacer_latched.load(std::memory_order_acquire)) {
        return false;
    }
    int64_t last_new = g_pacer_last_new_us.load(std::memory_order_acquire);
    if (last_new != 0 && now - last_new < EPOCH_PACER_IDLE_RELEASE_US) {
        return true;
    }
    g_pacer_latched.store(false, std::memory_order_release);
    g_pacer_floor.store(0, std::memory_order_release);
    return false;
}

static std::mutex g_pacer_session_mu;
static EpochPacerSession g_pacer_session;
static uint32_t g_pacer_next_session = 1;
static bool g_pacer_logged = false;
static bool g_pacer_pa_short_logged = false;

bool EpochPacerDbrEnabled(void)
{
    return g_enable_epoch_multimaster && EpochGetTotalNodes() > 1 && g_epoch_dyn_net_catchup;
}

bool EpochPacerPaEnabled(void)
{
    return g_enable_epoch_multimaster && EpochGetTotalNodes() > 1 && g_epoch_dyn_net_proxy;
}

uint64_t EpochPacerAssignmentFloor(void)
{
    if (!g_pacer_latched.load(std::memory_order_acquire) || !EpochPacerIsSlowSelf()) {
        return 0;
    }
    /* 随 logical/peer 前滑，新事务落到当前目的 epoch，不再钉死在第一次算出的号上。 */
    uint64_t target = EpochPacerComputeTarget(0);
    g_pacer_floor.store(target, std::memory_order_release);
    return target;
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

static uint64_t EpochPacerMaxPeerEpoch(void)
{
    uint64_t peer_max = 0;
    uint32_t self = EpochGetNodeId();
    uint32_t total = EpochGetTotalNodes();
    for (uint32_t peer = 0; peer < total; ++peer) {
        if (peer == self) {
            continue;
        }
        uint64_t sealed = EpochGetPeerMaxSealedEpoch(peer);
        if (sealed > peer_max) {
            peer_max = sealed;
        }
    }
    return peer_max;
}

bool EpochPacerHoldLocal(const E_WireTxn& txn, const std::string& wire)
{
    if (!EpochPacerDbrEnabled() || !EpochPacerIsSlowSelf() || txn.epoch == 0 || wire.empty()) {
        return false;
    }
    /* 已经关窗的 epoch 不能再改计数，写集照常发送。 */
    if (EpochPacerEpochSealed(txn.epoch)) {
        return false;
    }
    if (!g_pacer_latched.load(std::memory_order_acquire)) {
        return false;
    }

    EpochPacerNoteNewTxn();
    EpochPacerPending item;
    item.wire = wire;
    item.moves = 0;
    std::lock_guard<std::mutex> lock(g_pacer_mu);
    g_pacer_pending[txn.epoch].push_back(std::move(item));
    return true;
}

bool EpochPacerParkLocal(uint64_t epoch, const std::string& wire)
{
    if (!EpochPacerDbrEnabled() || !EpochPacerIsSlowSelf() || !g_pacer_latched.load(std::memory_order_acquire) ||
        epoch == 0 || wire.empty()) {
        return false;
    }
    EpochPacerNoteNewTxn();
    EpochPacerPending item;
    item.wire = wire;
    item.moves = 0;
    std::lock_guard<std::mutex> lock(g_pacer_mu);
    g_pacer_scheduled[epoch].push_back(std::move(item));
    return true;
}

bool EpochPacerBlocksSeal(uint64_t epoch)
{
    if (!EpochPacerDbrEnabled() || epoch == 0) {
        return false;
    }
    std::lock_guard<std::mutex> lock(g_pacer_mu);
    auto pending = g_pacer_pending.find(epoch);
    if (pending != g_pacer_pending.end() && !pending->second.empty()) {
        return true;
    }
    auto scheduled = g_pacer_scheduled.find(epoch);
    return scheduled != g_pacer_scheduled.end() && !scheduled->second.empty();
}

static void EpochPacerRepark(std::vector<std::pair<uint64_t, EpochPacerPending>>* leftover)
{
    if (leftover->empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(g_pacer_mu);
    for (auto& slot : *leftover) {
        g_pacer_scheduled[slot.first].push_back(std::move(slot.second));
    }
}

/* merge 只取 merge_epoch 那一行。更后面的 epoch 先留着，格子被写满会让 pack 线程退出。 */
static void EpochPacerAdmitList(std::vector<std::pair<uint64_t, EpochPacerPending>>& batch)
{
    const uint64_t merge_epoch = EpochMergePoolGetMergeEpoch();
    std::vector<std::pair<uint64_t, EpochPacerPending>> leftover;
    for (auto& slot : batch) {
        if (merge_epoch > 0 && slot.first > merge_epoch) {
            leftover.push_back(std::move(slot));
            continue;
        }
        E_WireTxn txn;
        if (!E_WireDeserializeTxn(slot.second.wire.data(), slot.second.wire.size(), &txn)) {
            continue;
        }
        if (!EpochPipelineAdmitLocalWire(txn, slot.second.wire)) {
            leftover.push_back(std::move(slot));
        }
    }
    EpochPacerRepark(&leftover);
}

static void EpochPacerTakeEpoch(
    std::map<uint64_t, std::vector<EpochPacerPending>>* table,
    uint64_t epoch,
    std::vector<std::pair<uint64_t, EpochPacerPending>>* release)
{
    auto it = table->find(epoch);
    if (it == table->end()) {
        return;
    }
    for (auto& item : it->second) {
        release->emplace_back(epoch, std::move(item));
    }
    table->erase(it);
}

static uint64_t EpochPacerComputeTarget(uint64_t base)
{
    int t_ms = EpochPacerEpochMs();
    int rtt = g_epoch_dyn_net_rtt_ms > 0 ? g_epoch_dyn_net_rtt_ms : t_ms;
    int rtt_epochs = (rtt + t_ms - 1) / t_ms;
    if (rtt_epochs < 1) {
        rtt_epochs = 1;
    }
    int k = g_epoch_dyn_net_catchup_k > 0 ? g_epoch_dyn_net_catchup_k : 0;
    uint64_t anchor = base;
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    uint64_t peer = EpochPacerMaxPeerEpoch();
    if (logical > anchor) {
        anchor = logical;
    }
    if (physical > anchor) {
        anchor = physical;
    }
    if (peer > anchor) {
        anchor = peer;
    }
    uint64_t target = anchor + static_cast<uint64_t>(rtt_epochs + k);
    if (base > 0 && target < base + 1) {
        target = base + 1;
    }
    if (base > 0 && target > base + EPOCH_PACER_MAX_DELTA) {
        target = base + EPOCH_PACER_MAX_DELTA;
    }
    return target;
}

/* 整批改记到后面的 epoch。持续受限时不放回当前 epoch。 */
static void EpochPacerShiftOffLocked(
    uint64_t src,
    std::vector<EpochPacerPending>* items)
{
    if (items->empty()) {
        return;
    }
    uint32_t self = EpochGetNodeId();
    uint64_t target = EpochPacerComputeTarget(src);
    size_t moved = 0;
    std::vector<EpochPacerPending> failed;
    for (auto& item : *items) {
        E_WireTxn txn;
        if (!E_WireDeserializeTxn(item.wire.data(), item.wire.size(), &txn)) {
            failed.push_back(std::move(item));
            continue;
        }
        txn.epoch = target;
        std::string rewritten;
        if (!E_WireSerializeTxn(txn, &rewritten)) {
            failed.push_back(std::move(item));
            continue;
        }
        (void)E_NodeManager::DecLocalPackTxnCounters(src, self);
        (void)E_NodeManager::DecChangeSetTxnCounters(src, self);
        (void)E_NodeManager::IncLocalPackTxnCounters(target, self);
        (void)E_NodeManager::IncChangeSetTxnCounters(target, self);
        EpochPacerPending next;
        next.wire = std::move(rewritten);
        next.moves = item.moves + 1;
        if (next.moves > EPOCH_PACER_MAX_MOVES) {
            next.moves = 1;
        }
        g_pacer_scheduled[target].push_back(std::move(next));
        moved++;
    }
    if (!failed.empty()) {
        auto& slot = g_pacer_pending[src];
        slot.insert(slot.end(), std::make_move_iterator(failed.begin()), std::make_move_iterator(failed.end()));
    }
    if (moved > 0) {
        ereport(LOG,
            (errmsg("epoch pacer DBR: moved %zu local write sets from epoch %lu to %lu",
                moved,
                src,
                target)));
    }
}

/* 慢节点关窗：还在受限就把本 epoch 的本地写集全部改记走，然后发 EOF。 */
static void EpochPacerDispatchEpochLocked(
    uint64_t epoch,
    bool congested,
    std::vector<std::pair<uint64_t, EpochPacerPending>>* release)
{
    if (EpochPacerEpochSealed(epoch) || !congested) {
        EpochPacerTakeEpoch(&g_pacer_pending, epoch, release);
        EpochPacerTakeEpoch(&g_pacer_scheduled, epoch, release);
        return;
    }
    std::vector<EpochPacerPending> pending;
    std::vector<EpochPacerPending> scheduled;
    auto pit = g_pacer_pending.find(epoch);
    if (pit != g_pacer_pending.end()) {
        pending.swap(pit->second);
        g_pacer_pending.erase(pit);
    }
    auto sit = g_pacer_scheduled.find(epoch);
    if (sit != g_pacer_scheduled.end()) {
        scheduled.swap(sit->second);
        g_pacer_scheduled.erase(sit);
    }
    EpochPacerShiftOffLocked(epoch, &pending);
    EpochPacerShiftOffLocked(epoch, &scheduled);
}

void EpochPacerBeforeSeal(uint64_t epoch)
{
    if (!EpochPacerDbrEnabled() || epoch == 0 || !EpochPacerIsSlowSelf()) {
        return;
    }
    bool congested = EpochPacerCongestionLatched();
    std::vector<std::pair<uint64_t, EpochPacerPending>> release;
    {
        std::lock_guard<std::mutex> lock(g_pacer_mu);
        EpochPacerDispatchEpochLocked(epoch, congested, &release);
    }
    EpochPacerAdmitList(release);
}

static void EpochPacerPublishPaActivate(uint32_t self, uint32_t proxy, uint32_t session_id, uint64_t e_f,
                                        uint64_t e_t)
{
    E_WireTxn msg;
    msg.csn = EPOCH_WIRE_PA_ACTIVATE_CSN;
    msg.epoch = e_f;
    msg.server_id = self;
    msg.route_wid = static_cast<uint16_t>(proxy);
    msg.txn_state = EPOCH_TXN_EMPTY;
    E_WireRow row;
    row.expected_csn = e_t;
    row.relid = session_id;
    msg.rows.push_back(row);
    std::string payload;
    if (!E_WireSerializeTxn(msg, &payload)) {
        return;
    }
    (void)EpochNetPublish(payload);
    ereport(LOG,
        (errmsg("epoch pacer PA: slow=%u proxy=%u session=%u range=[%lu,%lu]",
            self,
            proxy,
            session_id,
            e_f,
            e_t)));
}

void EpochPacerPackTick(void)
{
    if (!g_pacer_logged && (EpochPacerDbrEnabled() || EpochPacerPaEnabled())) {
        g_pacer_logged = true;
        ereport(LOG,
            (errmsg("epoch pacer: DBR=%d PA=%d queue_tolerance=%d k=%d rtt_ms=%d",
                EpochPacerDbrEnabled() ? 1 : 0,
                EpochPacerPaEnabled() ? 1 : 0,
                g_epoch_dyn_net_queue_tolerance,
                g_epoch_dyn_net_catchup_k,
                g_epoch_dyn_net_rtt_ms)));
    }

    if (EpochPacerDbrEnabled()) {
        /* 积压只在关窗时按预算放回。这里整表放行会把一个 epoch 撑到上千笔。 */
        (void)EpochPacerCongestionLatched();
    }

    if (!EpochPacerPaEnabled()) {
        return;
    }
    uint32_t self = EpochGetNodeId();
    if (!EpochPacerIdInSlowList(self)) {
        return;
    }

    /*
     * 下行受限时，慢节点收到的是所有健康副本的写集之和。
     * 至少要有两个健康副本，proxy 才能把多路收成一路。两个节点时这一路本来就只有一个发送方。
     */
    uint32_t total = EpochGetTotalNodes();
    uint32_t healthy = 0;
    uint32_t proxy = total;
    uint64_t best_epoch = 0;
    bool have = false;
    for (uint32_t peer = 0; peer < total; ++peer) {
        if (peer == self || EpochPacerIdInSlowList(peer)) {
            continue;
        }
        healthy++;
        uint64_t sealed = EpochGetPeerMaxSealedEpoch(peer);
        if (!have || sealed > best_epoch || (sealed == best_epoch && peer < proxy)) {
            proxy = peer;
            best_epoch = sealed;
            have = true;
        }
    }
    if (healthy < 2 || !have || proxy >= total) {
        if (!g_pacer_pa_short_logged) {
            g_pacer_pa_short_logged = true;
            ereport(LOG,
                (errmsg("epoch pacer PA: need 3 replicas so two healthy senders can share one proxy. "
                        "total=%u healthy=%u, keep direct send",
                    total,
                    healthy)));
        }
        return;
    }

    uint64_t e_f = E_NodeManager::GetLogicalEpoch();
    if (e_f == 0) {
        e_f = E_NodeManager::GetPhysicalEpoch();
    }
    if (e_f == 0) {
        return;
    }
    uint64_t want_to = e_f + EPOCH_PACER_PA_WINDOW - 1;

    uint32_t session_id = 0;
    uint32_t use_proxy = proxy;
    uint64_t use_from = e_f;
    uint64_t use_to = want_to;
    {
        std::lock_guard<std::mutex> lock(g_pacer_session_mu);
        if (g_pacer_session.active && g_pacer_session.slow_id == self &&
            g_pacer_session.epoch_to >= e_f + EPOCH_PACER_PA_REFRESH_LEAD) {
            return;
        }
        if (g_pacer_session.active && g_pacer_session.slow_id == self) {
            session_id = g_pacer_session.session_id;
            use_proxy = g_pacer_session.proxy_id;
            use_from = g_pacer_session.epoch_from;
            g_pacer_session.epoch_to = want_to;
            use_to = want_to;
        } else {
            session_id = g_pacer_next_session++;
            g_pacer_session.active = true;
            g_pacer_session.slow_id = self;
            g_pacer_session.proxy_id = proxy;
            g_pacer_session.session_id = session_id;
            g_pacer_session.epoch_from = e_f;
            g_pacer_session.epoch_to = want_to;
            use_proxy = proxy;
            use_from = e_f;
            use_to = want_to;
        }
    }
    EpochPacerPublishPaActivate(self, use_proxy, session_id, use_from, use_to);
}

bool EpochPacerRouteSend(const std::string& wire, const E_WireTxn& txn)
{
    if (!EpochPacerPaEnabled() || wire.empty() || txn.epoch == 0 || EpochGetTotalNodes() < 3) {
        return false;
    }
    EpochPacerSession session;
    {
        std::lock_guard<std::mutex> lock(g_pacer_session_mu);
        if (!g_pacer_session.active) {
            return false;
        }
        session = g_pacer_session;
    }
    if (txn.epoch < session.epoch_from || txn.epoch > session.epoch_to) {
        return false;
    }
    uint32_t self = EpochGetNodeId();
    if (self == session.slow_id) {
        return false;
    }

    uint32_t total = EpochGetTotalNodes();
    bool ok = true;
    for (uint32_t peer = 0; peer < total; ++peer) {
        if (peer == self || peer == session.slow_id) {
            continue;
        }
        if (!EpochNetPublishTo(peer, wire)) {
            ok = false;
        }
    }
    if (self == session.proxy_id) {
        if (!EpochNetPublishTo(session.slow_id, wire)) {
            ok = false;
        }
    }
    (void)ok;
    return true;
}

bool EpochPacerOnIncoming(const E_WireTxn& txn, const std::string& wire)
{
    if (E_WireTxnIsPaActivate(txn)) {
        uint64_t e_t = txn.epoch;
        uint32_t session_id = 0;
        if (!txn.rows.empty()) {
            if (txn.rows[0].expected_csn > e_t) {
                e_t = txn.rows[0].expected_csn;
            }
            session_id = txn.rows[0].relid;
        }
        std::lock_guard<std::mutex> lock(g_pacer_session_mu);
        if (!g_pacer_session.active || session_id > g_pacer_session.session_id) {
            g_pacer_session.active = true;
            g_pacer_session.slow_id = txn.server_id;
            g_pacer_session.proxy_id = txn.route_wid;
            g_pacer_session.session_id = session_id;
            g_pacer_session.epoch_from = txn.epoch;
            g_pacer_session.epoch_to = e_t;
        } else if (session_id == g_pacer_session.session_id && e_t > g_pacer_session.epoch_to) {
            g_pacer_session.epoch_to = e_t;
        }
        return true;
    }

    if (!EpochPacerPaEnabled() || wire.empty() || txn.epoch == 0) {
        return false;
    }
    EpochPacerSession session;
    {
        std::lock_guard<std::mutex> lock(g_pacer_session_mu);
        if (!g_pacer_session.active) {
            return false;
        }
        session = g_pacer_session;
    }
    uint32_t self = EpochGetNodeId();
    if (self != session.proxy_id) {
        return false;
    }
    if (txn.epoch < session.epoch_from || txn.epoch > session.epoch_to) {
        return false;
    }
    if (txn.server_id == self || txn.server_id == session.slow_id) {
        return false;
    }
    (void)EpochNetPublishTo(session.slow_id, wire);
    return false;
}
