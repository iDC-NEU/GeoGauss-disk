#include "postgres.h"

#include <mutex>
#include <cstdarg>

#include "epoch/e_recv_ledger.h"
#include "epoch/e_perf.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_config.h"
#include "epoch/e_log_path.h"
#include "epoch/e_shmem.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_statistician.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_types.h"

#include "miscadmin.h"

#include <atomic>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <sys/time.h>

static uint64_t g_last_perf_report_physical = 0;

/* logical stall 细因：进程内累计（epoch worker 同进程多线程可见），不改 shmem */
enum EpochStallDetailIdx : int {
    STALL_D_PHYS = 0,
    STALL_D_LOCAL_CMT,
    STALL_D_SEND,
    STALL_D_MERGE_PEER_END,
    STALL_D_MERGE_PEER_TXN,
    STALL_D_MERGE_SELF_SEND,
    STALL_D_MERGE_ASYNC,
    STALL_D_MERGE_COUNT,
    STALL_D_MERGE_OTHER,
    STALL_D_APPLY_ASYNC_MERGE,
    STALL_D_APPLY_ASYNC_APPLY,
    STALL_D_APPLY_POOL,
    STALL_D_APPLY_UNAPPLIED,
    STALL_D_APPLY_REMOTE_PENDING,
    STALL_D_APPLY_COMMIT_SHORT,
    STALL_D_APPLY_FLAG,
    STALL_D_APPLY_OTHER,
    STALL_D_UNKNOWN,
    STALL_D_N
};

static const char* g_stall_detail_names[STALL_D_N] = {
    "phys",
    "local_cmt",
    "send",
    "m_peer_end",
    "m_peer_txn",
    "m_self_send",
    "m_async",
    "m_count",
    "m_other",
    "a_async_merge",
    "a_async_apply",
    "a_pool",
    "a_unapplied",
    "a_remote",
    "a_commit_short",
    "a_flag",
    "a_other",
    "unknown",
};

static std::atomic<uint64_t> g_stall_detail_count[STALL_D_N];
static std::atomic<uint64_t> g_stall_detail_us[STALL_D_N];
static std::atomic<int> g_stall_last_idx{-1};
static std::atomic<uint64_t> g_stall_last_us{0};

static uint64_t EpochPerfWallUsNow(void)
{
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000ULL + static_cast<uint64_t>(tv.tv_usec);
}

static int EpochPerfStallDetailIndex(const char* reason)
{
    if (reason == nullptr) {
        return STALL_D_UNKNOWN;
    }
    if (strcmp(reason, "physical_not_ready") == 0) {
        return STALL_D_PHYS;
    }
    if (strcmp(reason, "local_commit_pending") == 0) {
        return STALL_D_LOCAL_CMT;
    }
    if (strcmp(reason, "send_incomplete") == 0) {
        return STALL_D_SEND;
    }
    if (strcmp(reason, "merge_peer_end") == 0) {
        return STALL_D_MERGE_PEER_END;
    }
    if (strcmp(reason, "merge_peer_txn") == 0) {
        return STALL_D_MERGE_PEER_TXN;
    }
    if (strcmp(reason, "merge_self_send") == 0) {
        return STALL_D_MERGE_SELF_SEND;
    }
    if (strcmp(reason, "merge_async") == 0) {
        return STALL_D_MERGE_ASYNC;
    }
    if (strcmp(reason, "merge_count") == 0) {
        return STALL_D_MERGE_COUNT;
    }
    if (strcmp(reason, "merge_incomplete") == 0) {
        return STALL_D_MERGE_OTHER;
    }
    if (strcmp(reason, "apply_async_merge") == 0) {
        return STALL_D_APPLY_ASYNC_MERGE;
    }
    if (strcmp(reason, "apply_async_apply") == 0) {
        return STALL_D_APPLY_ASYNC_APPLY;
    }
    if (strcmp(reason, "apply_pool") == 0) {
        return STALL_D_APPLY_POOL;
    }
    if (strcmp(reason, "apply_unapplied") == 0) {
        return STALL_D_APPLY_UNAPPLIED;
    }
    if (strcmp(reason, "apply_remote_pending") == 0) {
        return STALL_D_APPLY_REMOTE_PENDING;
    }
    if (strcmp(reason, "apply_commit_short") == 0) {
        return STALL_D_APPLY_COMMIT_SHORT;
    }
    if (strcmp(reason, "apply_flag") == 0) {
        return STALL_D_APPLY_FLAG;
    }
    if (strcmp(reason, "apply_incomplete") == 0) {
        return STALL_D_APPLY_OTHER;
    }
    return STALL_D_UNKNOWN;
}

static void EpochPerfNoteStallDetail(const char* reason)
{
    const int idx = EpochPerfStallDetailIndex(reason);
    g_stall_detail_count[idx].fetch_add(1, std::memory_order_relaxed);

    const uint64_t now = EpochPerfWallUsNow();
    const int prev = g_stall_last_idx.exchange(idx, std::memory_order_acq_rel);
    const uint64_t last = g_stall_last_us.exchange(now, std::memory_order_acq_rel);
    if (prev >= 0 && prev < STALL_D_N && last > 0 && now >= last) {
        const uint64_t delta = now - last;
        /* 单次间隔过大视为 idle，不计入，避免 skew */
        if (delta > 0 && delta < 500000ULL) {
            g_stall_detail_us[prev].fetch_add(delta, std::memory_order_relaxed);
        }
    }
}

static EpochSharedState::EpochPerfCounters* EpochPerfCounters(void)
{
    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr) {
        return nullptr;
    }
    return &state->perf;
}

static void EpochPerfZeroCounters(EpochSharedState::EpochPerfCounters* perf)
{
    if (perf == nullptr) {
        return;
    }
    for (uint32_t i = 0; i < EPOCH_PERF_GATE_COUNT; ++i) {
        perf->gate_wait_us[i].store(0, std::memory_order_relaxed);
        perf->gate_wait_count[i].store(0, std::memory_order_relaxed);
        perf->gate_spin_count[i].store(0, std::memory_order_relaxed);
    }
    perf->zmq_wire_txn_send_count.store(0, std::memory_order_relaxed);
    perf->zmq_wire_txn_send_bytes.store(0, std::memory_order_relaxed);
    perf->zmq_wire_row_bytes.store(0, std::memory_order_relaxed);
    perf->zmq_wire_row_count.store(0, std::memory_order_relaxed);
    perf->zmq_epoch_end_send_count.store(0, std::memory_order_relaxed);
    perf->zmq_epoch_end_send_bytes.store(0, std::memory_order_relaxed);
    perf->zmq_hello_send_count.store(0, std::memory_order_relaxed);
    perf->zmq_txn_req_send_count.store(0, std::memory_order_relaxed);
    perf->zmq_wire_recv_count.store(0, std::memory_order_relaxed);
    perf->zmq_wire_recv_bytes.store(0, std::memory_order_relaxed);
    perf->zmq_epoch_end_recv_count.store(0, std::memory_order_relaxed);
    perf->logical_advance_ok.store(0, std::memory_order_relaxed);
    perf->logical_advance_try.store(0, std::memory_order_relaxed);
    perf->stall_peer_not_connected.store(0, std::memory_order_relaxed);
    perf->stall_peer_lag.store(0, std::memory_order_relaxed);
    perf->stall_physical_not_ready.store(0, std::memory_order_relaxed);
    perf->stall_local_commit_pending.store(0, std::memory_order_relaxed);
    perf->stall_seal_pending.store(0, std::memory_order_relaxed);
    perf->stall_send_incomplete.store(0, std::memory_order_relaxed);
    perf->stall_merge_incomplete.store(0, std::memory_order_relaxed);
    perf->stall_apply_incomplete.store(0, std::memory_order_relaxed);
    perf->seal_block_physical_delay.store(0, std::memory_order_relaxed);
    perf->seal_block_peer_lag.store(0, std::memory_order_relaxed);
    perf->seal_block_exec_pending.store(0, std::memory_order_relaxed);
    perf->seal_block_change_set_pending.store(0, std::memory_order_relaxed);
    perf->seal_block_peer_txn_pending.store(0, std::memory_order_relaxed);
    perf->seal_ok_count.store(0, std::memory_order_relaxed);
    perf->worker_poll_count.store(0, std::memory_order_relaxed);
    perf->physical_advance_count.store(0, std::memory_order_relaxed);
    perf->apply_commit_txn.store(0, std::memory_order_relaxed);
    perf->apply_abort_txn.store(0, std::memory_order_relaxed);
    perf->merge_heap_abort.store(0, std::memory_order_relaxed);
}

bool EpochPerfIsEnabled(void)
{
    return g_enable_epoch_multimaster && g_epoch_perf_log;
}

void EpochPerfReset(void)
{
    EpochPerfZeroCounters(EpochPerfCounters());
    g_last_perf_report_physical = 0;
}

void EpochPerfAddGateWait(EpochPerfGateKind kind, uint64_t wait_us, uint64_t spin_count)
{
    if (!EpochPerfIsEnabled() || kind >= EPOCH_PERF_GATE_COUNT) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf == nullptr) {
        return;
    }
    perf->gate_wait_us[kind].fetch_add(wait_us, std::memory_order_relaxed);
    perf->gate_wait_count[kind].fetch_add(1, std::memory_order_relaxed);
    perf->gate_spin_count[kind].fetch_add(spin_count, std::memory_order_relaxed);
}

void EpochPerfNoteLogicalAdvanceOk(void)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf != nullptr) {
        perf->logical_advance_ok.fetch_add(1, std::memory_order_relaxed);
    }
}

void EpochPerfNoteLogicalAdvanceTry(void)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf != nullptr) {
        perf->logical_advance_try.fetch_add(1, std::memory_order_relaxed);
    }
}

static void EpochPerfBump(std::atomic<uint64_t>* counter)
{
    if (counter != nullptr) {
        counter->fetch_add(1, std::memory_order_relaxed);
    }
}

void EpochPerfNoteLogicalStall(const char* reason)
{
    if (!EpochPerfIsEnabled() || reason == nullptr) {
        return;
    }
    EpochPerfNoteStallDetail(reason);

    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf == nullptr) {
        return;
    }
    if (strcmp(reason, "peer_not_connected") == 0) {
        EpochPerfBump(&perf->stall_peer_not_connected);
    } else if (strcmp(reason, "peer_lag") == 0) {
        EpochPerfBump(&perf->stall_peer_lag);
    } else if (strcmp(reason, "physical_not_ready") == 0) {
        EpochPerfBump(&perf->stall_physical_not_ready);
    } else if (strcmp(reason, "local_commit_pending") == 0) {
        EpochPerfBump(&perf->stall_local_commit_pending);
    } else if (strcmp(reason, "seal_pending") == 0) {
        EpochPerfBump(&perf->stall_seal_pending);
    } else if (strcmp(reason, "send_incomplete") == 0) {
        EpochPerfBump(&perf->stall_send_incomplete);
    } else if (strncmp(reason, "merge_", 6) == 0 || strcmp(reason, "merge_incomplete") == 0) {
        EpochPerfBump(&perf->stall_merge_incomplete);
    } else if (strncmp(reason, "apply_", 6) == 0 || strcmp(reason, "apply_incomplete") == 0) {
        EpochPerfBump(&perf->stall_apply_incomplete);
    }
}

void EpochPerfNoteSealBlock(const char* reason)
{
    if (!EpochPerfIsEnabled() || reason == nullptr) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf == nullptr) {
        return;
    }
    if (strcmp(reason, "physical_delay") == 0) {
        EpochPerfBump(&perf->seal_block_physical_delay);
    } else if (strcmp(reason, "peer_lag") == 0) {
        EpochPerfBump(&perf->seal_block_peer_lag);
    } else if (strcmp(reason, "exec_pending") == 0) {
        EpochPerfBump(&perf->seal_block_exec_pending);
    } else if (strcmp(reason, "change_set_pending") == 0) {
        EpochPerfBump(&perf->seal_block_change_set_pending);
    } else if (strcmp(reason, "peer_txn_pending") == 0) {
        EpochPerfBump(&perf->seal_block_peer_txn_pending);
    }
}

void EpochPerfNoteSealOk(void)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    EpochPerfBump(perf != nullptr ? &perf->seal_ok_count : nullptr);
}

void EpochPerfNoteWorkerPoll(void)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    EpochPerfBump(perf != nullptr ? &perf->worker_poll_count : nullptr);
}

void EpochPerfNotePhysicalAdvance(void)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    EpochPerfBump(perf != nullptr ? &perf->physical_advance_count : nullptr);
}

static uint64_t EpochPerfSumWireRowBytes(const E_WireTxn& txn)
{
    uint64_t sum = 0;
    for (const auto& row : txn.rows) {
        sum += static_cast<uint64_t>(row.tuple_data.size());
    }
    return sum;
}

void EpochPerfNoteWireTxnSend(const E_WireTxn& txn, uint64_t wire_bytes)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf == nullptr) {
        return;
    }
    perf->zmq_wire_txn_send_count.fetch_add(1, std::memory_order_relaxed);
    perf->zmq_wire_txn_send_bytes.fetch_add(wire_bytes, std::memory_order_relaxed);
    perf->zmq_wire_row_bytes.fetch_add(EpochPerfSumWireRowBytes(txn), std::memory_order_relaxed);
    perf->zmq_wire_row_count.fetch_add(static_cast<uint64_t>(txn.rows.size()), std::memory_order_relaxed);
}

void EpochPerfNoteEpochEndSend(uint64_t wire_bytes)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf == nullptr) {
        return;
    }
    perf->zmq_epoch_end_send_count.fetch_add(1, std::memory_order_relaxed);
    perf->zmq_epoch_end_send_bytes.fetch_add(wire_bytes, std::memory_order_relaxed);
}

void EpochPerfNoteWireRecv(const E_WireTxn& txn, uint64_t wire_bytes)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf == nullptr) {
        return;
    }
    perf->zmq_wire_recv_bytes.fetch_add(wire_bytes, std::memory_order_relaxed);
    if (E_WireTxnIsEpochEnd(txn)) {
        perf->zmq_epoch_end_recv_count.fetch_add(1, std::memory_order_relaxed);
    } else {
        perf->zmq_wire_recv_count.fetch_add(1, std::memory_order_relaxed);
    }
}

void EpochPerfNoteHelloSend(void)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    EpochPerfBump(perf != nullptr ? &perf->zmq_hello_send_count : nullptr);
}

void EpochPerfNoteTxnRequestSend(void)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    EpochPerfBump(perf != nullptr ? &perf->zmq_txn_req_send_count : nullptr);
}

void EpochPerfNoteApplyCommitTxn(void)
{
    if (!g_enable_epoch_multimaster) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    EpochPerfBump(perf != nullptr ? &perf->apply_commit_txn : nullptr);
}

void EpochPerfNoteApplyAbortTxn(void)
{
    EpochPerfAddApplyAbortTxn(1);
}

/*
 * 一次加 n 笔 abort（时间片推进时把该片的全部 merge 输家一起计入）。
 * 用 fetch_add 而不是循环 Bump：并发写回线程同时调用也不会少算。
 */
void EpochPerfAddApplyAbortTxn(uint64_t n)
{
    if (!g_enable_epoch_multimaster || n == 0) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf != nullptr) {
        perf->apply_abort_txn.fetch_add(n, std::memory_order_relaxed);
    }
}

void EpochPerfNoteMergeHeapAbort(void)
{
    if (!g_enable_epoch_multimaster) {
        return;
    }
    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    EpochPerfBump(perf != nullptr ? &perf->merge_heap_abort : nullptr);
}

static const char* EpochPerfGateName(EpochPerfGateKind kind)
{
    switch (kind) {
        case EPOCH_PERF_GATE_START:
            return "start_gate";
        case EPOCH_PERF_GATE_LOGICAL_COMMIT:
            return "logical_commit_wait";
        case EPOCH_PERF_GATE_PACK_ADMIT:
            return "pack_admit_wait";
        default:
            return "unknown_gate";
    }
}

static void EpochPerfAppendLine(FILE* fp, const char* line)
{
    if (fp == nullptr || line == nullptr) {
        return;
    }
    fputs(line, fp);
    fputc('\n', fp);
}

/*
 * 按 epoch 粒度的粗延迟统计（论文里只需要量级，不追求单事务精确值）。
 *
 *   seal 时刻：physical 从 e 推进到 e+1 的那一刻（e 这一片的事务全部产生完毕）
 *   apply 时刻：apply 游标从 e 推进到 e+1 的那一刻（e 这一片写回完成）
 *   延迟 = apply - seal，即"一片从封口到写回完成"的时间
 *
 * 统计范围：第 10 ~ 110 片（避开启动抖动，也不受后期积压/收尾影响）。
 */
static std::atomic<uint64_t> g_epoch_seal_us[EPOCH_TXN_EPOCH_COUNT];

/*
 * 延迟统计区间：**去掉首尾各 10 片**
 *   - 跳过前 10 片（启动抖动：客户端刚连、jTPCC ramp-up）
 *   - 动态排除最近 10 片（收尾：客户端停止、最后几片可能填不满）
 */
static constexpr uint64_t EPOCH_LAT_SKIP_HEAD = 10;
static constexpr uint64_t EPOCH_LAT_SKIP_TAIL = 10;

/* 最近 10 片的延迟（用于排除收尾），以及它们的和 */
static std::mutex g_lat_tail_mu;
static uint64_t g_lat_tail[EPOCH_LAT_SKIP_TAIL] = {0};
static uint64_t g_lat_tail_sum_us = 0;
static uint64_t g_lat_tail_pos = 0;
static std::atomic<uint64_t> g_epoch_lat_sum_us{0};
static std::atomic<uint64_t> g_epoch_lat_cnt{0};

static inline uint64_t EpochNowUs(void)
{
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000ULL + static_cast<uint64_t>(tv.tv_usec);
}

void EpochPerfNoteEpochSeal(uint64_t epoch)
{
    if (epoch == 0) {
        return;
    }
    g_epoch_seal_us[EpochSlotOf(epoch)].store(EpochNowUs(), std::memory_order_relaxed);
}

void EpochPerfNoteEpochApplied(uint64_t epoch)
{
    if (epoch == 0) {
        return;
    }
    const uint64_t seal = g_epoch_seal_us[EpochSlotOf(epoch)].load(std::memory_order_relaxed);
    if (seal == 0) {
        return;
    }
    const uint64_t now = EpochNowUs();
    if (now <= seal) {
        return;
    }
    if (epoch <= EPOCH_LAT_SKIP_HEAD) {
        return;                        /* 去掉头部 10 片（启动抖动） */
    }

    const uint64_t lat_us = now - seal;
    g_epoch_lat_sum_us.fetch_add(lat_us, std::memory_order_relaxed);
    g_epoch_lat_cnt.fetch_add(1, std::memory_order_relaxed);

    /* 记录最近 10 片，算平均时扣掉（去掉尾部） */
    {
        std::lock_guard<std::mutex> lk(g_lat_tail_mu);
        const uint32_t idx = static_cast<uint32_t>(g_lat_tail_pos % EPOCH_LAT_SKIP_TAIL);
        g_lat_tail_sum_us = g_lat_tail_sum_us - g_lat_tail[idx] + lat_us;
        g_lat_tail[idx] = lat_us;
        ++g_lat_tail_pos;
    }
}

/* 快照闸门因超时放行的次数（一致性让路指标） */
static std::atomic<uint64_t> g_snap_timeout_cnt{0};

void EpochPerfNoteSnapTimeout(void)
{
    g_snap_timeout_cnt.fetch_add(1, std::memory_order_relaxed);
}

uint64_t EpochPerfSnapTimeoutCount(void)
{
    return g_snap_timeout_cnt.load(std::memory_order_relaxed);
}

/* 第 10~110 片的平均延迟（微秒）：封口 → 写回完成 */
uint64_t EpochPerfEpochLatAvgUs(void)
{
    const uint64_t cnt = g_epoch_lat_cnt.load(std::memory_order_relaxed);
    if (cnt == 0) {
        return 0;
    }
    uint64_t sum = g_epoch_lat_sum_us.load(std::memory_order_relaxed);

    /* 去掉尾部：把最近 10 片（不足 10 片时按实有）从分子分母里扣掉 */
    uint64_t tail_sum = 0;
    uint64_t tail_cnt = 0;
    {
        std::lock_guard<std::mutex> lk(g_lat_tail_mu);
        tail_cnt = (g_lat_tail_pos < EPOCH_LAT_SKIP_TAIL) ? g_lat_tail_pos : EPOCH_LAT_SKIP_TAIL;
        tail_sum = g_lat_tail_sum_us;
    }
    if (cnt <= tail_cnt) {
        return (sum / cnt);
    }
    return ((sum - tail_sum) / (cnt - tail_cnt));
}
void EpochPerfMaybeReport(void)
{
    if (!EpochPerfIsEnabled()) {
        return;
    }

    /*
     * 落盘条件：每推进 50 个时间片，或者距上次已满 1 秒。
     *
     * 只按时间片推进计数（原来唯一条件）时，60 秒的压测只有十来条快照，
     * 最后一条可能停在压测结束前好几秒；而 collect 是在压测结束后才读的，
     * 采样点越稀，60 秒窗口的误差越大。补一条“每秒至少一次”之后，
     * 冻结时刻与压测结束时刻的偏差不超过 1 秒。
     */
    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    static uint64_t last_report_us = 0;
    struct timeval perf_tv;
    gettimeofday(&perf_tv, nullptr);
    const uint64_t perf_now_us = static_cast<uint64_t>(perf_tv.tv_sec) * 1000000ULL +
                                 static_cast<uint64_t>(perf_tv.tv_usec);
    const bool by_physical = (g_last_perf_report_physical != 0 &&
                              physical > g_last_perf_report_physical + 49);
    const bool by_time = (last_report_us == 0 ||
                          (perf_now_us - last_report_us) >= 1000000ULL);
    if (!by_physical && !by_time) {
        return;
    }
    g_last_perf_report_physical = physical;
    last_report_us = perf_now_us;

    EpochSharedState::EpochPerfCounters* perf = EpochPerfCounters();
    if (perf == nullptr) {
        return;
    }

    char path[MAXPGPATH];
    if (!EpochFormatLogPath(path, sizeof(path), "epoch_perf")) {
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

    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t gap_pl = (physical >= logical) ? (physical - logical) : 0;
    uint32_t self = EpochGetNodeId();
    uint64_t total_gate_us = 0;
    for (uint32_t i = 0; i < EPOCH_PERF_GATE_COUNT; ++i) {
        total_gate_us += perf->gate_wait_us[i].load(std::memory_order_relaxed);
    }

    char buf[512];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "[%s] node=%u profile_gate_total_us=%lu physical=%lu logical=%lu gap=%lu",
        ts,
        self,
        total_gate_us,
        physical,
        logical,
        gap_pl);
    securec_check_ss(rc, "\0", "\0");
    EpochPerfAppendLine(fp, buf);

    for (uint32_t i = 0; i < EPOCH_PERF_GATE_COUNT; ++i) {
        uint64_t wait_us = perf->gate_wait_us[i].load(std::memory_order_relaxed);
        uint64_t waits = perf->gate_wait_count[i].load(std::memory_order_relaxed);
        uint64_t spins = perf->gate_spin_count[i].load(std::memory_order_relaxed);
        if (wait_us == 0 && waits == 0) {
            continue;
        }
        rc = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            "  GATE %s wait_us=%lu events=%lu spin_loops=%lu avg_us=%.1f",
            EpochPerfGateName(static_cast<EpochPerfGateKind>(i)),
            wait_us,
            waits,
            spins,
            waits > 0 ? (static_cast<double>(wait_us) / static_cast<double>(waits)) : 0.0);
        securec_check_ss(rc, "\0", "\0");
        EpochPerfAppendLine(fp, buf);
    }

    uint64_t wire_txn = perf->zmq_wire_txn_send_count.load(std::memory_order_relaxed);
    uint64_t wire_bytes = perf->zmq_wire_txn_send_bytes.load(std::memory_order_relaxed);
    uint64_t row_bytes = perf->zmq_wire_row_bytes.load(std::memory_order_relaxed);
    uint64_t row_count = perf->zmq_wire_row_count.load(std::memory_order_relaxed);
    rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "  ZMQ_SEND wire_txn=%lu wire_bytes=%lu row_bytes=%lu row_count=%lu avg_txn_bytes=%.1f avg_row_bytes=%.1f "
        "epoch_end=%lu epoch_end_bytes=%lu hello=%lu txn_req=%lu",
        wire_txn,
        wire_bytes,
        row_bytes,
        row_count,
        wire_txn > 0 ? (static_cast<double>(wire_bytes) / static_cast<double>(wire_txn)) : 0.0,
        row_count > 0 ? (static_cast<double>(row_bytes) / static_cast<double>(row_count)) : 0.0,
        perf->zmq_epoch_end_send_count.load(std::memory_order_relaxed),
        perf->zmq_epoch_end_send_bytes.load(std::memory_order_relaxed),
        perf->zmq_hello_send_count.load(std::memory_order_relaxed),
        perf->zmq_txn_req_send_count.load(std::memory_order_relaxed));
    securec_check_ss(rc, "\0", "\0");
    EpochPerfAppendLine(fp, buf);

    rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "  ZMQ_RECV wire_txn=%lu wire_bytes=%lu epoch_end=%lu",
        perf->zmq_wire_recv_count.load(std::memory_order_relaxed),
        perf->zmq_wire_recv_bytes.load(std::memory_order_relaxed),
        perf->zmq_epoch_end_recv_count.load(std::memory_order_relaxed));
    securec_check_ss(rc, "\0", "\0");
    EpochPerfAppendLine(fp, buf);

    uint64_t adv_try = perf->logical_advance_try.load(std::memory_order_relaxed);
    uint64_t adv_ok = perf->logical_advance_ok.load(std::memory_order_relaxed);
    rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "  LOGICAL advance_try=%lu advance_ok=%lu ok_rate=%.4f worker_poll=%lu physical_advance=%lu seal_ok=%lu",
        adv_try,
        adv_ok,
        adv_try > 0 ? (static_cast<double>(adv_ok) / static_cast<double>(adv_try)) : 0.0,
        perf->worker_poll_count.load(std::memory_order_relaxed),
        perf->physical_advance_count.load(std::memory_order_relaxed),
        perf->seal_ok_count.load(std::memory_order_relaxed));
    securec_check_ss(rc, "\0", "\0");
    EpochPerfAppendLine(fp, buf);

    rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "  STALL peer_nc=%lu peer_lag=%lu phys=%lu local_cmt=%lu seal=%lu send=%lu merge=%lu apply=%lu",
        perf->stall_peer_not_connected.load(std::memory_order_relaxed),
        perf->stall_peer_lag.load(std::memory_order_relaxed),
        perf->stall_physical_not_ready.load(std::memory_order_relaxed),
        perf->stall_local_commit_pending.load(std::memory_order_relaxed),
        perf->stall_seal_pending.load(std::memory_order_relaxed),
        perf->stall_send_incomplete.load(std::memory_order_relaxed),
        perf->stall_merge_incomplete.load(std::memory_order_relaxed),
        perf->stall_apply_incomplete.load(std::memory_order_relaxed));
    securec_check_ss(rc, "\0", "\0");
    EpochPerfAppendLine(fp, buf);

    {
        char detail[768];
        int off = 0;
        int n = snprintf_s(detail, sizeof(detail), sizeof(detail) - 1, "  STALL_US");
        if (n > 0) {
            off = n;
        }
        for (int i = 0; i < STALL_D_N && off < static_cast<int>(sizeof(detail)) - 48; ++i) {
            const uint64_t us = g_stall_detail_us[i].load(std::memory_order_relaxed);
            const uint64_t cnt = g_stall_detail_count[i].load(std::memory_order_relaxed);
            if (us == 0 && cnt == 0) {
                continue;
            }
            n = snprintf_s(detail + off,
                sizeof(detail) - static_cast<size_t>(off),
                sizeof(detail) - static_cast<size_t>(off) - 1,
                " %s=%luus/%lu",
                g_stall_detail_names[i],
                us,
                cnt);
            if (n > 0) {
                off += n;
            }
        }
        EpochPerfAppendLine(fp, detail);
    }

    rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "  SEAL_BLOCK phys_delay=%lu peer_lag=%lu exec=%lu chg_set=%lu peer_txn=%lu",
        perf->seal_block_physical_delay.load(std::memory_order_relaxed),
        perf->seal_block_peer_lag.load(std::memory_order_relaxed),
        perf->seal_block_exec_pending.load(std::memory_order_relaxed),
        perf->seal_block_change_set_pending.load(std::memory_order_relaxed),
        perf->seal_block_peer_txn_pending.load(std::memory_order_relaxed));
    securec_check_ss(rc, "\0", "\0");
    EpochPerfAppendLine(fp, buf);

    rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "  APPLY_TXN commit=%lu abort=%lu merge_heap_abort=%lu total=%lu",
        perf->apply_commit_txn.load(std::memory_order_relaxed),
        perf->apply_abort_txn.load(std::memory_order_relaxed),
        perf->merge_heap_abort.load(std::memory_order_relaxed),
        perf->apply_commit_txn.load(std::memory_order_relaxed) +
            perf->apply_abort_txn.load(std::memory_order_relaxed));
    securec_check_ss(rc, "\0", "\0");
    EpochPerfAppendLine(fp, buf);

    uint32_t total = EpochGetTotalNodes();
    for (uint32_t peer = 0; peer < total; ++peer) {
        if (peer == self) {
            continue;
        }
        uint64_t peer_pack = 0;
        uint64_t peer_should = 0;
        uint64_t peer_rcv = 0;
        for (uint64_t epoch = (logical > 8 ? logical - 8 : 1); epoch <= logical; ++epoch) {
            peer_pack += E_NodeManager::GetLocalPackTxnCounters(epoch, peer);
            peer_should += EpochRecvDeclaredCount(epoch, peer);
            peer_rcv += EpochRecvReceivedCount(epoch, peer);
        }
        rc = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            "  PEER peer=%u recent_pack=%lu should_rcv=%lu rcv=%lu peer_max_sealed=%lu",
            peer,
            peer_pack,
            peer_should,
            peer_rcv,
            EpochGetPeerMaxSealedEpoch(peer));
        securec_check_ss(rc, "\0", "\0");
        EpochPerfAppendLine(fp, buf);
    }

    EpochPerfAppendLine(fp, "");
    fclose(fp);
}

const char* EpochDbgTs(void)
{
    static thread_local char buf[32];
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    struct tm tm_buf;
    (void)localtime_r(&tv.tv_sec, &tm_buf);
    snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03d",
        tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec,
        static_cast<int>(tv.tv_usec / 1000));
    return buf;
}

/*
 * 诊断输出：写到数据目录下的独立文件，不进 pg_log。
 *
 * prep 的 reset data 会清空 pg_log，靠它做卡点定位时日志经常已经没了；
 * 独立文件放在数据目录根下，跨轮次保留。
 */
static FILE* g_diag_fp = nullptr;
static std::mutex g_diag_mu;

void EpochDiagWrite(const char* fmt, ...)
{
    std::lock_guard<std::mutex> lock(g_diag_mu);
    if (g_diag_fp == nullptr) {
        char path[MAXPGPATH];
        if (EpochFormatLogPath(path, sizeof(path), "epoch_diag")) {
            g_diag_fp = fopen(path, "a");
        }
        if (g_diag_fp == nullptr) {
            return;
        }
    }
    fprintf(g_diag_fp, "[%s] ", EpochDbgTs());
    va_list ap;
    va_start(ap, fmt);
    vfprintf(g_diag_fp, fmt, ap);
    va_end(ap);
    fflush(g_diag_fp);
}
