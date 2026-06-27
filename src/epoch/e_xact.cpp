#include "postgres.h"

#include "epoch/e_xact.h"
#include "epoch/e_abort_csn.h"
#include "epoch/e_config.h"
#include "epoch/e_epoch.h"
#include "epoch/e_exec_track.h"
#include "epoch/e_net.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_perf.h"
#include "epoch/e_shmem.h"
#include "epoch/e_txn_queue.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_types.h"

#include "occ/o_commit.h"
#include "occ/o_txn.h"
#include "occ/occ_internal.h"
#include "occ/o_csn_manager.h"
#include "occ/occ_stats.h"

#include "access/xact.h"
#include "gs_thread.h"
#include "knl/knl_variable.h"
#include "knl/knl_thread.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "postmaster/autovacuum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include <climits>
#include <cstdint>

static O_CSNManager g_epochCsnManager;

static bool E_TxnHasWriteSet(O_TxnManager* txn)
{
    if (txn == nullptr || txn->m_accessMgr == nullptr) {
        return false;
    }
    return txn->m_accessMgr->m_Size() > 0;
}

static EpochOpType E_MapAccessTypeToWireOp(AccessType type)
{
    switch (type) {
        case INS:
            return EPOCH_OP_INSERT;
        case WR:
            return EPOCH_OP_UPDATE;
        case DEL:
            return EPOCH_OP_DELETE;
        default:
            return EPOCH_OP_READ;
    }
}

static bool E_BuildWireTxn(O_TxnManager* txn, uint64_t csn, E_WireTxn* wire)
{
    if (txn == nullptr || wire == nullptr) {
        return false;
    }
    wire->csn = csn;
    wire->start_epoch = u_sess->epoch_cxt.start_epoch;
    wire->commit_epoch = u_sess->epoch_cxt.commit_epoch;
    wire->server_id = EpochGetNodeId();
    wire->txn_state = EPOCH_TXN_COMMIT;
    wire->rows.clear();

    txn->m_accessMgr->ForEachWriteEntry([&](const occ_sentinel& sent, O_Access* access) {
        if (access == nullptr) {
            return;
        }
        AccessType mapped = access->m_type;
        if (mapped != INS && mapped != WR && mapped != DEL) {
            return;
        }
        HeapTuple row = access->GetTxnRow();
        if (row == nullptr || row->t_data == nullptr) {
            return;
        }
        E_WireRow wire_row;
        wire_row.op_type = E_MapAccessTypeToWireOp(mapped);
        wire_row.relid = OccResolveRelid(sent);
        char* relname = get_rel_name(wire_row.relid);
        if (relname != nullptr) {
            wire_row.relname = relname;
        }
        wire_row.bi_hi = sent.tuple_id.ip_blkid.bi_hi;
        wire_row.bi_lo = sent.tuple_id.ip_blkid.bi_lo;
        wire_row.ip_posid = sent.tuple_id.ip_posid;
        wire_row.tuple_data.resize(row->t_len);
        errno_t rc = memcpy_s(wire_row.tuple_data.data(), wire_row.tuple_data.size(),
                              row->t_data, row->t_len);
        securec_check(rc, "\0", "\0");
        wire->rows.push_back(std::move(wire_row));
    });
    return !wire->rows.empty();
}

static bool E_ShouldTrackTxnEpoch(void)
{
    if (IsAutoVacuumWorkerProcess() || AmEpochWorkerProcess()) {
        return false;
    }
    if (t_thrd.role == EPOCHWORKER) {
        return false;
    }
    /*
     * 仅普通客户端 backend 连到 epoch 跟踪库时参与 epoch gate / 计数。
     * openGauss 中 XACT START 触发面很广，此处过滤后再 assign epoch。
     */
    if (t_thrd.role != WORKER && t_thrd.role != THREADPOOL_WORKER) {
        return false;
    }
    if (!IsNormalProcessingMode()) {
        return false;
    }
    if (u_sess->proc_cxt.MyProcPort == nullptr || u_sess->proc_cxt.MyProcPort->database_name == nullptr) {
        return false;
    }
    const char* track_db = (g_epoch_apply_database != nullptr && g_epoch_apply_database[0] != '\0')
        ? g_epoch_apply_database
        : "test";
    if (strcmp(u_sess->proc_cxt.MyProcPort->database_name, track_db) != 0) {
        return false;
    }
    return true;
}

static bool E_AllRequiredPeersConnected(void)
{
    uint32_t total = EpochGetTotalNodes();
    uint32_t self = EpochGetNodeId();
    for (uint32_t node = 0; node < total; ++node) {
        if (node == self) {
            continue;
        }
        if (!EpochPeerIsConnected(node)) {
            return false;
        }
    }
    return true;
}

/*
 * 仅当 epoch_commit_sync=on 且所有对端均已连通时才进入 sync 路径。
 * total_nodes=2 但 node1 未启动时，应按单节点退化，避免无限等待 remote_* 占满连接。
 */
static bool E_IsSyncEpochMode(void)
{
    return g_epoch_commit_sync && g_enable_epoch_multimaster && EpochGetTotalNodes() > 1 &&
           E_AllRequiredPeersConnected();
}

/*
 * 异步多主：epoch_commit_sync=off，COMMIT 不等待 logical/remote，pack 后立即返回。
 * replication/merge 由 worker 后台完成；tpmC 更高但提交时无跨节点持久化保证。
 */
static bool E_IsAsyncEpochMode(void)
{
    return !g_epoch_commit_sync && g_enable_epoch_multimaster && EpochGetTotalNodes() > 1 &&
           E_AllRequiredPeersConnected();
}

static bool EpochAsyncGapPressure(void)
{
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    uint64_t gap = (physical > logical) ? (physical - logical) : 0;
    return gap >= EpochGetPhysLogicalGapLimit();
}

static uint32_t EpochAsyncQueuePressureThreshold(void)
{
    return (EPOCH_TXN_QUEUE_CAPACITY * 3) / 4;
}

static bool EpochAsyncQueuePressure(void)
{
    return EpochTxnQueueCount() >= EpochAsyncQueuePressureThreshold();
}

static void EpochRecordGateWait(EpochPerfGateKind kind, instr_time* start, uint64_t spin_count)
{
    if (!EpochPerfIsEnabled() || start == nullptr) {
        return;
    }
    instr_time end;
    INSTR_TIME_SET_CURRENT(end);
    INSTR_TIME_SUBTRACT(end, *start);
    EpochPerfAddGateWait(kind, INSTR_TIME_GET_MICROSEC(end), spin_count);
}

/* gap 顶满时 cap epoch 硬 pack 配额（COMMIT pack 路径，跨 backend in-flight + 已 pack 计数） */
static constexpr long EPOCH_ASYNC_PAUSE_US = 200;

static uint64_t EpochAsyncNowMs(void)
{
    instr_time now;
    INSTR_TIME_SET_CURRENT(now);
    return static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(now) / 1000);
}

static uint64_t EpochAsyncCapEpoch(void)
{
    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    return logical + EpochGetPhysLogicalGapLimit();
}

/* cap 上尚未 seal 的 pack 数；seal 后不计入 gate 压力 */
static uint64_t EpochAsyncCapUnsealedBacklog(uint64_t cap)
{
    uint32_t node_id = EpochGetNodeId();
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(cap, node_id);
    uint64_t sealed = E_NodeManager::GetSealedPackTxnCounters(cap, node_id);
    return (packed > sealed) ? (packed - sealed) : 0;
}

static uint64_t EpochAsyncPackGateStagnationDrip(uint64_t limit)
{
    uint64_t drip = limit / 16;
    if (drip < 32) {
        drip = 32;
    }
    if (drip > 64) {
        drip = 64;
    }
    return drip;
}

static void EpochAsyncResetCapPackDripState(EpochSharedState* state)
{
    state->async_cap_pack_drip_tokens.store(0, std::memory_order_release);
    state->async_cap_pack_drip_refill_ms.store(0, std::memory_order_release);
}

/*
 * backlog 超 limit 且 stagnation 已过后，按周期补充 drip token；全节点共享同一 cap 槽预算。
 */
static void EpochAsyncRefillCapPackDripTokens(EpochSharedState* state, uint64_t limit, uint32_t stagnation_ms)
{
    if (stagnation_ms == 0) {
        return;
    }
    uint64_t drip = EpochAsyncPackGateStagnationDrip(limit);
    uint64_t max_tokens = drip * 2;
    uint64_t now_ms = EpochAsyncNowMs();
    uint64_t last_ms = state->async_cap_pack_drip_refill_ms.load(std::memory_order_acquire);
    if (last_ms == 0) {
        return;
    }
    while (now_ms - last_ms >= stagnation_ms) {
        uint64_t next_ms = last_ms + stagnation_ms;
        if (!state->async_cap_pack_drip_refill_ms.compare_exchange_weak(
                last_ms, next_ms, std::memory_order_acq_rel, std::memory_order_acquire)) {
            continue;
        }
        uint64_t prev = state->async_cap_pack_drip_tokens.fetch_add(drip, std::memory_order_acq_rel);
        if (prev + drip > max_tokens) {
            state->async_cap_pack_drip_tokens.store(max_tokens, std::memory_order_release);
        }
        last_ms = next_ms;
    }
}

static bool EpochAsyncTryConsumeCapPackDripToken(EpochSharedState* state)
{
    for (;;) {
        uint64_t cur = state->async_cap_pack_drip_tokens.load(std::memory_order_acquire);
        if (cur == 0) {
            return false;
        }
        if (state->async_cap_pack_drip_tokens.compare_exchange_weak(
                cur, cur - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return true;
        }
    }
}

static long EpochAsyncPackGateBackoffUs(uint64_t spins)
{
    if (spins < 32) {
        return EPOCH_ASYNC_PAUSE_US;
    }
    if (spins < 256) {
        return 1000L;
    }
    long cap_us = static_cast<long>(EpochGetAsyncAssignMaxSleepMs()) * 1000L;
    if (cap_us <= 0 || cap_us > 5000L) {
        cap_us = 5000L;
    }
    return cap_us;
}

static constexpr uint64_t EPOCH_ASYNC_COMMIT_RATE_COST = 1000000ULL;

/*
 * 各 backend 共享 token bucket：按 epoch_async_max_tpmc 估算 NewOrder/min，
 * 用 write_ratio_percent 折算为写事务 COMMIT 速率（TPCC 50/50 时约 1.8x）。
 */
static void EpochAsyncRefillCommitRateTokens(EpochSharedState* state, uint64_t refill_per_ms, uint64_t burst_cap)
{
    uint64_t now_ms = EpochAsyncNowMs();
    uint64_t last_ms = state->async_commit_rate_last_ms.load(std::memory_order_acquire);
    if (last_ms == 0) {
        uint64_t expect = 0;
        if (state->async_commit_rate_last_ms.compare_exchange_strong(
                expect, now_ms, std::memory_order_acq_rel, std::memory_order_acquire)) {
            state->async_commit_rate_tokens.store(burst_cap / 2, std::memory_order_release);
        }
        return;
    }
    if (now_ms <= last_ms) {
        return;
    }
    uint64_t elapsed = now_ms - last_ms;
    if (elapsed > 5000) {
        elapsed = 5000;
    }
    uint64_t next_ms = last_ms + elapsed;
    if (!state->async_commit_rate_last_ms.compare_exchange_weak(
            last_ms, next_ms, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
    }
    uint64_t added = elapsed * refill_per_ms;
    uint64_t tokens = state->async_commit_rate_tokens.fetch_add(added, std::memory_order_acq_rel) + added;
    if (tokens > burst_cap) {
        state->async_commit_rate_tokens.store(burst_cap, std::memory_order_release);
    }
}

static void EpochAsyncWaitForCommitRateLimit(void)
{
    uint32_t max_tpmc = EpochGetAsyncMaxTpmc();
    if (max_tpmc == 0 || !E_IsAsyncEpochMode()) {
        return;
    }
    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr) {
        return;
    }

    uint32_t ratio_pct = EpochGetAsyncTpmcWriteRatioPercent();
    uint64_t refill_per_ms = (static_cast<uint64_t>(max_tpmc) * ratio_pct * EPOCH_ASYNC_COMMIT_RATE_COST)
        / (100ULL * 60000ULL);
    if (refill_per_ms == 0) {
        refill_per_ms = 1;
    }
    uint64_t burst_cap = refill_per_ms * 2000ULL;
    if (burst_cap < EPOCH_ASYNC_COMMIT_RATE_COST) {
        burst_cap = EPOCH_ASYNC_COMMIT_RATE_COST;
    }

    for (;;) {
        CHECK_FOR_INTERRUPTS();
        EpochAsyncRefillCommitRateTokens(state, refill_per_ms, burst_cap);
        uint64_t tokens = state->async_commit_rate_tokens.load(std::memory_order_acquire);
        if (tokens >= EPOCH_ASYNC_COMMIT_RATE_COST) {
            if (state->async_commit_rate_tokens.compare_exchange_weak(tokens,
                    tokens - EPOCH_ASYNC_COMMIT_RATE_COST,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                break;
            }
            continue;
        }
        pg_usleep(EPOCH_ASYNC_PAUSE_US);
    }
}

static void EpochAsyncResetCapPackGateIfNeeded(EpochSharedState* state, uint64_t cap)
{
    uint64_t gate_epoch = state->async_cap_pack_epoch.load(std::memory_order_acquire);
    if (gate_epoch == cap) {
        return;
    }
    state->async_cap_pack_reserved.store(0, std::memory_order_release);
    state->async_cap_pack_blocked_ms.store(0, std::memory_order_release);
    EpochAsyncResetCapPackDripState(state);
    while (!state->async_cap_pack_epoch.compare_exchange_weak(gate_epoch,
               cap,
               std::memory_order_acq_rel,
               std::memory_order_acquire)) {
        if (gate_epoch == cap) {
            return;
        }
        state->async_cap_pack_reserved.store(0, std::memory_order_release);
        state->async_cap_pack_blocked_ms.store(0, std::memory_order_release);
        EpochAsyncResetCapPackDripState(state);
    }
}

/*
 * cap 槽 pack 配额：unsealed_backlog(cap) + in-flight >= limit 时等待。
 * limit 为 UINT64_MAX 时关闭。stagnation_ms 触顶后按周期发放 drip token（非抬升 effective_limit），
 * 在避免永久关断的同时保持各节点 pack 背压对称。
 * 返回 true 表示已占用 in-flight 槽，调用方 pack 结束必须 Release。
 */
static bool EpochAsyncWaitForCapPackSlot(uint64_t commit_epoch)
{
    if (!E_IsAsyncEpochMode() || !EpochAsyncGapPressure()) {
        return false;
    }
    uint64_t limit = EpochGetAsyncStalePackLimit();
    if (limit == UINT64_MAX) {
        return false;
    }

    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr) {
        return false;
    }

    instr_time wait_start;
    INSTR_TIME_SET_CURRENT(wait_start);
    uint64_t spins = 0;
    uint32_t stagnation_ms = EpochGetAsyncStagnationMs();

    for (;;) {
        CHECK_FOR_INTERRUPTS();
        if (!EpochAsyncGapPressure()) {
            return false;
        }

        uint64_t cap = EpochAsyncCapEpoch();
        if (commit_epoch != cap) {
            return false;
        }

        EpochAsyncResetCapPackGateIfNeeded(state, cap);

        uint64_t backlog = EpochAsyncCapUnsealedBacklog(cap);
        uint64_t inflight = state->async_cap_pack_reserved.load(std::memory_order_acquire);
        uint64_t pressure = backlog + inflight;
        if (pressure < limit) {
            state->async_cap_pack_blocked_ms.store(0, std::memory_order_release);
            EpochAsyncResetCapPackDripState(state);
            (void)state->async_cap_pack_reserved.fetch_add(1, std::memory_order_acq_rel);
            break;
        }

        if (stagnation_ms == 0) {
            pg_usleep(EpochAsyncPackGateBackoffUs(spins));
            spins++;
            continue;
        }

        uint64_t now_ms = EpochAsyncNowMs();
        uint64_t blocked_since = state->async_cap_pack_blocked_ms.load(std::memory_order_acquire);
        if (blocked_since == 0) {
            (void)state->async_cap_pack_blocked_ms.compare_exchange_strong(
                blocked_since, now_ms, std::memory_order_acq_rel, std::memory_order_acquire);
            blocked_since = state->async_cap_pack_blocked_ms.load(std::memory_order_acquire);
        }
        if (blocked_since == 0 || now_ms - blocked_since < stagnation_ms) {
            pg_usleep(EpochAsyncPackGateBackoffUs(spins));
            spins++;
            continue;
        }

        if (state->async_cap_pack_drip_refill_ms.load(std::memory_order_acquire) == 0) {
            state->async_cap_pack_drip_refill_ms.store(now_ms, std::memory_order_release);
        }
        EpochAsyncRefillCapPackDripTokens(state, limit, stagnation_ms);
        if (EpochAsyncTryConsumeCapPackDripToken(state)) {
            (void)state->async_cap_pack_reserved.fetch_add(1, std::memory_order_acq_rel);
            break;
        }

        pg_usleep(EpochAsyncPackGateBackoffUs(spins));
        spins++;
    }

    if (spins > 0) {
        EpochRecordGateWait(EPOCH_PERF_GATE_ASYNC_PACK, &wait_start, spins);
    }
    return true;
}

static void EpochAsyncReleaseCapPackSlot(uint64_t commit_epoch)
{
    if (!E_IsAsyncEpochMode() || !EpochAsyncGapPressure()) {
        return;
    }
    if (EpochGetAsyncStalePackLimit() == UINT64_MAX) {
        return;
    }

    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr) {
        return;
    }

    uint64_t cap = EpochAsyncCapEpoch();
    if (commit_epoch != cap) {
        return;
    }
    if (state->async_cap_pack_epoch.load(std::memory_order_acquire) != cap) {
        return;
    }

    uint64_t prev = state->async_cap_pack_reserved.load(std::memory_order_relaxed);
    if (prev > 0) {
        (void)state->async_cap_pack_reserved.fetch_sub(1, std::memory_order_acq_rel);
    }
}

/* pack 前仅在队列高压时等待 drain，不要求 logical 推进 */
static void EpochAsyncWaitForPackQueue(void)
{
    if (!E_IsAsyncEpochMode() || !EpochAsyncQueuePressure()) {
        return;
    }

    uint32_t queue_threshold = EpochAsyncQueuePressureThreshold();
    instr_time wait_start;
    INSTR_TIME_SET_CURRENT(wait_start);
    uint64_t spins = 0;

    while (EpochTxnQueueCount() >= queue_threshold) {
        pg_usleep(EPOCH_ASYNC_PAUSE_US);
        spins++;
        CHECK_FOR_INTERRUPTS();
    }

    if (spins > 0) {
        EpochRecordGateWait(EPOCH_PERF_GATE_ASYNC_ADMIT, &wait_start, spins);
    }
}

static bool E_PackWriteTxnToQueue(O_TxnManager* txn, uint64_t commit_epoch, uint32_t node_id)
{
    bool cap_slot_reserved = false;
    if (E_IsAsyncEpochMode() && EpochGetTotalNodes() > 1) {
        cap_slot_reserved = EpochAsyncWaitForCapPackSlot(commit_epoch);
    }

    uint64_t csn = g_epochCsnManager.GetNextCSN();
    txn->SetCommitSequenceNumber(csn);
    E_WireTxn wire;
    if (!E_BuildWireTxn(txn, csn, &wire)) {
        if (cap_slot_reserved) {
            EpochAsyncReleaseCapPackSlot(commit_epoch);
        }
        return false;
    }
    E_NodeManager::IncChangeSetTxnCounters(commit_epoch, node_id);
    std::string payload;
    if (!E_WireSerializeTxn(wire, &payload)) {
        if (cap_slot_reserved) {
            EpochAsyncReleaseCapPackSlot(commit_epoch);
        }
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("epoch: failed to serialize transaction")));
    }
    if (!EpochTxnQueuePush(payload.data(), static_cast<uint32_t>(payload.size()))) {
        if (cap_slot_reserved) {
            EpochAsyncReleaseCapPackSlot(commit_epoch);
        }
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("epoch: pending txn queue is full")));
    }
    E_NodeManager::IncLocalPackTxnCounters(commit_epoch, node_id);
    if (cap_slot_reserved) {
        EpochAsyncReleaseCapPackSlot(commit_epoch);
    }
    return true;
}

static void E_AssignEpochOnStart(void)
{
    if (u_sess->epoch_cxt.epoch_assigned) {
        return;
    }
    uint64_t epoch = EpochAssignTxnEpoch();
    u_sess->epoch_cxt.start_epoch = epoch;
    u_sess->epoch_cxt.commit_epoch = epoch;
    u_sess->epoch_cxt.epoch_assigned = true;
}

/*
 * GeoGauss is_sync_exec START gate (mot_fdw.cpp)：
 * while (start_epoch > logical || !IsRecordCommitted()) usleep(200);
 *
 * 单连接「多等一会」在 healthy 时可行（physical 仅领先 logical 数个 slot）；
 * logical 卡死时 physical 可领先上百，新事务 commit_epoch 远超 logical，此处会
 * 长时间自旋；每个 TPCC 终端占一个 backend，JDBC 建连/首条 SQL 有超时 → 表现为连不上。
 */
static void EpochWaitForStartGate(uint64_t commit_epoch)
{
    if (E_IsAsyncEpochMode()) {
        /*
         * async：不在 START 阻塞；gap 顶满时 cap epoch pack 硬配额在 COMMIT pack 路径生效。
         * pack 前仅在队列 ≥75% 时等待 drain。
         */
        (void)commit_epoch;
        return;
    }

    const long waitUs = 200;
    if (E_IsSyncEpochMode()) {
        instr_time wait_start;
        INSTR_TIME_SET_CURRENT(wait_start);
        uint64_t spins = 0;
        while (commit_epoch > E_NodeManager::GetLogicalEpoch() ||
               !E_NodeManager::IsRecordCommitted()) {
            pg_usleep(waitUs);
            spins++;
            CHECK_FOR_INTERRUPTS();
        }
        EpochRecordGateWait(EPOCH_PERF_GATE_START, &wait_start, spins);
    }
}

/*
 * 已在途事务 COMMIT/pack：只等 logical 关窗，不再看 record_committed。
 * 否则 remote_exec 将 record_committed 置 false 后，读事务无法 pack，
 * change_set 永不对账，epoch 无法 seal。
 */
static void EpochWaitForLogicalEpoch(uint64_t commit_epoch)
{
    if (!E_IsSyncEpochMode()) {
        return;
    }
    const long waitUs = 200;
    instr_time wait_start;
    INSTR_TIME_SET_CURRENT(wait_start);
    uint64_t spins = 0;
    while (commit_epoch > E_NodeManager::GetLogicalEpoch()) {
        pg_usleep(waitUs);
        spins++;
        CHECK_FOR_INTERRUPTS();
    }
    EpochRecordGateWait(EPOCH_PERF_GATE_LOGICAL_COMMIT, &wait_start, spins);
}

static void EpochWaitForRemoteExeced(void)
{
    if (!E_IsSyncEpochMode()) {
        return;
    }
    const long waitUs = 200;
    instr_time wait_start;
    INSTR_TIME_SET_CURRENT(wait_start);
    uint64_t spins = 0;
    while (!E_NodeManager::IsRemoteExeced()) {
        pg_usleep(waitUs);
        spins++;
        CHECK_FOR_INTERRUPTS();
    }
    EpochRecordGateWait(EPOCH_PERF_GATE_REMOTE_EXEC, &wait_start, spins);
}

static void EpochWaitForRemoteRecordCommitted(void)
{
    if (!E_IsSyncEpochMode()) {
        return;
    }
    const long waitUs = 200;
    instr_time wait_start;
    INSTR_TIME_SET_CURRENT(wait_start);
    uint64_t spins = 0;
    while (!E_NodeManager::IsRemoteRecordCommitted()) {
        pg_usleep(waitUs);
        spins++;
        CHECK_FOR_INTERRUPTS();
    }
    EpochRecordGateWait(EPOCH_PERF_GATE_REMOTE_RECORD, &wait_start, spins);
}

void EpochXactCallback(XactEvent event)
{
    if (!g_enable_epoch_multimaster) {
        return;
    }

    if (event == XACT_EVENT_START) {
        if (E_ShouldTrackTxnEpoch()) {
            E_AssignEpochOnStart();
            EpochWaitForStartGate(u_sess->epoch_cxt.commit_epoch);
        }
        return;
    }

    O_TxnManager* txn = u_sess->occ_cxt.txn_manager;
    if (txn == nullptr) {
        if (event == XACT_EVENT_ABORT || event == XACT_EVENT_END_TRANSACTION) {
            EpochReleaseTxnExecuting();
        }
        return;
    }

    if (event == XACT_EVENT_COMMIT) {
        if (!E_TxnHasWriteSet(txn)) {
            /* 只读：SQL 已结束，先释放 executing 再等待关窗，避免与 seal 互锁 */
            EpochReleaseTxnExecuting();
            if (u_sess->epoch_cxt.epoch_assigned && E_IsSyncEpochMode()) {
                EpochWaitForLogicalEpoch(u_sess->epoch_cxt.commit_epoch);
                /* GeoGauss read_committed_txn_num：只读不计入 pack */
                E_NodeManager::IncReadCommittedTxnCounters(u_sess->epoch_cxt.commit_epoch, EpochGetNodeId());
            }
            u_sess->epoch_cxt.epoch_assigned = false;
            return;
        }
        if (!u_sess->epoch_cxt.epoch_assigned) {
            E_AssignEpochOnStart();
            EpochWaitForStartGate(u_sess->epoch_cxt.commit_epoch);
        }

        /*
         * 必须先 ValidateCommit 再等待 logical epoch。
         * 否则等待期间 epoch apply 会更新 heap 上 m_rowCSN，单连接也会误判为 concurrent update。
         */
        if (O_ValidateCommit() != RC_OK) {
            if (u_sess->epoch_cxt.epoch_assigned) {
                E_NodeManager::IncAbortTxnCounters(u_sess->epoch_cxt.commit_epoch, EpochGetNodeId());
            }
            EpochReleaseTxnExecuting();
            ereport(ERROR,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                     errmsg("epoch: could not serialize access due to concurrent update"),
                     errhidestmt(true),
                     errhideprefix(true),
                     onlyfrontmsg(true)));
        }

        uint64_t commit_epoch = u_sess->epoch_cxt.commit_epoch;
        uint32_t node_id = EpochGetNodeId();

        /*
         * 异步多主：pack 后立即入账 produce/record_commit/record_committed（与 sync 关窗计数对齐），
         * 避免 worker 在 pack 与 RECORD_COMMIT 之间推进 logical 时 prod=0 → commit_path_incomplete，
         * 以及 TryFinishCommitPhase 等 record_committed 导致 logical 卡死。
         * RECORD_COMMIT 仅 O_RecordCommit。
         */
        if (E_IsAsyncEpochMode()) {
            EpochAsyncWaitForCommitRateLimit();
            if (EpochGetTotalNodes() > 1) {
                EpochAsyncWaitForPackQueue();
                if (E_PackWriteTxnToQueue(txn, commit_epoch, node_id)) {
                    E_NodeManager::IncLocalProduceTxnCounters(commit_epoch, node_id);
                    E_NodeManager::IncLocalRecordCommitCounters(commit_epoch, node_id);
                    E_NodeManager::IncLocalRecordCommittedCounters(commit_epoch, node_id);
                }
            }
            EpochReleaseTxnExecuting();
            txn->ReleaseOccLocks();
            u_sess->epoch_cxt.deferred_commit = true;
            return;
        }

        /*
         * 同步多主：先 pack 并释放 executing，再等待 logical 关窗与对端 exec。
         * 若先等待 logical 再 pack，executing 未释放会挡住 seal → 死锁。
         */
        if (EpochGetTotalNodes() > 1) {
            (void)E_PackWriteTxnToQueue(txn, commit_epoch, node_id);
        }
        EpochReleaseTxnExecuting();
        /*
         * ValidateCommit 已加 OCC 行锁；若带着锁等 remote_exec，其他事务无法提交 pack，
         * executing 永不为 0 → seal 卡住 → logical/remote_exec 永不推进 → 死锁。
         */
        txn->ReleaseOccLocks();

        EpochWaitForLogicalEpoch(u_sess->epoch_cxt.commit_epoch);
        EpochWaitForRemoteExeced();

        if (E_IsSyncEpochMode()) {
            E_NodeManager::IncLocalProduceTxnCounters(commit_epoch, node_id);
            uint64_t csn = txn->GetCommitSequenceNumber();
            if (EpochAbortCsnContains(commit_epoch, csn, node_id)) {
                E_NodeManager::IncWriteAbortAfterSendTxnCounters(commit_epoch, node_id);
                E_NodeManager::IncAbortTxnCounters(commit_epoch, node_id);
                O_OccStatsRecordAbort(OCC_ABORT_REMOTE_MERGE);
                ereport(ERROR,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                     errmsg("epoch: transaction aborted after remote merge (csn %lu)", csn),
                     errhidestmt(true),
                     errhideprefix(true),
                     onlyfrontmsg(true)));
            }
            E_NodeManager::IncLocalRecordCommitCounters(commit_epoch, node_id);
        }

        u_sess->epoch_cxt.deferred_commit = true;
        return;
    }

    if (event == XACT_EVENT_RECORD_COMMIT) {
        if (!E_TxnHasWriteSet(txn)) {
            return;
        }
        if (E_IsSyncEpochMode() && !u_sess->epoch_cxt.deferred_commit) {
            return;
        }

        uint64_t csn = txn->GetCommitSequenceNumber();
        if (csn == INVALID_CSN) {
            csn = g_epochCsnManager.GetNextCSN();
            txn->SetCommitSequenceNumber(csn);
        }
        uint64_t commit_epoch = u_sess->epoch_cxt.commit_epoch;
        uint32_t node_id = EpochGetNodeId();

        if (E_IsAsyncEpochMode()) {
            if (!u_sess->epoch_cxt.deferred_commit) {
                return;
            }
            O_RecordCommit(csn);
            O_OccStatsRecordCommit();
            return;
        }

        if (E_IsSyncEpochMode() && !O_RevalidateWriteSet()) {
            if (u_sess->epoch_cxt.epoch_assigned) {
                E_NodeManager::IncWriteAbortAfterSendTxnCounters(commit_epoch, node_id);
                E_NodeManager::IncAbortTxnCounters(commit_epoch, node_id);
            }
            O_OccStatsRecordAbort(OCC_ABORT_REVALIDATE);
            ereport(ERROR,
                (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                 errmsg("epoch: could not serialize access due to concurrent update (revalidate)"),
                 errhidestmt(true),
                 errhideprefix(true),
                 onlyfrontmsg(true)));
        }

        O_RecordCommit(csn);

        if (E_IsSyncEpochMode()) {
            E_NodeManager::IncLocalRecordCommittedCounters(commit_epoch, node_id);
            EpochWaitForRemoteRecordCommitted();
        } else if (EpochGetTotalNodes() > 1) {
            E_NodeManager::IncLocalRecordCommitCounters(commit_epoch, node_id);
        }
        O_OccStatsRecordCommit();
        return;
    }

    if (event == XACT_EVENT_END_TRANSACTION) {
        u_sess->epoch_cxt.deferred_commit = false;
        EpochReleaseTxnExecuting();
        txn->EndTransaction();
        u_sess->epoch_cxt.epoch_assigned = false;
        u_sess->epoch_cxt.start_epoch = 0;
        u_sess->epoch_cxt.commit_epoch = 0;
        return;
    }

    if (event == XACT_EVENT_ABORT) {
        bool had_deferred = u_sess->epoch_cxt.deferred_commit;
        if (had_deferred) {
            u_sess->epoch_cxt.deferred_commit = false;
        }
        if (u_sess->epoch_cxt.epoch_assigned && (E_IsSyncEpochMode() || E_IsAsyncEpochMode())) {
            if (had_deferred) {
                E_NodeManager::IncWriteAbortAfterSendTxnCounters(u_sess->epoch_cxt.commit_epoch, EpochGetNodeId());
            }
            E_NodeManager::IncAbortTxnCounters(u_sess->epoch_cxt.commit_epoch, EpochGetNodeId());
        }
        EpochReleaseTxnExecuting();
        txn->Rollback();
        u_sess->epoch_cxt.epoch_assigned = false;
        u_sess->epoch_cxt.start_epoch = 0;
        u_sess->epoch_cxt.commit_epoch = 0;
    }
}
