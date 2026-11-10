#include "postgres.h"

#include "epoch/e_epoch.h"
#include "epoch/e_apply_shmem.h"
#include "epoch/e_recv_ledger.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_txn_queue.h"
#include "epoch/e_merge_shmem.h"
#include "epoch/e_net.h"

#include "epoch/e_config.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_recv_ledger.h"
#include "epoch/e_apply_pool.h"
#include "epoch/e_shmem.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_merge_pool.h"
#include "epoch/e_epoch_lifecycle.h"
#include "epoch/e_statistician.h"
#include "epoch/e_pacer.h"
#include "epoch/e_perf.h"
#include "epoch/e_log_path.h"
#include "epoch/e_txn_wire.h"

#include <unordered_map>

#include "miscadmin.h"

#include "portability/instr_time.h"

static void EpochMaybeWriteOffStaleChangeSet(uint64_t epoch);

/* physical assign 计数（含 since=0）连续不变超过该时长，视为无客户端，允许未满 limit 也推进 physical */
static constexpr long EPOCH_PHYSICAL_TXN_IDLE_MS = 1000;

static uint64_t g_physical_txn_idle_last_since = UINT64_MAX;
static instr_time g_physical_txn_idle_last_change;
static bool g_physical_txn_idle_tracker_inited = false;

static bool EpochPhysicalTxnAssignIdleReady(uint64_t since, uint64_t limit)
{
    if (since >= limit) {
        g_physical_txn_idle_last_since = since;
        INSTR_TIME_SET_CURRENT(g_physical_txn_idle_last_change);
        g_physical_txn_idle_tracker_inited = true;
        return false;
    }

    instr_time now;
    INSTR_TIME_SET_CURRENT(now);
    if (!g_physical_txn_idle_tracker_inited || since != g_physical_txn_idle_last_since) {
        g_physical_txn_idle_last_since = since;
        g_physical_txn_idle_last_change = now;
        g_physical_txn_idle_tracker_inited = true;
        return false;
    }

    instr_time delta = now;
    INSTR_TIME_SUBTRACT(delta, g_physical_txn_idle_last_change);
    return INSTR_TIME_GET_MILLISEC(delta) >= EPOCH_PHYSICAL_TXN_IDLE_MS;
}

static void EpochPhysicalTxnIdleTrackerReset(void)
{
    g_physical_txn_idle_last_since = UINT64_MAX;
    g_physical_txn_idle_tracker_inited = false;
}

/*
 * 系统可用内存是否偏低（提前预防 OOM）。
 * 读 /proc/meminfo 的 MemAvailable（含可回收 cache，比 sysconf 更准），
 * 判据「可用 < 总量的 5%」——按比例，随机器内存自适应，无硬编码绝对值。
 * 每 500ms 才真读一次，其余走缓存（epochphys 每 tick 都会问）。
 */
/*
 * 稳定性诊断：每 5 秒打一行本节点的四个游标 + 发送队列水位。
 * 三节点的 beat 按时间对齐后，能直接看出“谁在什么时候停滞、停了多久”。
 */
void EpochStateLog(void)
{
    static uint64_t last_us = 0;
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    const uint64_t now_us = static_cast<uint64_t>(tv.tv_sec) * 1000000ULL +
                            static_cast<uint64_t>(tv.tv_usec);
    /* 每 5 秒一条：五个游标 + 队列水位 + 当前 merge 时间片的各节点贡献 */
    if (last_us != 0 && (now_us - last_us) < 5000000ULL) {
        return;
    }
    last_us = now_us;
    const uint64_t merge_ep = GetMergeEpoch();
    /*
     * 写到数据目录根下的独立文件，而不是 pg_log：
     * prep 的 “reset data” 会清空 pg_log，压测期日志会被抹掉，
     * 这样即使重跑也能保留每一轮的状态曲线。
     */
    /*
     * 每个对端各自的 send 游标（不再只报最小值）：
     * 快节点应该看到自己发往各对端的进度是否被某个慢对端拖住。
     */
    char sendcols[192];
    int soff = 0;
    {
        const uint32_t self_id = EpochGetNodeId();
        const uint32_t total_nodes = EpochGetTotalNodes();
        for (uint32_t p = 0; p < total_nodes && soff < static_cast<int>(sizeof(sendcols)) - 24; ++p) {
            if (p == self_id) {
                continue;
            }
            soff += snprintf(sendcols + soff, sizeof(sendcols) - soff, " send%u=%lu",
                p, static_cast<unsigned long>(EpochGetSendEpochPeer(p)));
        }
    }

    /* 每个对端各自的待发队列深度（队列按 peer 拆分了，合计值看不出是哪条堵） */
    char qcols[192];
    int qoff = 0;
    {
        const uint32_t self_id = EpochGetNodeId();
        const uint32_t total_nodes = EpochGetTotalNodes();
        for (uint32_t p = 0; p < total_nodes && qoff < static_cast<int>(sizeof(qcols)) - 24; ++p) {
            if (p == self_id) {
                continue;
            }
            qoff += snprintf(qcols + qoff, sizeof(qcols) - qoff, " q%u=%lu",
                p, static_cast<unsigned long>(EpochTxnQueueCountPeer(p)));
        }
    }
    char state_path[MAXPGPATH];
    FILE* state_fp = nullptr;
    if (EpochFormatLogPath(state_path, sizeof(state_path), "epoch_state")) {
        state_fp = fopen(state_path, "a");
    }
    if (state_fp != nullptr) {
        char tsbuf[32];
        struct tm tmbuf;
        time_t now_sec = time(nullptr);
        (void)localtime_r(&now_sec, &tmbuf);
        (void)strftime(tsbuf, sizeof(tsbuf), "%Y-%m-%d %H:%M:%S", &tmbuf);
        fprintf(state_fp,
            "%s.%03d node=%u phys=%lu%s recv=%lu merge=%lu apply=%lu log=%lu qdepth=%lu%s "
            "snap_to=%lu eofq=%lu eofr=%lu mret=%lu stale=%lu lat=%.1fms\n",
            tsbuf, static_cast<int>((now_us / 1000) % 1000),
            EpochGetNodeId(),
            (unsigned long)E_NodeManager::GetPhysicalEpoch(),
            sendcols,
            (unsigned long)EpochRecvGetEpoch(),
            (unsigned long)merge_ep,
            (unsigned long)EpochApplyQueueGetApplyEpoch(),
            (unsigned long)E_NodeManager::GetLogicalEpoch(),
            (unsigned long)EpochTxnQueueCount(),
            qcols,
            (unsigned long)EpochPerfSnapTimeoutCount(),
            (unsigned long)EpochNetEofRequestSent(),
            (unsigned long)EpochNetEofResent(),
            (unsigned long)EpochPacerMergeRetryCount(),
            (unsigned long)EpochRecvStaleCount(),
            static_cast<double>(EpochPerfEpochLatAvgUs()) / 1000.0);
        fclose(state_fp);
    }

    EPOCH_DBG_FPRINTF("EPOCH_DBG state node=%u phys=%lu%s recv=%lu merge=%lu apply=%lu log=%lu "
        "qdepth=%lu%s snap_to=%lu lat=%.1fms\n",
        EpochGetNodeId(),
        (unsigned long)E_NodeManager::GetPhysicalEpoch(),
        sendcols,
        (unsigned long)EpochRecvGetEpoch(),
        (unsigned long)merge_ep,
        (unsigned long)EpochApplyQueueGetApplyEpoch(),
        (unsigned long)E_NodeManager::GetLogicalEpoch(),
        (unsigned long)EpochTxnQueueCount(),
        qcols,
        (unsigned long)EpochPerfSnapTimeoutCount(),
        static_cast<double>(EpochPerfEpochLatAvgUs()) / 1000.0);
    fflush(stderr);
}/*
 * 给【当前这一片】封口：塞收尾包，然后把 physical 推到下一片。
 *
 * 由 backend 在抢到“第 24 个名额”时调用 —— 一片收满 24 笔事务就立刻封口，
 * 不再依赖每 10ms 的定时推进。这样：
 *   · 每片条数固定，队列容量不会溢出；
 *   · 没有事务时不产生空片（定时推进时代前期会跑出上千个空片）。
 */
void EpochSealEpoch(uint64_t epoch)
{
    if (!g_enable_epoch_multimaster || epoch == 0 || EpochGetTotalNodes() <= 1) {
        return;
    }

    /* 收尾包只是“到此为止”的标记，不带任何计数 */
    E_WireTxn eof_wire;
    eof_wire.epoch = epoch;
    eof_wire.server_id = EpochGetNodeId();
    eof_wire.txn_state = EPOCH_TXN_EMPTY;
    eof_wire.csn = 0;
    eof_wire.rows.clear();

    std::string payload;
    if (E_WireSerializeTxn(eof_wire, &payload)) {
        const uint32_t total_nodes = EpochGetTotalNodes();

        for (uint32_t peer = 0; peer < total_nodes; ++peer) {
            if (!EpochTxnQueuePush(peer, epoch, payload.data(),
                    static_cast<uint32_t>(payload.size()))) {
                EPOCH_DBG_FPRINTF("EPOCH_DBG eof_push_fail peer=%u epoch=%lu size=%zu\n",
                    peer, static_cast<unsigned long>(epoch), payload.size());
                fflush(stderr);
            }
        }
    }

    (void)E_NodeManager::AddPhysicalEpoch();
    EpochPerfNotePhysicalAdvance();

    /* 记录这一片的封口时刻，供“封口 → apply 完成”的粗延迟统计 */
    EpochPerfNoteEpochSeal(epoch);
}

bool EpochTryReservePhysicalTxnSlot(uint64_t* out_slot_num)
{
    if (!g_enable_epoch_multimaster || g_epoch_physical_txns_per_epoch <= 0) {
        if (out_slot_num != nullptr) {
            *out_slot_num = 0;
        }
        return true;
    }
    EpochSharedState* state = EpochGetSharedState();
    if (state == nullptr) {
        return false;
    }
    /*
     * 只计数，不再设上限。
     *
     * 时间片的结束判据改成了“入队条数达到上限”，而 abort 的事务不会入队，
     * 若这里仍卡在“分配满 25 就拒绝”，这些名额就白白浪费，入队数永远到不了 25。
     * 因此分配不再限流：事务可以继续分配到当前时间片，直到入队数凑满才推进；
     * 推进之后仍未提交的事务，会在打包入口看到“已封口”而自动改期到新时间片。
     */
    const uint64_t n = state->physical_txn_since_advance.fetch_add(1, std::memory_order_acq_rel) + 1;
    if (out_slot_num != nullptr) {
        *out_slot_num = n;
    }
    return true;
}

bool EpochIsLogicalReady(uint64_t logicalEpoch)
{
    uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    uint32_t delay = static_cast<uint32_t>(g_epoch_delay_slots);
    if (delay == 0) {
        delay = EPOCH_DEFAULT_DELAY;
    }
    return physical > logicalEpoch + delay;
}

bool EpochAllNodesSendCompleted(uint64_t epochId)
{
    uint32_t totalNodes = EpochGetTotalNodes();
    for (uint32_t node = 0; node < totalNodes; ++node) {
        if (node != EpochGetNodeId() && !EpochPeerIsConnected(node)) {
            return false;
        }
        if (!E_NodeManager::IsNodeSendCompleted(epochId, node)) {
            return false;
        }
    }
    return true;
}

/* change_set（仅写）<= pack + abort */
bool EpochLocalTxnExcComplete(uint64_t epochId)
{
    uint32_t node_id = EpochGetNodeId();
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epochId, node_id);
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epochId, node_id);
    uint64_t aborts = E_NodeManager::GetAbortTxnCounters(epochId, node_id);
    return change_set <= packed + aborts;
}

static void EpochHealLocalTxnExc(uint64_t epoch, uint32_t node_id)
{
    return;
    EpochMaybeWriteOffStaleChangeSet(epoch);
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epoch, node_id);
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    uint64_t aborts = E_NodeManager::GetAbortTxnCounters(epoch, node_id);
    while (change_set > packed + aborts) {
        E_NodeManager::IncAbortTxnCounters(epoch, node_id);
        aborts++;
    }
}

static void EpochMaybeWriteOffStaleChangeSet(uint64_t epoch)
{
    uint32_t node_id = EpochGetNodeId();
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epoch, node_id);
    uint64_t packed = E_NodeManager::GetLocalPackTxnCounters(epoch, node_id);
    uint64_t aborts = E_NodeManager::GetAbortTxnCounters(epoch, node_id);
    uint64_t write_abort_after = E_NodeManager::GetWriteAbortAfterSendTxnCounters(epoch, node_id);
    uint64_t abort_net = (aborts > write_abort_after) ? (aborts - write_abort_after) : 0;
    uint64_t should_send = (change_set > abort_net) ? (change_set - abort_net) : 0;
    if (should_send > 0 && packed < should_send) {
        return;
    }
    if (change_set <= packed + aborts) {
        return;
    }
    /* 用队列水位替代 gap：本地待发队列空了才认定这些 pack 是孤立残留 */
    if (EpochTxnQueueCount() > 0) {
        return;
    }
    uint64_t orphan = change_set - packed - aborts;
    for (uint64_t i = 0; i < orphan; ++i) {
        E_NodeManager::IncAbortTxnCounters(epoch, node_id);
    }
}

bool EpochIsLocalCommitStageReady(uint64_t epoch)
{
    EpochMaybeWriteOffStaleChangeSet(epoch);
    if (EpochLocalTxnExcComplete(epoch)) {
        return true;
    }
    return EpochLocalTxnExcComplete(epoch);
}

bool EpochTryAdvanceLogical(void)
{
    if (!g_enable_epoch_multimaster) {
        return false;
    }

    EpochPerfNoteLogicalAdvanceTry();

    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint64_t targetEpoch = logical;
    E_NodeManager::EnsureEpochSlot(targetEpoch);

    /*
     * 单节点：无 ZMQ/seal，physical 越过 delay 后即可推进 logical。
     */
    if (EpochGetTotalNodes() <= 1) {
        if (!EpochIsLogicalReady(targetEpoch)) {
            return false;
        }
        E_NodeManager::AddLogicalEpoch();
        E_NodeManager::ClearEpochSlot(targetEpoch);
        EpochPerfNoteLogicalAdvanceOk();
        return true;
    }

    if (!EpochPipelineAllStagesReady(targetEpoch)) {
        EpochPipelineNoteLogicalAdvanceStall(targetEpoch);
        return false;
    }

    E_NodeManager::AddLogicalEpoch();
    EpochLifecycleEmitOnAdvance(targetEpoch);
    E_NodeManager::ClearEpochSlot(targetEpoch);
    EpochPipelineClearStageFlags(targetEpoch);
    EpochPerfNoteLogicalAdvanceOk();
    return true;
}

bool EpochWaitForCommitGate(uint64_t commitEpoch)
{
    if (!g_enable_epoch_multimaster) {
        return true;
    }

    /*
     * GeoGauss is_sync_exec COMMIT gate: commit_epoch <= logical.
     */
    const long waitUs = 200;
    while (commitEpoch > E_NodeManager::GetLogicalEpoch()) {
        pg_usleep(waitUs);
        CHECK_FOR_INTERRUPTS();
    }
    return true;
}

bool EpochWaitForLogicalPast(uint64_t commitEpoch)
{
    if (!g_enable_epoch_multimaster) {
        return true;
    }

    const long waitUs = 200;
    while (commitEpoch >= E_NodeManager::GetLogicalEpoch()) {
        pg_usleep(waitUs);
        CHECK_FOR_INTERRUPTS();
    }
    return true;
}

uint64_t EpochAssignTxnEpoch(void)
{
    if (!g_enable_epoch_multimaster) {
        return 0;
    }
    /*
     * apply 写回连接（LoginDatabase 带 xc_maintenance_mode=on）必须绕过 epoch：
     * 它要连 test 库执行写回，若走 epoch 则 pack 后要等 apply 落地，而 logical
     * 又依赖这条 apply → 死锁闭环（实测 apply worker 连 localhost:32000 超时、
     * apply_inflight 只增不减、logical 卡死、所有新会话卡 START gate）。
     */
    if (u_sess->attr.attr_common.xc_maintenance_mode) {
        return 0;
    }
    /*
     * GeoGauss：直接用 physical 发号。physical 领先 logical 的上限由
     * EpochAdvancePhysicalOnce 的 gap_limit 背压保证；sync START gate 再约束执行时机。
     */
    return E_NodeManager::GetPhysicalEpoch();
}
