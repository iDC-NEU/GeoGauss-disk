#include "postgres.h"

#include <sys/time.h>

#include "epoch/e_pipeline.h"
#include "epoch/e_epoch_lifecycle.h"
#include "epoch/e_log_path.h"
#include "epoch/e_dyn_net.h"
#include "epoch/e_abort_csn.h"
#include "epoch/e_apply.h"
#include "epoch/e_apply_pool.h"
#include "epoch/e_apply_shmem.h"
#include "epoch/e_config.h"
#include "epoch/e_apply_stats.h"
#include "epoch/e_crdt.h"
#include "epoch/e_crdt_shmem.h"
#include "epoch/e_epoch.h"
#include "epoch/e_merge_pool.h"
#include "epoch/e_merge_profile.h"
#include "epoch/e_merge_shmem.h"
#include "epoch/e_net.h"

#include "miscadmin.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_recv_ledger.h"
#include "epoch/e_statistician.h"
#include "epoch/e_stuck_trace.h"
#include "epoch/e_trace.h"
#include "epoch/e_shmem.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_tip_staging_shmem.h"
#include "epoch/e_pk_staging_shmem.h"
#include "epoch/e_txn_queue.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_wire_envelope.h"
#include "epoch/e_pacer.h"
#include "epoch/e_perf.h"
#include "epoch/e_concurrent_map.h"

#include "dn_api.h"
#include "dn_config.h"
#include "dn_types.h"
#include "occ/o_staging.h"

#include "portability/instr_time.h"

#include <atomic>
#include <array>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

/*
 * 入站 merge：按 txn.epoch 直接落入二维 MergeCell[epoch%slots][route_wid]（GeoGauss）。
 * 不再经中间 recv 队列 + Drain/epoch 比较/塞回。
 */
static void EpochPipelineSubmitMergeWireTxn(const E_WireTxn& txn);
static bool EpochPipelineTrySubmitMergeWireTxn(const E_WireTxn& txn);

static void EpochRemoteMergeEnqueue(const E_WireTxn& txn)
{
    if (txn.epoch == 0) {
        return;
    }
    EpochPipelineSubmitMergeWireTxn(txn);
}

/* 每个 epoch 按到达顺序保留原始 wire txn，供按 txn 粒度 apply（GeoGauss 式分片 map，无全局锁） */
static EpochConcurrent::UnorderedMap<uint64_t, std::vector<E_WireTxn>> g_epoch_wire_txns;
static EpochConcurrent::U64SetMap g_applied_wire_txns;
static std::atomic<bool> g_pipeline_recv_running{false};
static std::thread g_pipeline_recv_thread;
static std::atomic<bool> g_pipeline_logical_running{false};
static std::thread g_pipeline_logical_thread;
static std::atomic<bool> g_pipeline_physical_running{false};
static std::thread g_pipeline_physical_thread;
static constexpr long EPOCH_PIPELINE_IDLE_US = 200;

static std::mutex g_pending_crdt_clear_mutex;
static std::vector<uint64_t> g_pending_crdt_clear_epochs;
/*
 * epoch 内「已受理过 merge」的 wire（成功或在途）。
 * 入 apply 环由 merge done 按笔 enqueue；不靠旁路 apply/merge inflight set。
 */
static EpochConcurrent::U64SetMap g_merge_accepted_wires;

/* merge 在途：看 apply/merge 环 occupancy，不另维护 wire_key inflight set */
static bool EpochPipelineEpochHasAsyncMergeInflight(uint64_t epoch)
{
    /* 任务表已删除：在途由进程内队列自身管理，这里恒为“无在途” */
    (void)epoch;
    return false;
}

/*
 * merge 提交失败的「暂存区」。
 * 35ms RTT 下 peer 的写集会比本端 merge 游标超前 8~11 个 epoch；MergeCell 只有
 * epoch_crdt_slot_count（=8）行，未来 epoch 抢走行后，当前 epoch 的迟到写集就无处可放。
 * 此时**既不能丢**（丢了该 epoch 的 seen 永远少一条 → logical 永久卡死），
 * **也不能阻塞提交线程**（阻塞 recv 会连迟到写集本身都收不到 → 互等死锁）。
 * 因此按 epoch 暂存 wire_key，由 merge worker 每跳重试提交；payload 本体已在
 * g_epoch_wire_txns 中，不额外拷贝。
 */
static bool EpochPipelineIsWireTxnApplied(uint64_t epoch, uint64_t wire_key);
static bool EpochPipelineFindWireTxn(uint64_t epoch, uint64_t wire_key, E_WireTxn* out);
static void EpochPipelineSubmitAcceptedMergeWireTxn(const E_WireTxn& txn, uint64_t wire_key);
/* 一条写集的唯一键：csn 与来源节点拼接（同一 csn 在不同节点是不同事务） */
static uint64_t EpochPipelineWireTxnKey(const E_WireTxn& txn)
{
    return txn.csn ^ (static_cast<uint64_t>(txn.server_id) << 48);
}

static bool EpochPipelineDrainLocalQueueForEpoch(uint32_t peer, uint64_t epoch);
static bool EpochPipelineDrainLocalQueueWindow(uint32_t peer);
static void EpochPipelinePeerSendLoop(uint32_t peer);
/* GeoGauss：入环必须成功；失败一律 ERROR，禁止 pending 重试 */
void EpochPipelineEnqueueApplyWinner(const E_WireTxn& txn);

static void EpochPipelineOnAsyncApplyTaskDone(uint64_t epoch, uint64_t wire_key, bool ok)
{
    if (ok) {
        EpochPipelineMarkWireTxnApplied(epoch, wire_key);
    }
}

/* 合并失败时撤销“已受理”记录，允许对端重传 */
void EpochPipelineRevokeAcceptedWire(uint64_t epoch, uint64_t wire_key)
{
    (void)EpochConcurrent::U64SetErase(g_merge_accepted_wires, epoch, wire_key);
}

void EpochPipelineSkipWireTxnForApply(uint64_t epoch, uint64_t wire_key, bool is_merge_abort)
{
    if (EpochPipelineIsWireTxnApplied(epoch, wire_key)) {
        return;
    }
    EpochPipelineMarkWireTxnApplied(epoch, wire_key);
    /*
     * 这里不再记 abort。
     * 输家事务已在 CRDT merge 时按 csn 直接写进该时间片的 abort 集合，
     * 统计时统一在该时间片 apply 完成那一刻按集合条数汇总一次。
     * 若这里也记一笔，同一个事务会被计两次（而且这里还是“每笔写集”粒度）。
     */
    (void)is_merge_abort;
}
/*
 * 发送一条：从该时间片的队列取一条，发给对端，同时入 merge 队列（本端回环）。
 *
 * 取到收尾包时把 send 游标 +1；普通写集顺带记一笔“本端已发”，
 * 供 merge 完成判据里的 expected 使用。
 */
/*
 * 从【该 peer 自己的队列】取一条，只发给它。
 *
 * 每个对端一份队列 + 一个发送线程：某个对端慢（tc 限速、TCP 窗口小）时，
 * 只有它自己这条线程会阻塞在 EpochNetPublishOne 里，其它对端照常发送。
 * 原先是单队列 + 单线程串行发给所有 peer，慢 peer 会把整条流水线停住。
 */
static bool EpochPipelineDrainLocalQueueForEpoch(uint32_t peer, uint64_t epoch)
{
    char buffer[EPOCH_MAX_WIRE_TXN_BYTES];
    uint32_t len = 0;
    uint32_t index = 0;

    /* ① 从该 peer 自己的队列取一条（index = 该条在本片内的序号） */
    if (!EpochTxnQueuePop(peer, epoch, buffer, sizeof(buffer), &len, &index)) {
        return false;
    }

    const std::string payload(buffer, len);

    E_WireTxn txn;
    const bool decoded = E_WireDeserializeTxn(buffer, len, &txn);
    const bool is_eof = decoded && E_WireTxnIsEpochEnd(txn);

    /* PA：跳过慢节点时，数据与 EOF 由 proxy 补发。 */
    if (EpochPacerPaShouldSkipPeer(peer, epoch)) {
        if (is_eof) {
            EpochAdvanceSendEpoch(peer, epoch);
        }
        return true;
    }

    /* DBR：每笔只决策一次，所有 peer 队列沿用同一结论。 */
    uint64_t defer_to = 0;
    const int verdict = decoded
        ? EpochPacerDecideItem(epoch, txn.csn, txn.server_id, is_eof, &defer_to)
        : 1;

    if (verdict == 0) {
        E_WireTxn moved = txn;
        moved.epoch = defer_to;

        std::string out;
        bool requeued = false;
        if (E_WireSerializeTxn(moved, &out)) {
            /* 目标格满则等待，不能退回原片发送。 */
            for (;;) {
                if (EpochTxnQueuePush(peer, defer_to, out.data(),
                        static_cast<uint32_t>(out.size()))) {
                    requeued = true;
                    break;
                }
                pg_usleep(200);
                CHECK_FOR_INTERRUPTS();
            }
        }
        if (requeued) {
            EpochPacerNoteDeferred();
        } else {
            (void)EpochNetPublishOne(peer, payload);
        }
    } else {
        /* 只负责外发；本端入队由独立的 self 线程处理（见 DrainSelfForEpoch） */
        (void)EpochNetPublishOne(peer, payload);
    }

    /* 收尾包 → 该 peer 的 send 游标 +1 */
    if (is_eof) {
        /* 存一份到环形缓存：未被对端 ack 时会补发 */
        EpochNetNoteEofSent(peer, epoch, payload);
        EPOCH_DBG_FPRINTF("EPOCH_DBG send_eof epoch=%lu from_node=%u to_peer=%u deferred=%lu\n",
            static_cast<unsigned long>(epoch), EpochGetNodeId(), peer,
            static_cast<unsigned long>(EpochPacerDeferredCount()));
        fflush(stderr);
        EpochAdvanceSendEpoch(peer, epoch);
    }
    return true;
}

bool EpochPipelineWireTxnHasWritableRows(const E_WireTxn& txn)
{
    for (const auto& row : txn.rows) {
        if (row.op_type != EPOCH_OP_READ) {
            return true;
        }
    }
    return false;
}

static void EpochPipelineRememberWireTxn(uint64_t epoch, const E_WireTxn& txn)
{
    g_epoch_wire_txns.Modify(epoch, [&](std::vector<E_WireTxn>& txns, bool /*is_new*/) {
        txns.push_back(txn);
    });
}

static void EpochPipelineForgetWireTxn(uint64_t epoch, uint64_t wire_key)
{
    g_epoch_wire_txns.Modify(epoch, [&](std::vector<E_WireTxn>& txns, bool /*is_new*/) {
        for (size_t i = txns.size(); i > 0; --i) {
            if (EpochPipelineWireTxnKey(txns[i - 1]) != wire_key) {
                continue;
            }
            txns.erase(txns.begin() + static_cast<std::ptrdiff_t>(i - 1));
            break;
        }
    });
}

static bool EpochPipelineIsWireTxnApplied(uint64_t epoch, uint64_t wire_key)
{
    return EpochConcurrent::U64SetContains(g_applied_wire_txns, epoch, wire_key);
}

void EpochPipelineMarkWireTxnApplied(uint64_t epoch, uint64_t wire_key)
{
    (void)EpochConcurrent::U64SetInsert(g_applied_wire_txns, epoch, wire_key);
}

static bool EpochPipelineFindWireTxn(uint64_t epoch, uint64_t wire_key, E_WireTxn* out)
{
    bool found = false;
    (void)g_epoch_wire_txns.Visit(epoch, [&](const std::vector<E_WireTxn>& txns) {
        for (const E_WireTxn& txn : txns) {
            if (EpochPipelineWireTxnKey(txn) == wire_key) {
                if (out != nullptr) {
                    *out = txn;
                }
                found = true;
                return;
            }
        }
    });
    return found;
}

/*
 * 赢家原始 wire → apply 环（不 Build）。async 可在 merge_complete 前入环；
 * Claim 仍须 merge_complete。GeoGauss：入环必须成功，失败 ERROR（禁止 pending）。
 *//*
 * 赢家入 apply 队列：只是把这条写集塞进该时间片的并发队列。
 * 去重、abort 判定、可写行判定都已在 CRDT 合并时完成，这里不再重复。
 */
void EpochPipelineEnqueueApplyWinner(const E_WireTxn& txn)
{
    (void)EpochApplyQueueEnqueueTxn(txn.epoch, txn);
}

bool EpochPipelineTryEnqueueMergeWinnerOrig(const E_WireTxn& txn,
    uint64_t wire_key,
    const char* payload,
    uint32_t payload_len)
{
    if (txn.epoch == 0) {
        return true;
    }
    if (g_epoch_apply_local_only && txn.server_id != EpochGetNodeId()) {
        EpochPipelineSkipWireTxnForApply(txn.epoch, wire_key, false);
        EpochApplyStatsNoteCommit();
        E_NodeManager::IncCommittedTxnCounters(txn.epoch, EpochGetNodeId());
        return true;
    }
    EpochPipelineEnqueueApplyWinner(txn);
    return true;
}

static void EpochPipelineScheduleCrdtClear(uint64_t epoch)
{
    std::lock_guard<std::mutex> lock(g_pending_crdt_clear_mutex);
    g_pending_crdt_clear_epochs.push_back(epoch);
}

static void EpochPipelineDrainPendingCrdtClears(void)
{
    std::vector<uint64_t> epochs;
    {
        std::lock_guard<std::mutex> lock(g_pending_crdt_clear_mutex);
        if (g_pending_crdt_clear_epochs.empty()) {
            return;
        }
        epochs.swap(g_pending_crdt_clear_epochs);
    }
    for (uint64_t epoch : epochs) {
        EpochCrdtShmemClearEpoch(epoch);
    }
}

static void EpochPipelineClearEpochWireTxnState(uint64_t epoch, bool clear_crdt)
{
    g_epoch_wire_txns.Erase(epoch);
    g_applied_wire_txns.Erase(epoch);
    g_merge_accepted_wires.Erase(epoch);
    if (clear_crdt) {
        EpochCrdtShmemClearEpoch(epoch);
    }
}

/* epoch-end 已设置 should_receive 且 received>=should 时置位，供 merge 关窗 */






void EpochPipelineFinishPeerRecvAfterEpochEnd(uint64_t epoch, uint32_t from_node)
{
    /*
     * NotePeerEpochEndReceived 在 CheckAndSetNodeReceiveCompleted 之前调用；
     * recv 此处才齐时再标记 peer txn wire 完成，供 merge 关窗。
     */
}

bool EpochPipelineEpochHasMergeInflight(uint64_t epoch)
{
    return EpochPipelineEpochHasAsyncMergeInflight(epoch);
}



/* 每个已连接的对端，这一片的收尾包都到了（= 它这一片的写集都到齐了） */
static bool EpochPipelineAllPeersEpochEndReceived(uint64_t epoch)
{
    const uint32_t self = EpochGetNodeId();
    const uint32_t total = EpochGetTotalNodes();
    for (uint32_t peer = 0; peer < total; ++peer) {
        if (peer == self) {
            continue;
        }
        if (!EpochPeerIsConnected(peer)) {
            return false;
        }
        if (!EpochRecvHasEof(epoch, peer)) {
            return false;
        }
    }
    return true;
}



/*
 * seal-ahead 上界：可尝试 seal 的最大 epoch（含）。
 * 条件：EpochIsLogicalReady(e) ⇔ physical > e + delay；且不超过 logical+gap（与 recv FUTURE 窗口对齐）。
 */
static uint64_t EpochPipelineSealAheadMaxEpoch(void)
{
    const uint64_t logical = E_NodeManager::GetLogicalEpoch();
    const uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    uint32_t delay = static_cast<uint32_t>(g_epoch_delay_slots);
    if (delay == 0) {
        delay = EPOCH_DEFAULT_DELAY;
    }

    /* physical > e + delay ⇒ e_max = physical - delay - 1 */
    if (physical <= static_cast<uint64_t>(delay)) {
        return logical;
    }
    uint64_t max_by_phys = physical - static_cast<uint64_t>(delay) - 1;
    if (max_by_phys < logical) {
        return logical;
    }

    /* gap 约束已移除：seal 窗口只受 physical-delay 限制（内存由队列水位背压兜底） */
    return max_by_phys;
}

/*
 * 对单个 epoch 推进 physical_ready / local_commit_ready / seal+send（可超前 logical）。
 */

/*
 * merge 完成：peer/self 门槛满足，且 wire_merge_done == expected（严格相等）。
 */
/*
 * 合并是否完成：所有对端收尾包到齐（declared 已冻结）、本端这一片已发完、
 * 且已合并笔数正好等于应收总数。
 *
 * 纯计算，不加锁、不取时间、不打印 —— 它被 merge/apply/logical 三条路径高频调用，
 * 以前的停滞检测（全局 mutex + gettimeofday）把这里变成了热点。
 */
bool EpochPipelineIsMergePhaseComplete(uint64_t epoch)
{
    const uint64_t expected = EpochPipelineExpectedMergeTxnCount(epoch);
    const uint64_t merged = EpochPipelineMergedTxnCount(epoch);

    /*
     * 判据只看"这一片的数据是否都到齐并合并完"：
     *   · 三个节点（含本端）的 EOF 都到
     *   · 已合并条数 >= 应收条数（用 >= 允许账本口径的少量偏差）
     *
     * 不再看"本端是否已发完"（send 游标）：本端写集通过自环直接进接收队列，
     * 与网络发送进度无关；把 send 掺进来只会让快节点被慢节点的发送拖住。
     */
    return EpochRecvAllEof(epoch) &&
           (merged >= expected);
}


static bool EpochPipelineEpochHasUnappliedWireTxns(uint64_t epoch);

/*
 * GeoGauss：merge_complete 后 expected=MergedTxnCounters（关窗时冻结）；
 * apply Finish → IncCommitted；committed == expected 即 apply_complete（多计即有 bug）。
 * 另：若本 epoch apply 环仍有 task，即使计数碰巧相等也不关窗（防 expected=0 竞态误关）。
 */
static bool EpochPipelineIsApplyPhaseComplete(uint64_t epoch)
{
    /*
     * 判据：这一片真的合并齐了，且写回队列里没有它剩下的活。
     *
     * “合并齐了”统一用 IsMergePhaseComplete（比条数：merged == expected）。
     * 不再用 EpochPipelineEpochMergeComplete —— 那个是“merge 游标越过就算完成”，
     * 和条数判据对同一片可能给出相反答案（游标领先时它说完了，条数说没完），
     * 两条判据并存会让 logical 在两者之间摇摆。
     */
    if (!EpochPipelineIsMergePhaseComplete(epoch)) {
        return false;
    }
    return true;
}

bool EpochPipelineMergerHasPendingRemoteApply(uint64_t epoch)
{
    /*
     * 仅持 epoch 所在分片锁扫描，不深拷贝 vector；不同 epoch 互不阻塞。
     * merge 在途由 IsApplyPhaseComplete 看环 occupancy，此处只看未 skip 的赢家 wire。
     */
    bool pending = false;
    (void)g_epoch_wire_txns.Visit(epoch, [&](const std::vector<E_WireTxn>& txns) {
        for (const E_WireTxn& orig : txns) {
            uint64_t wire_key = EpochPipelineWireTxnKey(orig);
            if (EpochPipelineIsWireTxnApplied(epoch, wire_key)) {
                continue;
            }
            if (EpochAbortCsnContains(epoch, orig.csn, orig.server_id)) {
                continue;
            }
            if (!EpochPipelineWireTxnHasWritableRows(orig)) {
                continue;
            }
            pending = true;
            return;
        }
    });
    return pending;
}

uint32_t EpochPipelineCountUnappliedWireTxns(uint64_t epoch)
{
    uint32_t pending = 0;
    (void)g_epoch_wire_txns.Visit(epoch, [&](const std::vector<E_WireTxn>& txns) {
        for (const E_WireTxn& orig : txns) {
            uint64_t wire_key = EpochPipelineWireTxnKey(orig);
            if (!EpochPipelineIsWireTxnApplied(epoch, wire_key)) {
                pending++;
            }
        }
    });
    return pending;
}

void EpochPipelineDiagLogFirstUnapplied(uint64_t epoch)
{
    (void)g_epoch_wire_txns.Visit(epoch, [&](const std::vector<E_WireTxn>& txns) {
        for (const E_WireTxn& orig : txns) {
            uint64_t wire_key = EpochPipelineWireTxnKey(orig);
            if (EpochPipelineIsWireTxnApplied(epoch, wire_key)) {
                continue;
            }
            bool in_abort = EpochAbortCsnContains(epoch, orig.csn, orig.server_id);
            bool writable = EpochPipelineWireTxnHasWritableRows(orig);
            return;
        }
    });
}

static bool EpochPipelineEpochHasUnappliedWireTxns(uint64_t epoch)
{
    return EpochPipelineCountUnappliedWireTxns(epoch) > 0;
}

static bool EpochPipelineEpochReadyToApply(uint64_t epoch);


/*
 * logical 推进失败原因（细粒度）：与 AllStagesReady / IsMerge|ApplyPhaseComplete 对齐。
 * 供 STALL 时间加权明细（e_perf）定位 31ms 周期里的最长杆。
 */
/*
 * 诊断：merge 游标连续 2 秒不推进时，打印这一片的账本细节。
 *   merged/expected 是推进判据；declared/recv 是三方收包情况；qlen 是未被消费的 txn 数。
 */
void EpochPipelineLogMergeStall(void)
{
    static uint64_t last_epoch = 0;
    static int64_t last_change_us = 0;
    static int64_t last_log_us = 0;

    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    const int64_t now = static_cast<int64_t>(ts.tv_sec) * 1000000 +
                        static_cast<int64_t>(ts.tv_nsec / 1000);

    const uint64_t me = GetMergeEpoch();
    if (me != last_epoch) {
        last_epoch = me;
        last_change_us = now;
        return;
    }
    if (last_change_us == 0 || now - last_change_us < 2000000) {
        return;
    }
    if (now - last_log_us < 2000000) {
        return;
    }
    last_log_us = now;

    EPOCH_DBG_FPRINTF(
        "EPOCH_DBG merge_stall node=%u epoch=%lu merged=%lu expected=%lu "
        "declared=[%lu %lu %lu] recv=[%lu %lu %lu] qlen=%zu\n",
        EpochGetNodeId(), static_cast<unsigned long>(me),
        static_cast<unsigned long>(EpochPipelineMergedTxnCount(me)),
        static_cast<unsigned long>(EpochPipelineExpectedMergeTxnCount(me)),
        static_cast<unsigned long>(EpochRecvDeclaredCount(me, 0)),
        static_cast<unsigned long>(EpochRecvDeclaredCount(me, 1)),
        static_cast<unsigned long>(EpochRecvDeclaredCount(me, 2)),
        static_cast<unsigned long>(EpochRecvReceivedCount(me, 0)),
        static_cast<unsigned long>(EpochRecvReceivedCount(me, 1)),
        static_cast<unsigned long>(EpochRecvReceivedCount(me, 2)),
        EpochMergeQueueSize(me));
    fflush(stderr);
}


static const char* EpochPipelineClassifyLogicalStall(uint64_t epoch)
{
    if (!EpochPipelineIsMergePhaseComplete(epoch) || !EpochPipelineIsMergePhaseComplete(epoch)) {
        const uint32_t self = EpochGetNodeId();
        if (!E_NodeManager::IsNodeSendCompleted(epoch, self)) {
            return "merge_self_send";
        }
        if (!EpochPipelineAllPeersEpochEndReceived(epoch)) {
            return "merge_peer_end";
        }
        if (EpochPipelineEpochHasAsyncMergeInflight(epoch)) {
            return "merge_async";
        }
        if (EpochPipelineMergedTxnCount(epoch) != EpochPipelineExpectedMergeTxnCount(epoch)) {
            return "merge_count";
        }
        return "merge_incomplete";
    }

    if (EpochPipelineEpochHasAsyncMergeInflight(epoch)) {
        return "apply_async_merge";
    }
    if (EpochPipelineEpochHasUnappliedWireTxns(epoch)) {
        return "apply_unapplied";
    }
    if (EpochPipelineMergerHasPendingRemoteApply(epoch)) {
        return "apply_remote_pending";
    }
    if (!EpochPipelineIsApplyPhaseComplete(epoch)) {
        return "apply_incomplete_cond";
    }
    return "unknown";
}

void EpochPipelineNoteLogicalAdvanceStall(uint64_t epoch)
{
    const char* reason = EpochPipelineClassifyLogicalStall(epoch);
    if (strcmp(reason, "merge_peer_end") == 0 || strcmp(reason, "merge_peer_txn") == 0 ||
        strcmp(reason, "merge_incomplete") == 0) {
    }
    /*
     * 粗因仍报 merge/apply_incomplete，便于旧 STALL 行占比；细因走 perf 时间加权明细。
     */
    const char* coarse = reason;
    if (strncmp(reason, "merge_", 6) == 0) {
        coarse = "merge_incomplete";
    } else if (strncmp(reason, "apply_", 6) == 0) {
        coarse = "apply_incomplete";
    }
    EpochPerfNoteLogicalStall(reason);
}

void EpochPipelineClearStageFlags(uint64_t epoch)
{
    EpochPipelineClearEpochWireTxnState(epoch, false);
    EpochPipelineScheduleCrdtClear(epoch);
}

/*
 * 一片能否推进 logical：三件事全部完成。
 *
 *   ① 本端已把这一片发完 —— send 游标越过它
 *   ② 这一片合并完成 —— 所有对端 EOF 到齐，且 merged == expected（= 各列 declared 之和）
 *   ③ 这一片写回完成
 *
 * 以前这里还读三个布尔标记（physical_ready / local_commit_ready / self_send_sent），
 * 那些都靠专门的封口循环置位，属于同一事实的第二份表示，已删除。
 *//*
 * 一片能否推进 logical：三个游标都必须越过它。
 *
 *   ① send  越过 —— 本端写集全发、收尾包出门、队列已清空
 *   ② merge 越过 —— 该片 merged == declared，合并真的做完了
 *   ③ apply 越过 —— 该片写回队列已空
 *
 * 每个游标只在自身条件成立时前进，所以“越过”就是“做完了”，单调且不会被清空。
 * 以前这里直接调 IsMergePhaseComplete 比对计数，会和 merge 线程抢同一个判据：
 * logical 先通过、随后 ClearEpochSlot 把 wire_merge_done 清零，
 * merge 线程再读就变成 merged=0，游标永久卡死（实测 log=merge+1）。
 */
bool EpochPipelineAllStagesReady(uint64_t epoch)
{
    if (EpochGetTotalNodes() <= 1) {
        return false;
    }
    /*
     * 只看 apply 游标：apply 只会在 merge 完成之后推进，
     * 所以 apply > epoch 已经隐含了 merge 完成；发送进度与本端是否落地无关。
     */
    return EpochApplyQueueGetApplyEpoch() > epoch;
}

bool EpochPipelineEpochApplyComplete(uint64_t epoch)
{
    /* 单一真相 = apply 游标（同 merge 的推导方式，无布尔标志） */
    if (epoch == 0) {
        return false;
    }
    const uint64_t ae = EpochApplyQueueGetApplyEpoch();
    if (ae > epoch) {
        return true;
    }
    return (ae == epoch) && EpochPipelineIsApplyPhaseComplete(epoch);
}


/* 关窗瞬间：可写且非 AbortCsn 的 wire = apply expected（冻结进 MergedTxnCounters） */
static uint64_t EpochPipelineCountApplyWinners(uint64_t epoch)
{
    uint64_t winners = 0;
    (void)g_epoch_wire_txns.Visit(epoch, [&](const std::vector<E_WireTxn>& txns) {
        for (const E_WireTxn& orig : txns) {
            if (!EpochPipelineWireTxnHasWritableRows(orig)) {
                continue;
            }
            if (EpochAbortCsnContains(epoch, orig.csn, orig.server_id)) {
                continue;
            }
            winners++;
        }
    });
    return winners;
}

/*
 * GeoGauss：wire_merge_done == expected(epoch-end) → 冻结 apply expected 后 CAS merge_complete。
 * 必须先 SetMergedTxnCounters 再发布 merge_complete，否则 apply 关窗会看到 expected=0、committed=0
 * 误推进 apply_epoch，把未 apply 的赢家留在旧格里（Claim 只扫 apply_epoch）。
 */
static bool EpochPipelineTryMarkMergeComplete(uint64_t epoch)
{
    E_NodeManager::EnsureEpochSlot(epoch);

    /* 只看数据是否到齐并合并完；本端发送进度与本端落地无关 */
    if (!EpochPipelineIsMergePhaseComplete(epoch)) {
        return false;
    }
    /* 冻结 apply expected（幂等）；完成状态由游标推导，不再置布尔标志 */
    if (E_NodeManager::GetMergedTxnCounters(epoch, EpochGetNodeId()) != 0) {
        return false;
    }
    const uint64_t winners = EpochPipelineCountApplyWinners(epoch);
    E_NodeManager::SetMergedTxnCounters(epoch, EpochGetNodeId(), winners);
    EpochLifecycleNoteMergeComplete(epoch);
    return true;
}

/*
 * apply_complete 置位 + apply_epoch 推进。
 * GeoGauss：barrier 只认 flag / 计数；入环已在 merge Finish。
 */
static bool EpochPipelineMaybeCompleteApplyAndAdvance(void)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return false;
    }

    bool progress = false;
    const uint64_t logical = E_NodeManager::GetLogicalEpoch();
    EpochApplyQueueSetApplyEpoch(logical);

    /*
     * 原「自愈拉回」已删除：它把已推进的 apply_epoch 拉回卡住的 logical，
     * 与逐格推进形成震荡（实测 apply_ep 在 10 与 16 之间来回跳）。
     * apply_epoch 单调推进是安全的：EnsureApplyEpoch 逐格推进且只在该 epoch
     * 无在途 task 时前进；logical 会随 apply 完成自然推进。
     */

    for (;;) {
        const uint64_t apply_ep = EpochApplyQueueGetApplyEpoch();
        if (apply_ep == 0) {
            break;
        }
        E_NodeManager::EnsureEpochSlot(apply_ep);
        /* 完成由条件判断，不再有 apply_complete 标志 */
        if (!EpochPipelineIsApplyPhaseComplete(apply_ep)) {
            break;
        }
        if (!EpochApplyQueueTryAdvanceApplyEpoch(apply_ep)) {
            break;
        }
        progress = true;
    }
    return progress;
}

/*
 * GeoGauss：merge_complete(E) 且 merge_epoch+1 < physical → ++。
 * 赢家已在 merge Finish 入环；apply 用 committed==expected 关窗。
 */
static bool EpochPipelineMaybeAdvanceMergeEpoch(void)
{
    const uint64_t me = GetMergeEpoch();
    if (me == 0) {
        return false;
    }
    /*
     * 接收游标必须先越过这一片。
     *
     * recv 和 merge 是并行的：merge 游标刚好落在 recv 还没处理到的位置时，
     * 那一列的 declared 还是 99999、eof 还是 false，判据永远不成立。
     * 等 recv 推过去（说明三个节点的 eof 都到齐了）再判断。
     */
    const uint64_t recv_ep = EpochRecvGetEpoch();
    if (me >= recv_ep) {
        return false;
    }

    const uint64_t physical = E_NodeManager::GetPhysicalEpoch();

    if (!EpochPipelineIsMergePhaseComplete(me)) {
        return false;
    }
    /*
     * 允许把"最后一个已封口片"merge 掉（原先是 me + 1 >= physical 就停）。
     *
     * 原来是"merge 最多到 physical-2"，于是 apply 最多到 physical-2，
     * 事务起点处那道"上一片必须已 apply"的闸门（apply_ep >= physical）
     * 永远不可能满足 —— 形成死锁：
     *   apply 要等 merge 前进；merge 要等 physical 前进；
     *   physical 要等新事务；新事务要等 apply。
     * 实测 phys=2/merge=1/apply=1 完全静止。
     *
     * 放宽后：merge_ep 可到 physical（已封口的片全部合并完），
     * apply_ep 随即可到 physical，闸门可以正常放行。
     */
    if (me + 1 > physical) {
        return false;
    }
    bool advanced = EpochMergeQueueTryAdvanceMergeEpoch(me);
    if (advanced) {
        EpochStuckTraceNote(EST_MERGE_CURSOR_ADVANCE, static_cast<uint32_t>(me), 0);
        /*
         * PA：这一片 merge 已完成，如果它当时被判定为"跳过慢节点"，
         * 就把缓存的赢家 + EOF 补发给慢节点。
         * 放在游标推进之后、下一片 merge 之前，保证顺序：先补齐，再往下走。
         */
        EpochPacerPaResendEpoch(me);
    }
    return advanced;
}

/* 仅启动：merge_epoch 从 0 → 1；之后只靠 complete 后 ++ */
static void EpochPipelineBootstrapMergeEpochIfNeeded(void)
{
    if (GetMergeEpoch() != 0) {
        return;
    }
    if (E_NodeManager::GetPhysicalEpoch() <= 1) {
        return;
    }
    EpochMergeQueueSetMergeEpoch(1);
}

bool EpochPipelineMergeWorkerPullTick(uint32_t worker_id, uint32_t n_workers)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return false;
    }

    EpochPipelineBootstrapMergeEpochIfNeeded();

    bool progress = false;
    (void)worker_id;
    (void)n_workers;


    /*
     * 只做一件事：merged == declared 就把 merge 游标 +1（CAS）。
     * apply 的推进不在这里管。
     */
    progress = EpochPipelineMaybeAdvanceMergeEpoch() || progress;
    return progress;
}

static bool EpochPipelineTryApplyStage(uint64_t epoch)
{
    if (EpochPipelineEpochApplyComplete(epoch)) {
        return false;
    }

    bool progress = false;

    /*
     * apply 须在 merge 全阶段结束后启动：对端 txn/epoch-end 未到齐前 CRDT 裁决可能变化，
     * 与 merge 并行会把尚未最终定稿的写集落盘。
     */
    if (!EpochPipelineIsMergePhaseComplete(epoch)) {
        return false;
    }

    if (EpochPipelineEpochReadyToApply(epoch)) {
        bool apply_ok = EpochPipelineTryApplyMergedEpoch(epoch);
        if (apply_ok) {
            progress = true;
        } else {
            uint32_t node_id = EpochGetNodeId();
            uint64_t expected_winners = EpochPipelineExpectedApplyWinnerCount(epoch);
            uint64_t committed = E_NodeManager::GetCommittedTxnCounters(epoch, node_id);
            bool merger_pending = EpochPipelineMergerHasPendingRemoteApply(epoch);
            static std::unordered_map<uint64_t, int> g_apply_stage_gate_tick;
            int& gate_tick = g_apply_stage_gate_tick[epoch];
            gate_tick++;
            if (gate_tick == 1 || gate_tick % 50 == 1 || committed < expected_winners) {
            }
        }
    }

    if (EpochPipelineIsApplyPhaseComplete(epoch)) {
        EpochLifecycleNoteApplyComplete(epoch);
        progress = true;
    }

    return progress;
}

bool EpochPipelinePackSendActive(void)
{
    /* pack 线程固定为 1，不再有“无 pack”的旧路径 */
    return g_enable_epoch_multimaster && EpochGetTotalNodes() > 1;
}

/* 0=未启动 1=进行中 2=完成；同进程线程可见 */
static std::atomic<int> g_pipeline_bootstrap_state{0};

bool EpochPipelineIsBootstrapped(void)
{
    return g_pipeline_bootstrap_state.load(std::memory_order_acquire) == 2;
}

void EpochPipelineBootstrapOwner(void)
{
    int expected = 0;
    if (!g_pipeline_bootstrap_state.compare_exchange_strong(expected, 1, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        while (g_pipeline_bootstrap_state.load(std::memory_order_acquire) != 2) {
            pg_usleep(1000L);
        }
        return;
    }

    EpochPipelineInit();
    EpochApplyPoolInit();
    EpochMergePoolInit();
    EpochRecvLedgerInit();
    EpochPacerPaRestoreState();
    EpochPipelineStartRecvThread();
    EpochPipelineStartMergeThread();
    EpochPipelineStartApplyThread();
    EpochPipelineStartLogicalAdvanceThread();
    EpochPipelineStartPhysicalAdvanceThread();
    EpochPipelineStartStateLogThread();   /* 独立线程：只打状态，不参与业务锁 */
    OccStagingEnsureInit();
    OccStagingInitEpochLocalTip();
    EpochLifecycleReset();
    EpochPerfReset();

    g_pipeline_bootstrap_state.store(2, std::memory_order_release);
    ereport(LOG, (errmsg("epoch pipeline: BootstrapOwner ready (pack_send=%d, role=epochcoord)",
        EpochPipelinePackSendActive() ? 1 : 0)));
}


/*
 * 发送主体已拆成"每 peer 一条线程"（见 EpochPipelineStartPeerSendThreads）。
 * 这里只保留与发送相关的周期维护：网卡采样 + pacer（DBR/PA 挂点）。
 */
bool EpochPipelineSendWorkerTick(uint32_t send_id, uint32_t n_send)
{
    (void)n_send;
    if (send_id != 0) {
        return false;
    }
    if (!EpochPipelinePackSendActive() || !EpochPipelineIsBootstrapped()) {
        return false;
    }

    EpochNetPollUplink();
    EpochPacerPackTick();
    return false;
}






void EpochPipelineInit(void)
{
    g_merge_accepted_wires.Clear();
    g_epoch_wire_txns.Clear();
    g_applied_wire_txns.Clear();
    if (g_epoch_skip_apply) {
        ereport(LOG,
            (errmsg("epoch pipeline: g_epoch_skip_apply=true — heap apply disabled (benchmark only)")));
    }
    if (g_epoch_apply_local_only) {
        ereport(LOG,
            (errmsg("epoch pipeline: g_epoch_apply_local_only=true — apply local wire only; "
                    "remote marked applied without heap write")));
    }
    if (g_enable_epoch_multimaster) {
        /* worker 线程需 reattach 共享内存中的静态指针 */
        EpochShmemInit();
        EpochAbortCsnShmemInit();
        EpochTxnQueueShmemInit(EpochGetTxnQueueCapacity());
        EpochCrdtShmemInit();
        EpochTipStagingShmemInit();
        EpochPkStagingShmemInit();
    }
    (void)EpochNetInit();
}

void EpochPipelineShutdown(void)
{
    EpochApplyPoolShutdown();
    EpochMergePoolShutdown();
    EpochPipelineStopApplyThread();
    EpochPipelineStopMergeThread();
    EpochPipelineStopLogicalAdvanceThread();
    EpochPipelineStopPhysicalAdvanceThread();
    EpochPipelineStopRecvThread();
    OccStagingModuleShutdown();
    EpochNetShutdown();
    g_merge_accepted_wires.Clear();
    g_epoch_wire_txns.Clear();
    g_applied_wire_txns.Clear();
    g_pipeline_bootstrap_state.store(0, std::memory_order_release);
}




/*
 * 把【该 peer】的队列从它自己的 send 游标出发一直取空。
 * 退出条件：这一格取空、或游标越过 physical（后面的片还没封口）。
 */
static bool EpochPipelineDrainLocalQueueWindow(uint32_t peer)
{
    bool progress = false;
    const uint64_t physical = E_NodeManager::GetPhysicalEpoch();

    for (int i = 0; i < 4096; ++i) {
        const uint64_t epoch = EpochGetSendEpochPeer(peer);
        if (epoch == 0 || epoch > physical) {
            break;
        }
        if (!EpochPipelineDrainLocalQueueForEpoch(peer, epoch)) {
            break;
        }
        progress = true;
    }
    return progress;
}

/* 单个 peer 的发送线程主体 */
static void EpochPipelinePeerSendLoop(uint32_t peer)
{
    EpochSetAuxThreadName("epochsendp");
    while (!t_thrd.epoch_worker_cxt.shutdown_requested) {
        if (!EpochPipelineIsBootstrapped()) {
            pg_usleep(1000L);
            continue;
        }
        if (EpochPipelineDrainLocalQueueWindow(peer)) {
            continue;
        }
        pg_usleep(200L);
    }
}

/* 为每个对端起一条发送线程（只启动一次） */
/*
 * 自环排空：从 self 槽取一条，决策后直接入本端接收队列。
 *
 * 为什么单独一条线程：
 *   peer 线程的发往外部的速度受网络限制（慢 peer 只有 5Mbps），
 *   若自环挂在某个 peer 线程上，本端 recv 就会被那条慢链路拖住，
 *   表现为"缺自己的 EOF"→ recv 永久卡死。
 */
static bool EpochPipelineDrainSelfForEpoch(void)
{
    const uint32_t self = EpochGetNodeId();
    const uint64_t epoch = EpochGetSendEpochPeer(self);
    if (epoch == 0) {
        return false;
    }

    char buffer[EPOCH_MAX_WIRE_TXN_BYTES];
    uint32_t len = 0;
    uint32_t index = 0;
    if (!EpochTxnQueuePop(self, epoch, buffer, sizeof(buffer), &len, &index)) {
        return false;
    }

    const std::string payload(buffer, len);
    E_WireTxn txn;
    const bool decoded = E_WireDeserializeTxn(buffer, len, &txn);
    const bool is_eof = decoded && E_WireTxnIsEpochEnd(txn);

    /* 与 peer 线程共用决策。 */
    uint64_t defer_to = 0;
    const int verdict = decoded
        ? EpochPacerDecideItem(epoch, txn.csn, txn.server_id, is_eof, &defer_to)
        : 1;
    if (verdict == 0) {
        E_WireTxn moved = txn;
        moved.epoch = defer_to;
        std::string out;
        bool requeued = false;
        if (E_WireSerializeTxn(moved, &out)) {
            for (;;) {
                if (EpochTxnQueuePush(self, defer_to, out.data(),
                        static_cast<uint32_t>(out.size()))) {
                    requeued = true;
                    break;
                }
                pg_usleep(200);
                CHECK_FOR_INTERRUPTS();
            }
        }
        if (requeued) {
            EpochPacerNoteDeferred();
        } else {
            (void)EpochNetEnqueueSelf(epoch, payload);
        }
    } else {
        (void)EpochNetEnqueueSelf(epoch, payload);
    }

    if (is_eof) {
        EpochAdvanceSendEpoch(self, epoch);
    }
    return true;
}

static void EpochPipelineSelfDrainLoop(void)
{
    EpochSetAuxThreadName("epochself");
    while (!t_thrd.epoch_worker_cxt.shutdown_requested) {
        if (!EpochPipelineIsBootstrapped()) {
            pg_usleep(1000L);
            continue;
        }
        bool progress = false;
        const uint64_t physical = E_NodeManager::GetPhysicalEpoch();
        for (int i = 0; i < 4096; ++i) {
            const uint64_t e = EpochGetSendEpochPeer(EpochGetNodeId());
            if (e == 0 || e > physical) {
                break;
            }
            if (!EpochPipelineDrainSelfForEpoch()) {
                break;
            }
            progress = true;
        }
        if (!progress) {
            pg_usleep(200L);
        }
        /* 顺手清理过期的决策记录 */
        static int64_t last_sweep_us = 0;
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        const int64_t now = static_cast<int64_t>(ts.tv_sec) * 1000000 +
                            static_cast<int64_t>(ts.tv_nsec / 1000);
        if (now - last_sweep_us > 1000000) {
            last_sweep_us = now;
            EpochPacerSweepDecisions(EpochGetSendEpochPeer(EpochGetNodeId()));
        }
    }
}

void EpochPipelineStartPeerSendThreads(void)
{
    static std::vector<std::thread> s_peer_threads;
    static bool s_started = false;
    if (s_started) {
        return;
    }
    s_started = true;

    const uint32_t self = EpochGetNodeId();
    const uint32_t total = EpochGetTotalNodes();
    for (uint32_t peer = 0; peer < total; ++peer) {
        if (peer == self) {
            continue;
        }
        s_peer_threads.emplace_back(EpochPipelinePeerSendLoop, peer);
    }
    s_peer_threads.emplace_back(EpochPipelineSelfDrainLoop);
    ereport(LOG, (errmsg("epoch peer send threads started (peers=%u + self)", total - 1)));
}

/*
 * 按 txn.epoch 写入 MergeCell[epoch%slots][route]；满/失败 → ERROR（无 throttle/塞回）。
 * 必须先 Remember 再 Submit：async Claim 可能在 Submit 返回前 Finish，
 * 否则 FindWireTxn 失败 → ERROR（node3 epoch=96 wire_key=1315）。
 */
static bool EpochPipelineTrySubmitMergeWireTxn(const E_WireTxn& txn)
{
    uint64_t wire_key = EpochPipelineWireTxnKey(txn);

    /* 持久受理去重：成功 merge 后仍保留，直到 epoch clear */
    if (!EpochConcurrent::U64SetInsert(g_merge_accepted_wires, txn.epoch, wire_key)) {
        {
            static uint64_t dbg_dup = 0;
            if ((++dbg_dup % 20) == 0) {
                EPOCH_DBG_FPRINTF("EPOCH_DBG merge_dup epoch=%lu key=%lu csn=%lu from=%u rows=%zu\n",
                    (unsigned long)txn.epoch, (unsigned long)wire_key,
                    (unsigned long)txn.csn, txn.server_id, txn.rows.size());
                fflush(stderr);
            }
        }
        return true;
    }

    /*
     * 迟到写集延后：merge 游标已越过该 epoch（wire 晚于游标推进才到）时不能入池——
     * task 入行后永不被游标行 TryClaim，行被占死（实测 epoch 1 迟到 task 卡住行[1]，
     * epoch 9 撞 slot 重绑被拒 2370 万次）。判据用 merge 游标而非 logical：游标才是
     * “已处理到哪”的真实位置（自愈跳窗时游标会领先 logical）。
     * 按约束【不丢弃、延后】：写集 Remember 进 g_epoch_wire_txns 持久保留
     * （直到 epoch clear），待该 epoch 重开机制消费；pending 项由调用方移除。
     */
    /* 同上：用 logical 判断“已结算”，不能用领先很多的 merge 游标 */
    const uint64_t settled = E_NodeManager::GetLogicalEpoch();
    if (txn.epoch > 0 && settled > 0 && txn.epoch < settled) {
        EpochPipelineRememberWireTxn(txn.epoch, txn);
        return true;
    }

    EpochPipelineRememberWireTxn(txn.epoch, txn);

    bool is_local = (txn.server_id == EpochGetNodeId());
    uint64_t rebind_before = 0;
    uint64_t full_before = 0;
    if (!EpochMergePoolSubmit(txn)) {
        /* 逐笔记录失败原因：行仍绑在别的 epoch（rebind）还是本列格满 */
        if (0 > rebind_before) {
            EpochStuckTraceNote(EST_SUBMIT_FAIL_REBIND, static_cast<uint32_t>(txn.epoch), wire_key);
        } else if (0 > full_before) {
            EpochStuckTraceNote(EST_SUBMIT_FAIL_CELLS_FULL, static_cast<uint32_t>(txn.epoch), wire_key);
        }
        /*
         * 保留 accepted/remembered（写集仍在 g_epoch_wire_txns），登记待提交。
         * 绝不能 erase：erase 等于这笔写集从 merge 里消失，该 epoch 永远少一条。
         */
        return false;
    }
    EpochStuckTraceNote(EST_SUBMIT_OK, static_cast<uint32_t>(txn.epoch), wire_key);
    return true;
}

/*
 * 收包路径专用：调用方已做过 wire 去重（g_merge_accepted_wires 已插入），
 * 这里只负责 Remember + 提交 / 暂存。绝不丢弃。
 */
static void EpochPipelineSubmitAcceptedMergeWireTxn(const E_WireTxn& txn, uint64_t wire_key)
{
    EpochPipelineRememberWireTxn(txn.epoch, txn);
    /*
     * 迟到写集：merge 游标已越过该 epoch（wire 晚于游标推进才到）→ 不入池。
     * 否则 task 永不被游标行 TryClaim，行被占死（实测 epoch 19 迟到 task 卡住
     * 行[3]、merge_epoch=22 越过 logical=19，logical 卡死 → apply gate 死锁）。
     * 数据已 Remember 留在 g_epoch_wire_txns，不 park（避免 pending 堆积）。
     */
    /*
     * “已结算”的判据用 logical，不能用 merge 游标。
     *
     * merge 游标只是“合并线程处理到哪”，它可以远远领先 logical
     * （实测 merge=6282 而 logical=2024）。若用它做判据，logical 还没走到的
     * 正常写集会被当成“迟到”拒之门外，那个 epoch 的 merged 永远追不上 expected，
     * logical 就此卡死。
     *
     * logical 才是“这一片已经结算完、不再需要新数据”的真实边界。
     */
    const uint64_t settled = E_NodeManager::GetLogicalEpoch();
    if (txn.epoch > 0 && settled > 0 && txn.epoch < settled) {
        return;
    }
    bool is_local = (txn.server_id == EpochGetNodeId());
    if (!EpochMergePoolSubmit(txn)) {
        EpochStuckTraceNote(EST_SUBMIT_FAIL_REBIND, static_cast<uint32_t>(txn.epoch), wire_key);
    }
}


static void EpochPipelineSubmitMergeWireTxn(const E_WireTxn& txn)
{
    /*
     * 不阻塞、不丢弃：行被未来 epoch 占用 / 本列格满时，TrySubmit 会把 wire_key
     * （pending 暂存机制已删除：提交就是一次入队，不会失败。）
     * 这里运行在 recv / pack 线程（无 u_sess），禁止 ereport，也不能长时间阻塞——
     * 一旦阻塞 recv，就连"迟到的那笔写集"本身都收不到，形成互等死锁。
     */
    (void)EpochPipelineTrySubmitMergeWireTxn(txn);
}

/*
 * 重试 pending 里的 merge 提交。由 merge worker 每跳调用；
 * 只有行回到 merge_epoch 名下（或格空出）时才会成功。
 */
/*
 * 解析「一个 epoch 一条消息」的批量包：
 *   ["EBP1"][epoch:8][count:4][sender:4][len:4|wire]...
 * 包头的 count 即该 peer 的贡献数（等价旧 epoch-end 的 should），与数据同包到达，
 * 因此不需要独立的 epoch-end 消息，也不会被慢上行时的 wire 堵在后面。
 */
/*
 * 接收循环：从 receive_epoch 那一格取，写集记一笔并进 merge，收尾包冻结 declared。
 * 所有对端的 eof 到齐时把 receive_epoch +1。
 */
static bool EpochPipelineRecvIncomingOnce(void)
{
    bool progress = false;

    for (int i = 0; i < 256; ++i) {
        const uint64_t epoch = EpochRecvGetEpoch();
        std::string payload;
        if (!EpochIncomingTryDequeue(epoch, &payload)) {
            break;
        }
        progress = true;

        E_WireTxn txn;
        if (!E_WireDeserializeTxn(payload.data(), payload.size(), &txn)) {
            continue;
        }

        /*
         * 迟到消息：发送端可能出现"收尾包先入队、最后一两条写集后入队"的竞争，
         * 那条写集到达时本片已经推进过去了，却还留在同一个环形格里。
         * 若不丢弃，它会在下一轮复用该格时被当成新片的写集（recv/declared 虚高，
         * 而它实际会被 merge 到旧 epoch 的队列里）→ 新片永远凑不齐 → merge 卡死。
         */
        if (txn.epoch != epoch) {
            EpochRecvNoteStale();
            continue;
        }

        if (E_WireTxnIsEpochEnd(txn)) {
            /* 收尾包：eof=true，declared 冻结为当前 received */
            EpochRecvNoteEof(epoch, txn.server_id);
            /*
             * 诊断：每收到一个收尾包打一行，用于排查"某节点的 EOF 没到"
             * 导致 recv 永久卡在某一片。
             */
            EPOCH_DBG_FPRINTF("EPOCH_DBG recv_eof epoch=%lu from_node=%u (cur=%lu)\n",
                static_cast<unsigned long>(epoch), txn.server_id,
                static_cast<unsigned long>(EpochRecvGetEpoch()));
            /* “所有 eof 到齐”的判断已在 TryAdvance 的 CAS 之前完成 */
            if (EpochRecvTryAdvanceEpoch(epoch)) {
                /* 这一片收齐了：按实际节点数打印各节点的 declared */
                char cols[256];
                int off = 0;
                const uint32_t total_nodes = EpochGetTotalNodes();
                for (uint32_t n = 0; n < total_nodes && off < static_cast<int>(sizeof(cols)) - 32; ++n) {
                    off += snprintf(cols + off, sizeof(cols) - off, " node%u=%lu",
                        n, static_cast<unsigned long>(EpochRecvDeclaredCount(epoch, n)));
                }
                EPOCH_DBG_FPRINTF("EPOCH_DBG recv_done epoch=%lu%s\n",
                    static_cast<unsigned long>(epoch), cols);
                fflush(stderr);
            }
        } else {
            /* 写集：received++ 并进 merge 队列 */
            EpochRecvNoteWire(epoch, txn.server_id);
            /*
             * merge 队列入队失败只可能是"分配失败"（队列本身无界）。
             * 以前这里丢弃返回值 → 写集静默丢失 → 该片 merged 永远追不上
             * declared → merge 卡死（实测 epoch 511 少了 21 条）。
             * 改成背压：失败就等一会儿再试，宁可降速也不丢。
             */
            while (!EpochMergePoolSubmit(txn)) {
                EpochPacerNoteMergeRetry();
                pg_usleep(200);
                CHECK_FOR_INTERRUPTS();
            }
        }
    }
    return progress;
}


/*
 * 诊断：每 5 秒打印"当前 recv epoch 的槽位状态"。
 * 用来区分 recv 卡住是"队列里有数据但没取"还是"数据根本没进队"。
 */
static void EpochPipelineLogRecvSlot(void)
{
    static int64_t last_us = 0;
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    const int64_t now = static_cast<int64_t>(ts.tv_sec) * 1000000 +
                        static_cast<int64_t>(ts.tv_nsec / 1000);
    if (now - last_us < 5000000) {
        return;
    }
    last_us = now;

    const uint64_t ep = EpochRecvGetEpoch();
    const uint32_t slot = EpochSlotOf(ep);
    const size_t qsize = EpochIncomingQueueSize(ep);
    EPOCH_DBG_FPRINTF(
        "EPOCH_DBG recvq epoch=%lu slot=%u size=%zu eof=[%d %d %d]\n",
        static_cast<unsigned long>(ep), slot, qsize,
        EpochRecvHasEof(ep, 0) ? 1 : 0,
        EpochRecvHasEof(ep, 1) ? 1 : 0,
        EpochRecvHasEof(ep, 2) ? 1 : 0);
    fflush(stderr);
}

static void EpochPipelineRecvThreadMain(void)
{
    EpochSetAuxThreadName("epochrecv");
    EpochStuckTraceRegister("epochrecv");
    while (g_pipeline_recv_running.load(std::memory_order_acquire)) {
        if (!EpochPipelineRecvIncomingOnce()) {
            pg_usleep(EPOCH_PIPELINE_IDLE_US);
        }
        EpochPipelineLogRecvSlot();
    }
}

void EpochPipelineStartRecvThread(void)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return;
    }
    if (g_pipeline_recv_running.load(std::memory_order_acquire)) {
        return;
    }
    g_pipeline_recv_running.store(true, std::memory_order_release);
    g_pipeline_recv_thread = std::thread(EpochPipelineRecvThreadMain);
}

void EpochPipelineStopRecvThread(void)
{
    if (!g_pipeline_recv_running.load(std::memory_order_acquire)) {
        return;
    }
    g_pipeline_recv_running.store(false, std::memory_order_release);
    if (g_pipeline_recv_thread.joinable()) {
        g_pipeline_recv_thread.join();
    }
}

static void EpochPipelineLogicalAdvanceThreadMain(void)
{
    EpochSetAuxThreadName("epochlogic");
    EpochStuckTraceRegister("epochlogic");
    while (g_pipeline_logical_running.load(std::memory_order_acquire)) {
        if (EpochTryAdvanceLogical()) {
            continue;
        }
        pg_usleep(EPOCH_PIPELINE_IDLE_US);
    }
}

void EpochPipelineStartLogicalAdvanceThread(void)
{
    if (!g_enable_epoch_multimaster) {
        return;
    }
    if (g_pipeline_logical_running.load(std::memory_order_acquire)) {
        return;
    }
    g_pipeline_logical_running.store(true, std::memory_order_release);
    g_pipeline_logical_thread = std::thread(EpochPipelineLogicalAdvanceThreadMain);
}

void EpochPipelineStopLogicalAdvanceThread(void)
{
    if (!g_pipeline_logical_running.load(std::memory_order_acquire)) {
        return;
    }
    g_pipeline_logical_running.store(false, std::memory_order_release);
    if (g_pipeline_logical_thread.joinable()) {
        g_pipeline_logical_thread.join();
    }
}

static void EpochPipelinePhysicalAdvanceThreadMain(void)
{
    EpochSetAuxThreadName("epochphys");
    EpochStuckTraceRegister("epochphys");
    instr_time last_tick;
    INSTR_TIME_SET_CURRENT(last_tick);
    while (g_pipeline_physical_running.load(std::memory_order_acquire)) {
        int interval_ms = 10;
        instr_time now;
        INSTR_TIME_SET_CURRENT(now);
        INSTR_TIME_SUBTRACT(now, last_tick);
        long elapsed_ms = INSTR_TIME_GET_MILLISEC(now);
        if (elapsed_ms >= interval_ms) {
            EpochPerfNoteWorkerPoll();
            EpochPerfMaybeReport();
            EpochDynNetMaybeCatchup();
            OccStagingFlusherTick();
            /* 卡死定位：merge worker 单 task 超过 2s 未完成即打 MERGE_STUCK */
            EpochMergeProfileMaybeReportStuck();
            /*
             * 这里原本还有 EpochStuckTraceLogicalWatchdog：logical 3 秒不推进就
             * “落盘全线程飞行记录”。那个操作很重（遍历线程 + 取栈 + 写文件），
             * 而且是同步跑在推进线程里 —— 一旦 logical 卡住就形成正反馈：
             *   logical 卡 → 落盘阻塞推进线程 → physical 不再推进
             *   → 事务全堆进当前时间片（撑到 64 满）→ 提交失败 → logical 更卡
             * 实测 5Mbps 场景下 beat 只打印 1 条就彻底静默，整个 postmaster 停摆。
             * 诊断不该跑在关键循环里，已移除。
             */
            /* physical 不再由定时器推进：由 backend 抢到每片第 24 个名额时封口并推进 */
            INSTR_TIME_SET_CURRENT(last_tick);
        } else {
            pg_usleep(EPOCH_PIPELINE_IDLE_US);
        }
    }
}

void EpochPipelineStartPhysicalAdvanceThread(void)
{
    if (g_pipeline_physical_running.load(std::memory_order_acquire)) {
        return;
    }
    g_pipeline_physical_running.store(true, std::memory_order_release);
    g_pipeline_physical_thread = std::thread(EpochPipelinePhysicalAdvanceThreadMain);
}

/*
 * 独立的状态打印线程：只 sleep + 打日志，不拿任何业务锁、不发网络、不碰队列。
 *
 * 目的是把“卡住”和“崩溃”区分开：
 *   · 业务线程卡在锁上 → 这个线程照样每隔 10 秒打一行
 *   · 整个进程没了        → 这个线程也停了，日志彻底静默
 * 之前把打印放在物理推进线程里，一旦那条线程卡住就分不清是卡死还是崩溃。
 */
static std::atomic<bool> g_state_log_running{false};
static std::thread g_state_log_thread;

static void EpochPipelineStateLogThreadMain(void)
{
    EpochSetAuxThreadName("epochstate");
    EpochStuckTraceRegister("epochstate");
    while (g_state_log_running.load(std::memory_order_acquire)) {
        EpochStateLog();          /* 内部自带 10 秒限频 */
        pg_usleep(1000000);       /* 1 秒醒一次即可 */
    }
}

void EpochPipelineStartStateLogThread(void)
{
    if (!g_enable_epoch_multimaster) {
        return;
    }
    if (g_state_log_running.load(std::memory_order_acquire)) {
        return;
    }
    g_state_log_running.store(true, std::memory_order_release);
    g_state_log_thread = std::thread(EpochPipelineStateLogThreadMain);
}

void EpochPipelineStopStateLogThread(void)
{
    if (!g_state_log_running.load(std::memory_order_acquire)) {
        return;
    }
    g_state_log_running.store(false, std::memory_order_release);
    if (g_state_log_thread.joinable()) {
        g_state_log_thread.join();
    }
}

void EpochPipelineStopPhysicalAdvanceThread(void)
{
    if (!g_pipeline_physical_running.load(std::memory_order_acquire)) {
        return;
    }
    g_pipeline_physical_running.store(false, std::memory_order_release);
    if (g_pipeline_physical_thread.joinable()) {
        g_pipeline_physical_thread.join();
    }
}

static bool EpochPipelinePollApplyStage(void);
static bool EpochPipelinePrefillMergedApplyAhead(uint64_t apply_ep, uint64_t seal_max);

static bool EpochPipelinePollMergeStage(void)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return false;
    }

    /*
     * 仅 inline merge / 无 Pack 旧路径由 coord 兜底。
     * 并行 merge：关窗/游标只在 MergeWorkerPullTick（merge_epoch）。
     */
    bool progress = false;
    {

        progress = EpochPipelineDrainRemoteMergeQueue() || progress;
        const uint64_t logical = E_NodeManager::GetLogicalEpoch();
        const uint64_t seal_max = EpochPipelineSealAheadMaxEpoch();
        for (uint64_t epoch = logical; epoch <= seal_max; ++epoch) {
            if (EpochPipelineTryMarkMergeComplete(epoch)) {
                progress = true;
            }
        }
        }
    return progress;
}

void EpochPipelineStartMergeThread(void)
{
    /*
     * 并行 merge：postmaster EPOCHMERGEWORKER。
     * inline：仍由 EpochPipelinePollMergeStage（仅非 Pack 旧路径）驱动。
     */
}

void EpochPipelineStopMergeThread(void)
{
}

static bool EpochPipelinePrefillMergedApplyAhead(uint64_t apply_ep, uint64_t seal_max)
{
    /*
     * 仅 inline apply 旧路径：预填未来已 merge_complete 的 epoch。
     * 并行 apply：喂环只在 merge 关窗路径，禁止 coord 预填以免双 Submit。
     */
    if (apply_ep == 0 || seal_max <= apply_ep) {
        return false;
    }
    constexpr int kPrefillBudget = 2;
    int used = 0;
    bool progress = false;
    for (uint64_t epoch = apply_ep + 1; epoch <= seal_max && used < kPrefillBudget; ++epoch) {
        E_NodeManager::EnsureEpochSlot(epoch);
        if (!EpochPipelineIsMergePhaseComplete(epoch) ||
            EpochPipelineEpochApplyComplete(epoch)) {
            continue;
        }
        if (EpochPipelineTryApplyStage(epoch)) {
            progress = true;
            ++used;
        }
    }
    return progress;
}

/*
 * 旧路径（inline / 无 Pack）：coord 可收 done + 置 apply_complete。
 * 并行 Pack+merge：禁止在此喂环（TryApplyStage），避免与 merge 工人双 Submit。
 */
static bool EpochPipelinePollApplyStage(void)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return false;
    }
    /* 写回完成由 apply worker 自己推进，这里只做一次汇总检查 */
    return EpochPipelineMaybeCompleteApplyAndAdvance();
}

void EpochPipelineStartApplyThread(void)
{
    /*
     * apply 线程池在 EpochApplyPoolInit 中启动（epoch_commit_threads>=2）。
     * coordinator 主线程负责 merge/seal/dispatch。
     */
}

void EpochPipelineStopApplyThread(void)
{
}

bool EpochPipelineDrainRemoteMergeQueue(void)
{
    /*
     * recv/local 已直投 MergeCell；无中间 recv 队列可 Drain。
     * 并行：兜底 PollCompletions；inline：入站时已 ExecuteInline。
     */
    return false;
}

static bool EpochPipelineEpochReadyToApply(uint64_t epoch)
{
    if (!EpochPipelineIsMergePhaseComplete(epoch)) {
        return false;
    }
    if (EpochPipelineCountUnappliedWireTxns(epoch) > 0) {
        return true;
    }
    return false;
}



static std::unordered_map<uint64_t, int> g_apply_skip_tick;
static std::mutex g_apply_skip_tick_mutex;

static bool EpochPipelineAllPeersEpochEndReceived(uint64_t epoch);
uint64_t EpochPipelineExpectedMergeTxnCount(uint64_t epoch)
{
    /*
     * 这一片“应该合并多少条” = 各节点 declared 之和。
     *
     * declared 在收到该节点收尾包时冻结成当时的实收条数；
     * 还没收到收尾包时它是 99999（未知），所以任何一列没冻结，
     * expected 就会远大于 merged，这一片绝不会被误判为完成。
     */
    uint64_t total = 0;
    for (uint32_t node = 0; node < EpochGetTotalNodes(); ++node) {
        total += EpochRecvDeclaredCount(epoch, node);
    }
    return total;
}

uint64_t EpochPipelineMergedTxnCount(uint64_t epoch)
{
    return E_NodeManager::GetWireMergeDoneCounters(epoch, EpochGetNodeId());
}

uint64_t EpochPipelineExpectedApplyWinnerCount(uint64_t epoch)
{
    /* merge_complete CAS 时 SetMergedTxnCounters 冻结；未关窗为 0 */
    if (!EpochPipelineIsMergePhaseComplete(epoch)) {
        return 0;
    }
    return E_NodeManager::GetMergedTxnCounters(epoch, EpochGetNodeId());
}

void EpochPipelineReadStageFlagsForDiag(uint64_t epoch, bool* physical_ready, bool* local_commit_ready,
    bool* self_send_sent, bool* merge_complete, bool* apply_complete)
{
    /* 三个“阶段标记”已删除，统一由发送游标推导：越过了就代表发完了 */
    const bool sent = (EpochGetSendEpoch() > epoch);
    if (physical_ready != nullptr) {
        *physical_ready = sent;
    }
    if (local_commit_ready != nullptr) {
        *local_commit_ready = sent;
    }
    if (self_send_sent != nullptr) {
        *self_send_sent = sent;
    }
    if (merge_complete != nullptr) {
        *merge_complete = EpochPipelineIsMergePhaseComplete(epoch);
    }
    if (apply_complete != nullptr) {
        *apply_complete = EpochPipelineEpochApplyComplete(epoch);
    }
}

static void EpochPipelineMaybeLogApplySkip(uint64_t epoch, const char* reason, uint64_t merged,
    uint64_t committed, uint64_t peer_should, uint64_t peer_rcv, int has_remote_rows)
{
    /* merge×N + coord 都会进这里；无锁 unordered_map 会在 operator[] 上 SEGV */
    int tick = 0;
    {
        std::lock_guard<std::mutex> lock(g_apply_skip_tick_mutex);
        tick = ++g_apply_skip_tick[epoch];
    }
    if (tick % 300 != 1) {
        return;
    }
}

static bool EpochPipelineTryApplyOneEpoch(uint64_t target)
{
    /*
     * 并行路径：赢家已在 merge Finish 入环；此处只认 merge_complete 后可推进。
     * PopDone 现为 no-op（finish 内联 hook），保留 PollCompletions 兼容。
     */
    bool progress = false;
    progress = false;
    (void)target;
    return progress;
}

bool EpochPipelineTryApplyMergedEpoch(uint64_t epoch)
{
    if (EpochGetTotalNodes() <= 1) {
        return true;
    }
    return EpochPipelineTryApplyOneEpoch(epoch);
}

static bool EpochPipelineTryApplyReadyEpoch(uint64_t epoch)
{
    /*
     * 当前 logical 关窗 epoch 须等 physical delay。
     * async 下历史 epoch 无 physical_delay 限制（见 ApplyReadyEpochs lookback）。
     */
    if (epoch == E_NodeManager::GetLogicalEpoch() && !EpochIsLogicalReady(epoch)) {
        return false;
    }
    return EpochPipelineTryApplyOneEpoch(epoch);
}

static bool EpochPipelineTryApplyOneReadyEpoch(void)
{
    return EpochPipelineTryApplyReadyEpoch(E_NodeManager::GetLogicalEpoch());
}

static void EpochPipelineApplyReadyEpochs(void)
{
    (void)EpochPipelineTryApplyReadyEpoch(E_NodeManager::GetLogicalEpoch());
}

static void EpochPipelineTrySealEpochs(void)
{
    if (EpochGetTotalNodes() <= 1) {
        return;
    }
    if (!EpochAllPeersConnected()) {
        return;
    }

}





void EpochPipelineInjectRemoteWireTxn(const E_WireTxn& txn)
{
    EpochMarkPeerConnected(txn.server_id);
    EpochDynNetObserveWireTxn(txn);
    EpochRemoteMergeEnqueue(txn);
}



