#include "postgres.h"

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
#include "epoch/e_merge_shmem.h"
#include "epoch/e_net.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_statistician.h"
#include "epoch/e_trace.h"
#include "epoch/e_shmem.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_tip_staging_shmem.h"
#include "epoch/e_pk_staging_shmem.h"
#include "epoch/e_txn_queue.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_wire_envelope.h"
#include "epoch/e_send_queue.h"
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
/* 按 epoch/peer 累计已收 wire txn；slot 被 Clear 后 dedupe 不重计，靠此恢复 received */
static std::unordered_map<uint64_t, std::array<uint64_t, EPOCH_MAX_NODES>> g_peer_received_counts;

/* merge 在途：看 apply/merge 环 occupancy，不另维护 wire_key inflight set */
static bool EpochPipelineEpochHasAsyncMergeInflight(uint64_t epoch)
{
    if (EpochMergePoolUsesInlineMerge()) {
        return false;
    }
    return EpochMergeQueueInflightForEpoch(epoch) > 0;
}

static void EpochPipelineMarkWireTxnApplied(uint64_t epoch, uint64_t wire_key);
static bool EpochPipelineIsWireTxnApplied(uint64_t epoch, uint64_t wire_key);
static bool EpochPipelineFindWireTxn(uint64_t epoch, uint64_t wire_key, E_WireTxn* out);
static bool EpochPipelineWireTxnHasWritableRows(const E_WireTxn& txn);
static void EpochPipelineSkipWireTxnForApply(uint64_t epoch, uint64_t wire_key, bool is_merge_abort);
/* GeoGauss：入环必须成功；失败一律 ERROR，禁止 pending 重试 */
static void EpochPipelineEnqueueApplyWinner(uint64_t epoch, uint64_t wire_key, uint64_t csn, uint32_t server_id,
    const E_WireTxn* orig_opt, const char* payload_opt, uint32_t payload_len);

static void EpochPipelineOnAsyncMergeTaskDone(const EpochMergeDoneSlot* done)
{
    if (done == nullptr) {
        return;
    }
    uint32_t node_id = EpochGetNodeId();
    /* GeoGauss：每笔 merge Finish → IncMerged（wire_merge_done）；计数齐了才 merge_complete */
    (void)E_NodeManager::IncWireMergeDoneCounters(done->epoch, node_id);

    if (!done->ok) {
        EpochConcurrent::U64SetErase(g_merge_accepted_wires, done->epoch, done->wire_key);
        return;
    }

    /*
     * GeoGauss：赢家 merge 完成即入 apply 环（Claim 仍须 merge_complete）。
     * 输家 Skip，不计入 apply expected。入环失败 → ERROR（无 pending）。
     */
    if (done->txn_accepted == 0) {
        if (EpochPipelineFindWireTxn(done->epoch, done->wire_key, nullptr)) {
            EpochPipelineSkipWireTxnForApply(done->epoch, done->wire_key, true);
        }
        return;
    }

    E_WireTxn orig;
    if (!EpochPipelineFindWireTxn(done->epoch, done->wire_key, &orig)) {
        ereport(ERROR,
            (errmsg("epoch: merge winner missing Remember wire epoch=%lu wire_key=%lu",
                static_cast<unsigned long>(done->epoch),
                static_cast<unsigned long>(done->wire_key))));
    }
    /* 与 CountApplyWinners 对齐：无可写行不入 apply、不 IncCommitted */
    if (!EpochPipelineWireTxnHasWritableRows(orig)) {
        EpochPipelineMarkWireTxnApplied(done->epoch, done->wire_key);
        return;
    }
    if (EpochAbortCsnContains(done->epoch, orig.csn, orig.server_id)) {
        EpochPipelineSkipWireTxnForApply(done->epoch, done->wire_key, true);
        return;
    }
    EpochPipelineEnqueueApplyWinner(done->epoch, done->wire_key, orig.csn, orig.server_id, &orig, nullptr, 0);
}

static void EpochPipelineOnAsyncApplyTaskDone(uint64_t epoch, uint64_t wire_key, bool ok)
{
    if (ok) {
        EpochPipelineMarkWireTxnApplied(epoch, wire_key);
    }
}

static void EpochPipelineSkipWireTxnForApply(uint64_t epoch, uint64_t wire_key, bool is_merge_abort)
{
    if (EpochPipelineIsWireTxnApplied(epoch, wire_key)) {
        return;
    }
    EpochPipelineMarkWireTxnApplied(epoch, wire_key);
    if (is_merge_abort) {
        EpochApplyStatsNoteAbort(OCC_ABORT_REMOTE_MERGE);
    }
}

static bool EpochPipelineDrainLocalQueueForEpoch(uint64_t epoch);
static bool EpochPipelineDrainLocalQueueWindow(void);
static void EpochPipelineMaybeResealEpoch(uint64_t epoch);
static bool EpochPipelineTrySealOneEpoch(uint64_t epoch);

static uint64_t EpochPipelineWireTxnKey(const E_WireTxn& txn)
{
    return txn.csn ^ (static_cast<uint64_t>(txn.server_id) << 48);
}

static bool EpochPipelineWireTxnHasWritableRows(const E_WireTxn& txn)
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

static bool EpochPipelineIsWireTxnApplied(uint64_t epoch, uint64_t wire_key)
{
    return EpochConcurrent::U64SetContains(g_applied_wire_txns, epoch, wire_key);
}

static void EpochPipelineMarkWireTxnApplied(uint64_t epoch, uint64_t wire_key)
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
 */
static void EpochPipelineEnqueueApplyWinner(uint64_t epoch, uint64_t wire_key, uint64_t csn, uint32_t server_id,
    const E_WireTxn* orig_opt, const char* payload_opt, uint32_t payload_len)
{
    if (EpochPipelineIsWireTxnApplied(epoch, wire_key)) {
        return;
    }
    if (EpochAbortCsnContains(epoch, csn, server_id)) {
        EpochPipelineSkipWireTxnForApply(epoch, wire_key, true);
        return;
    }

    E_WireTxn orig_storage;
    const E_WireTxn* orig = orig_opt;
    if (orig == nullptr) {
        if (!EpochPipelineFindWireTxn(epoch, wire_key, &orig_storage)) {
            ereport(ERROR,
                (errmsg("epoch: apply enqueue missing wire epoch=%lu wire_key=%lu",
                    static_cast<unsigned long>(epoch),
                    static_cast<unsigned long>(wire_key))));
        }
        orig = &orig_storage;
        csn = orig->csn;
        server_id = orig->server_id;
    }
    if (!EpochPipelineWireTxnHasWritableRows(*orig)) {
        EpochPipelineSkipWireTxnForApply(epoch, wire_key, false);
        return;
    }

    uint32_t local_node = EpochGetNodeId();
    bool is_local = (orig->server_id == local_node);

    bool submitted = false;
    if (!EpochApplyPoolUsesInlineApply()) {
        const char* payload = payload_opt;
        uint32_t len = payload_len;
        std::string owned;
        if (payload == nullptr || len == 0) {
            if (!E_WireSerializeTxn(*orig, &owned)) {
                ereport(ERROR,
                    (errcode(ERRCODE_OUT_OF_MEMORY),
                        errmsg("epoch: serialize apply winner failed epoch=%lu csn=%lu",
                            static_cast<unsigned long>(epoch),
                            static_cast<unsigned long>(csn))));
            }
            payload = owned.data();
            len = static_cast<uint32_t>(owned.size());
        }
        /* EpochApplyQueueSubmit 满/异常时直接 ERROR，无 soft-fail */
        /* 入环存原始 wire；apply 直接用 wire + tip/pk staging，不再 BuildApplyTxn */
        submitted = EpochApplyPoolSubmitSerialized(epoch,
            wire_key,
            local_node,
            is_local,
            payload,
            len,
            0);
    } else {
        if (!EpochPipelineEpochMergeComplete(epoch)) {
            ereport(ERROR,
                (errmsg("epoch: inline apply enqueue before merge_complete epoch=%lu",
                    static_cast<unsigned long>(epoch))));
        }
        EpochApplyPoolTask task;
        task.epoch = epoch;
        task.wire_key = wire_key;
        task.local_node = local_node;
        task.is_local = is_local;
        task.need_crdt_build = false;
        task.apply_txn = *orig;
        submitted = EpochApplyPoolSubmit(task);
    }

    if (!submitted) {
        ereport(ERROR,
            (errmsg("epoch: apply enqueue failed epoch=%lu wire_key=%lu; "
                    "increase epoch_apply_queue_capacity (no pending retry)",
                static_cast<unsigned long>(epoch),
                static_cast<unsigned long>(wire_key))));
    }

    EpochLifecycleNoteApplyFirstSubmit(epoch);
    EpochPipelineMarkWireTxnApplied(epoch, wire_key);
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
    EpochPipelineEnqueueApplyWinner(txn.epoch, wire_key, txn.csn, txn.server_id, &txn, payload, payload_len);
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

struct EpochLogicalStageFlags {
    /* 主 worker seal 推进用；不参与 logical++ */
    std::atomic<bool> physical_ready{false};
    std::atomic<bool> local_commit_ready{false};
    /* SealEpoch 已入队 epoch-end（含 pack=0）；真正 send_completed 仍等 publish */
    std::atomic<bool> seal_enqueued{false};
    std::atomic<bool> self_send_sent{false};
    /* logical++：recv 同步 + apply 结束 */
    std::atomic<bool> merge_complete{false};
    std::atomic<bool> apply_complete{false};
};

static EpochConcurrent::UnorderedMap<uint64_t, EpochLogicalStageFlags> g_logical_stage_flags;
static EpochConcurrent::UnorderedMap<uint64_t, std::array<bool, EPOCH_MAX_NODES>> g_peer_epoch_end_received;
/* epoch-end 已设置 should_receive 且 received>=should 时置位，供 merge 关窗 */
static EpochConcurrent::UnorderedMap<uint64_t, std::array<bool, EPOCH_MAX_NODES>> g_peer_txn_wire_received;

static bool EpochPipelinePeerFlagGet(
    const EpochConcurrent::UnorderedMap<uint64_t, std::array<bool, EPOCH_MAX_NODES>>& map,
    uint64_t epoch,
    uint32_t peer)
{
    std::array<bool, EPOCH_MAX_NODES> flags{};
    if (!map.Find(epoch, &flags)) {
        return false;
    }
    return flags[peer];
}

static void EpochPipelinePeerFlagSet(
    EpochConcurrent::UnorderedMap<uint64_t, std::array<bool, EPOCH_MAX_NODES>>& map,
    uint64_t epoch,
    uint32_t peer,
    bool value)
{
    map.Modify(epoch, [peer, value](std::array<bool, EPOCH_MAX_NODES>& flags, bool /*is_new*/) {
        flags[peer] = value;
    });
}

void EpochPipelineNotePeerTxnWireProgress(uint64_t epoch, uint32_t from_node);

void EpochPipelineNotePeerEpochEndReceived(uint64_t epoch, uint32_t from_node)
{
    if (from_node >= EPOCH_MAX_NODES) {
        return;
    }
    g_peer_epoch_end_received.Modify(epoch, [from_node](std::array<bool, EPOCH_MAX_NODES>& flags, bool /*is_new*/) {
        flags[from_node] = true;
    });
    EpochRecvDiagLogPeerEpochEndMark(epoch, from_node, E_NodeManager::GetLogicalEpoch());
    EpochPipelineNotePeerTxnWireProgress(epoch, from_node);
}

bool EpochPipelinePeerEpochEndReceived(uint64_t epoch, uint32_t peer)
{
    if (peer >= EPOCH_MAX_NODES) {
        return false;
    }
    return EpochPipelinePeerFlagGet(g_peer_epoch_end_received, epoch, peer);
}

void EpochPipelineFinishPeerRecvAfterEpochEnd(uint64_t epoch, uint32_t from_node)
{
    /*
     * NotePeerEpochEndReceived 在 CheckAndSetNodeReceiveCompleted 之前调用；
     * recv 此处才齐时再标记 peer txn wire 完成，供 merge 关窗。
     */
    EpochPipelineNotePeerTxnWireProgress(epoch, from_node);
}

bool EpochPipelineEpochHasMergeInflight(uint64_t epoch)
{
    return EpochPipelineEpochHasAsyncMergeInflight(epoch);
}

void EpochPipelineNotePeerTxnWireProgress(uint64_t epoch, uint32_t from_node)
{
    if (from_node >= EPOCH_MAX_NODES || from_node == EpochGetNodeId()) {
        return;
    }
    if (!EpochPipelinePeerFlagGet(g_peer_epoch_end_received, epoch, from_node)) {
        /* should_receive 由 epoch-end 写入；txn 先到也须等 epoch-end */
        return;
    }
    if (!E_NodeManager::IsNodeReceiveCompleted(epoch, from_node)) {
        return;
    }
    bool was = EpochPipelinePeerFlagGet(g_peer_txn_wire_received, epoch, from_node);
    EpochPipelinePeerFlagSet(g_peer_txn_wire_received, epoch, from_node, true);
    if (!was) {
        EpochRecvDiagLogPeerTxnWireDone(epoch,
            from_node,
            E_NodeManager::GetStoreShouldReceivedPackCounters(epoch, from_node),
            E_NodeManager::GetStoreReceivedPackCounters(epoch, from_node));
    }
}

static void EpochPipelineMaybeLogMergePeerEndGate(uint64_t epoch)
{
    static std::unordered_map<uint64_t, int> g_merge_peer_end_gate_tick;
    int& tick = g_merge_peer_end_gate_tick[epoch];
    tick++;
    if (tick != 1 && tick % 200 != 1) {
        return;
    }

    /* 周期性补发：对端可能曾 FUTURE_DROP 本端超前 seal 的 epoch-end */
    EpochRetransmitSealedEpochEnd(epoch);

    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    uint32_t self = EpochGetNodeId();
    char detail[320];
    int offset = 0;
    for (uint32_t peer = 0; peer < EpochGetTotalNodes(); ++peer) {
        if (peer == self) {
            continue;
        }
        bool end_mark = EpochPipelinePeerFlagGet(g_peer_epoch_end_received, epoch, peer);
        bool txn_done = EpochPipelinePeerFlagGet(g_peer_txn_wire_received, epoch, peer);
        bool send_done = E_NodeManager::IsNodeSendCompleted(epoch, peer);
        bool recv_done = E_NodeManager::IsNodeReceiveCompleted(epoch, peer);
        uint64_t should = E_NodeManager::GetStoreShouldReceivedPackCounters(epoch, peer);
        uint64_t got = E_NodeManager::GetStoreReceivedPackCounters(epoch, peer);
        int rc = snprintf_s(detail + offset,
            sizeof(detail) - static_cast<size_t>(offset),
            sizeof(detail) - static_cast<size_t>(offset) - 1,
            " p%u_end=%d p%u_txn=%d p%u_send=%d p%u_recv=%d p%u_should=%lu p%u_got=%lu",
            peer,
            end_mark ? 1 : 0,
            peer,
            txn_done ? 1 : 0,
            peer,
            send_done ? 1 : 0,
            peer,
            recv_done ? 1 : 0,
            peer,
            should,
            peer,
            got);
        securec_check_ss(rc, "\0", "\0");
        offset += rc;
        if (offset >= static_cast<int>(sizeof(detail)) - 1) {
            break;
        }
    }
    EpochRecvDiagLogMergePeerEndGate(epoch, logical, detail);
}

static bool EpochPipelineAllPeersEpochEndReceived(uint64_t epoch)
{
    uint32_t self = EpochGetNodeId();
    uint32_t total = EpochGetTotalNodes();
    for (uint32_t peer = 0; peer < total; ++peer) {
        if (peer == self) {
            continue;
        }
        if (!EpochPeerIsConnected(peer)) {
            return false;
        }
        if (!EpochPipelinePeerFlagGet(g_peer_epoch_end_received, epoch, peer)) {
            return false;
        }
    }
    return true;
}

static bool EpochPipelineAllPeersTxnWireReceived(uint64_t epoch)
{
    uint32_t self = EpochGetNodeId();
    uint32_t total = EpochGetTotalNodes();
    for (uint32_t peer = 0; peer < total; ++peer) {
        if (peer == self) {
            continue;
        }
        if (!EpochPeerIsConnected(peer)) {
            return false;
        }
        if (!EpochPipelinePeerFlagGet(g_peer_txn_wire_received, epoch, peer)) {
            return false;
        }
    }
    return true;
}

static EpochLogicalStageFlags* EpochPipelineGetStageFlags(uint64_t epoch);
static void EpochPipelineMarkSelfEpochEndSent(uint64_t epoch);

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

    if (!EpochIsPhysLogicalGapDisabled()) {
        const uint64_t gap = EpochGetPhysLogicalGapLimit();
        const uint64_t max_by_gap = logical + gap;
        if (max_by_phys > max_by_gap) {
            max_by_phys = max_by_gap;
        }
    }
    return max_by_phys;
}

/*
 * 对单个 epoch 推进 physical_ready / local_commit_ready / seal+send（可超前 logical）。
 */
static bool EpochPipelineTrySealOneEpoch(uint64_t epoch)
{
    E_NodeManager::EnsureEpochSlot(epoch);
    EpochLifecycleEnsureStarted(epoch);
    EpochLogicalStageFlags* flags = EpochPipelineGetStageFlags(epoch);
    bool progress = false;

    if (!flags->physical_ready.load(std::memory_order_acquire) && EpochIsLogicalReady(epoch)) {
        flags->physical_ready.store(true, std::memory_order_release);
        progress = true;
    }
    if (!flags->local_commit_ready.load(std::memory_order_acquire) && EpochIsLocalCommitStageReady(epoch)) {
        flags->local_commit_ready.store(true, std::memory_order_release);
        progress = true;
    }

    const bool seal_pre_ready = flags->physical_ready.load(std::memory_order_acquire) &&
                                flags->local_commit_ready.load(std::memory_order_acquire);
    if (seal_pre_ready && !flags->seal_enqueued.load(std::memory_order_acquire) &&
        !flags->self_send_sent.load(std::memory_order_acquire)) {
        /*
         * 只负责 enqueue seal/epoch-end；self_send_sent / send_completed 须等
         * DrainLocalQueue 真正 publish 成功后再置位，避免对端永远收不到 epoch-end。
         * seal_enqueued 防止 pack=0 时 sealed==0 被反复 SealEpoch。
         */
        if (EpochTrySealEpoch(epoch)) {
            flags->seal_enqueued.store(true, std::memory_order_release);
            progress = true;
        }
    }
    return progress;
}

/*
 * merge 完成：peer/self 门槛满足，且 wire_merge_done == expected（严格相等）。
 */
bool EpochPipelineIsMergePhaseComplete(uint64_t epoch)
{
    if (!EpochPipelineAllPeersEpochEndReceived(epoch)) {
        return false;
    }
    if (!EpochPipelineAllPeersTxnWireReceived(epoch)) {
        return false;
    }
    uint32_t self = EpochGetNodeId();
    if (!E_NodeManager::IsNodeSendCompleted(epoch, self)) {
        return false;
    }
    if (EpochPipelineEpochHasAsyncMergeInflight(epoch)) {
        return false;
    }
    const uint64_t expected = EpochPipelineExpectedMergeTxnCount(epoch);
    const uint64_t seen = EpochPipelineSeenMergeTxnCount(epoch);
    return seen == expected;
}

static EpochLogicalStageFlags* EpochPipelineGetStageFlags(uint64_t epoch)
{
    return g_logical_stage_flags.FindOrInsertPtr(epoch);
}

static bool EpochPipelineEpochHasUnappliedWireTxns(uint64_t epoch);

/*
 * GeoGauss：merge_complete 后 expected=MergedTxnCounters（关窗时冻结）；
 * apply Finish → IncCommitted；committed == expected 即 apply_complete（多计即有 bug）。
 * 另：若本 epoch apply 环仍有 task，即使计数碰巧相等也不关窗（防 expected=0 竞态误关）。
 */
static bool EpochPipelineIsApplyPhaseComplete(uint64_t epoch)
{
    const EpochLogicalStageFlags* flags = EpochPipelineGetStageFlags(epoch);
    if (!flags->merge_complete.load(std::memory_order_acquire)) {
        return false;
    }
    const uint32_t node_id = EpochGetNodeId();
    const uint64_t expected = E_NodeManager::GetMergedTxnCounters(epoch, node_id);
    const uint64_t committed = E_NodeManager::GetCommittedTxnCounters(epoch, node_id);
    if (committed != expected) {
        return false;
    }
    if (EpochApplyPoolHasPendingWorkForEpoch(epoch)) {
        return false;
    }
    if (EpochApplyQueueInflightForEpoch(epoch) > 0) {
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
            EpochDiagLogApplyPendingFirst(epoch,
                orig.csn,
                orig.server_id,
                wire_key,
                in_abort,
                writable,
                false,
                false);
            return;
        }
    });
}

static bool EpochPipelineEpochHasUnappliedWireTxns(uint64_t epoch)
{
    return EpochPipelineCountUnappliedWireTxns(epoch) > 0;
}

static bool EpochPipelineEpochReadyToApply(uint64_t epoch);

static void EpochPipelineMarkSelfEpochEndSent(uint64_t epoch)
{
    EpochLogicalStageFlags* flags = EpochPipelineGetStageFlags(epoch);
    if (flags->self_send_sent.load(std::memory_order_acquire)) {
        return;
    }
    uint32_t self = EpochGetNodeId();
    E_NodeManager::SetNodeSendCompleted(epoch, self, true);
    E_NodeManager::SetNodeReceiveCompleted(epoch, self, true);
    flags->seal_enqueued.store(true, std::memory_order_release);
    flags->self_send_sent.store(true, std::memory_order_release);
    EpochRecvDiagLogSent(epoch, E_NodeManager::GetLocalPackTxnCounters(epoch, self));
}

/*
 * logical 推进失败原因（细粒度）：与 AllStagesReady / IsMerge|ApplyPhaseComplete 对齐。
 * 供 STALL 时间加权明细（e_perf）定位 31ms 周期里的最长杆。
 */
static const char* EpochPipelineClassifyLogicalStall(uint64_t epoch)
{
    const EpochLogicalStageFlags* flags = EpochPipelineGetStageFlags(epoch);
    if (!flags->physical_ready.load(std::memory_order_acquire)) {
        return "physical_not_ready";
    }
    if (!flags->local_commit_ready.load(std::memory_order_acquire)) {
        return "local_commit_pending";
    }
    if (!flags->self_send_sent.load(std::memory_order_acquire)) {
        return "send_incomplete";
    }

    const bool merge_flag = flags->merge_complete.load(std::memory_order_acquire);
    if (!merge_flag || !EpochPipelineIsMergePhaseComplete(epoch)) {
        const uint32_t self = EpochGetNodeId();
        if (!E_NodeManager::IsNodeSendCompleted(epoch, self)) {
            return "merge_self_send";
        }
        if (!EpochPipelineAllPeersEpochEndReceived(epoch)) {
            return "merge_peer_end";
        }
        if (!EpochPipelineAllPeersTxnWireReceived(epoch)) {
            return "merge_peer_txn";
        }
        if (EpochPipelineEpochHasAsyncMergeInflight(epoch)) {
            return "merge_async";
        }
        if (EpochPipelineSeenMergeTxnCount(epoch) != EpochPipelineExpectedMergeTxnCount(epoch)) {
            return "merge_count";
        }
        return "merge_incomplete";
    }

    if (EpochPipelineEpochHasAsyncMergeInflight(epoch)) {
        return "apply_async_merge";
    }
    if (EpochApplyPoolHasPendingWorkForEpoch(epoch)) {
        return "apply_pool";
    }
    if (EpochPipelineEpochHasUnappliedWireTxns(epoch)) {
        return "apply_unapplied";
    }
    if (EpochPipelineMergerHasPendingRemoteApply(epoch)) {
        return "apply_remote_pending";
    }
    if (!flags->apply_complete.load(std::memory_order_acquire)) {
        return "apply_flag";
    }
    return "unknown";
}

void EpochPipelineNoteLogicalAdvanceStall(uint64_t epoch)
{
    const char* reason = EpochPipelineClassifyLogicalStall(epoch);
    if (strcmp(reason, "merge_peer_end") == 0 || strcmp(reason, "merge_peer_txn") == 0 ||
        strcmp(reason, "merge_incomplete") == 0) {
        EpochPipelineMaybeLogMergePeerEndGate(epoch);
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
    EpochRecvDiagLogStall(epoch, coarse);
    EpochDiagLogEpochStallSnapshot(epoch, coarse);
}

void EpochPipelineClearStageFlags(uint64_t epoch)
{
    EpochPipelineClearEpochWireTxnState(epoch, false);
    EpochPipelineScheduleCrdtClear(epoch);
    g_logical_stage_flags.Erase(epoch);
    g_peer_epoch_end_received.Erase(epoch);
    g_peer_txn_wire_received.Erase(epoch);
}

bool EpochPipelineAllStagesReady(uint64_t epoch)
{
    if (EpochGetTotalNodes() <= 1) {
        return false;
    }
    const EpochLogicalStageFlags* flags = EpochPipelineGetStageFlags(epoch);
    if (!flags->physical_ready.load(std::memory_order_acquire)) {
        return false;
    }
    if (!flags->local_commit_ready.load(std::memory_order_acquire)) {
        return false;
    }
    if (!flags->self_send_sent.load(std::memory_order_acquire)) {
        return false;
    }
    if (!flags->merge_complete.load(std::memory_order_acquire)) {
        return false;
    }
    if (!EpochPipelineIsMergePhaseComplete(epoch)) {
        return false;
    }
    if (!EpochPipelineIsApplyPhaseComplete(epoch)) {
        return false;
    }
    return true;
}

bool EpochPipelineEpochMergeComplete(uint64_t epoch)
{
    const EpochLogicalStageFlags* flags = EpochPipelineGetStageFlags(epoch);
    return flags->merge_complete.load(std::memory_order_acquire);
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
    EpochLogicalStageFlags* flags = EpochPipelineGetStageFlags(epoch);
    if (!flags->self_send_sent.load(std::memory_order_acquire)) {
        return false;
    }

    if (flags->merge_complete.load(std::memory_order_acquire)) {
        return false;
    }
    if (!EpochPipelineIsMergePhaseComplete(epoch)) {
        return false;
    }

    const uint64_t winners = EpochPipelineCountApplyWinners(epoch);
    E_NodeManager::SetMergedTxnCounters(epoch, EpochGetNodeId(), winners);

    bool expected = false;
    if (!flags->merge_complete.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        return false;
    }
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
    EpochApplyPoolEnsureApplyEpoch(logical);

    /*
     * 自愈：apply_epoch 曾因 merge_complete/expected=0 竞态越过 logical，
     * 旧 epoch 格里还有 task（HasPendingWork），Claim 却扫不到 → 永久 apply_pool。
     * 若 logical 的 apply 尚未真正完成，把游标拉回 logical 并清错置的 apply_complete。
     */
    if (!EpochApplyPoolUsesInlineApply()) {
        const uint64_t apply_ep = EpochApplyPoolGetApplyEpoch();
        if (apply_ep > logical && logical > 0 &&
            !EpochPipelineIsApplyPhaseComplete(logical)) {
            EpochLogicalStageFlags* lf = EpochPipelineGetStageFlags(logical);
            lf->apply_complete.store(false, std::memory_order_release);
            EpochApplyQueueSetApplyEpoch(logical);
            progress = true;
        }
    }

    for (;;) {
        const uint64_t apply_ep = EpochApplyPoolGetApplyEpoch();
        if (apply_ep == 0) {
            break;
        }
        E_NodeManager::EnsureEpochSlot(apply_ep);
        EpochLogicalStageFlags* flags = EpochPipelineGetStageFlags(apply_ep);
        if (!flags->apply_complete.load(std::memory_order_acquire)) {
            if (!flags->merge_complete.load(std::memory_order_acquire)) {
                break;
            }
            if (!EpochPipelineIsApplyPhaseComplete(apply_ep)) {
                break;
            }
            flags->apply_complete.store(true, std::memory_order_release);
            EpochLifecycleNoteApplyComplete(apply_ep);
            progress = true;
        }
        if (!EpochApplyPoolTryAdvanceApplyEpoch(apply_ep)) {
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
    if (EpochMergePoolUsesInlineMerge()) {
        return false;
    }
    const uint64_t me = EpochMergePoolGetMergeEpoch();
    if (me == 0) {
        return false;
    }
    if (!EpochPipelineEpochMergeComplete(me)) {
        return false;
    }
    const uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    if (me + 1 >= physical) {
        return false;
    }
    return EpochMergePoolTryAdvanceMergeEpoch(me);
}

/* 仅启动：merge_epoch 从 0 → 1；之后只靠 complete 后 ++ */
static void EpochPipelineBootstrapMergeEpochIfNeeded(void)
{
    if (EpochMergePoolGetMergeEpoch() != 0) {
        return;
    }
    if (E_NodeManager::GetPhysicalEpoch() <= 1) {
        return;
    }
    EpochMergePoolEnsureMergeEpoch(1);
}

bool EpochPipelineMergeWorkerPullTick(uint32_t worker_id, uint32_t n_workers)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return false;
    }
    if (EpochMergePoolUsesInlineMerge()) {
        return false;
    }

    EpochPipelineBootstrapMergeEpochIfNeeded();

    bool progress = false;
    (void)worker_id;
    (void)n_workers;

    /*
     * 自愈：logical 已越过但 merge_epoch 仍停在已 Clear 的格（竞态遗留）。
     * 只 CAS ++ 追到 logical，不按 logical Ensure 跳窗。
     */
    {
        uint64_t me = EpochMergePoolGetMergeEpoch();
        const uint64_t logical = E_NodeManager::GetLogicalEpoch();
        while (me > 0 && me < logical) {
            if (!EpochMergePoolTryAdvanceMergeEpoch(me)) {
                break;
            }
            progress = true;
            me = EpochMergePoolGetMergeEpoch();
        }
    }

    const uint64_t me = EpochMergePoolGetMergeEpoch();
    const uint64_t physical = E_NodeManager::GetPhysicalEpoch();
    /*
     * recv 已按 txn.epoch 直投 MergeCell；本 tick 只关窗 / ++merge_epoch。
     * Claim 只取 merge_epoch 行，未来 epoch 已在格里等游标。
     */
    if (me > 0 && me < physical) {
        if (EpochPipelineTryMarkMergeComplete(me)) {
            progress = true;
        }
        progress = EpochPipelineMaybeAdvanceMergeEpoch() || progress;
    }
    progress = EpochPipelineMaybeCompleteApplyAndAdvance() || progress;
    return progress;
}

static bool EpochPipelineTryApplyStage(uint64_t epoch, EpochLogicalStageFlags* flags)
{
    if (flags->apply_complete.load(std::memory_order_acquire)) {
        return false;
    }

    bool progress = false;

    /*
     * apply 须在 merge 全阶段结束后启动：对端 txn/epoch-end 未到齐前 CRDT 裁决可能变化，
     * 与 merge 并行会把尚未最终定稿的写集落盘。
     */
    if (!flags->merge_complete.load(std::memory_order_acquire)) {
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
                EpochRecvDiagLogLogicalApplyGate(epoch,
                    apply_ok ? 1 : 0,
                    merger_pending ? 1 : 0,
                    expected_winners,
                    committed);
            }
        }
    }

    if (EpochPipelineIsApplyPhaseComplete(epoch)) {
        flags->apply_complete.store(true, std::memory_order_release);
        EpochLifecycleNoteApplyComplete(epoch);
        progress = true;
    }

    return progress;
}

bool EpochPipelinePackSendActive(void)
{
    return g_enable_epoch_multimaster && EpochGetTotalNodes() > 1 && g_epoch_pack_threads >= 1;
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
    EpochPipelineStartRecvThread();
    EpochPipelineStartMergeThread();
    EpochPipelineStartApplyThread();
    EpochPipelineStartLogicalAdvanceThread();
    EpochPipelineStartPhysicalAdvanceThread();
    OccStagingEnsureInit();
    OccStagingInitEpochLocalTip();
    EpochDiagReset();
    EpochRecvDiagReset();
    EpochLifecycleReset();
    EpochPerfReset();

    g_pipeline_bootstrap_state.store(2, std::memory_order_release);
    ereport(LOG, (errmsg("epoch pipeline: BootstrapOwner ready (pack_send=%d, role=epochcoord)",
        EpochPipelinePackSendActive() ? 1 : 0)));
}

bool EpochPipelineAdvanceMainWorkerStages(void)
{
    if (!g_enable_epoch_multimaster || EpochGetTotalNodes() <= 1) {
        return false;
    }

    /*
     * Pack×N + Send×1：seal/drain 在 Pack，发包在 Send；epochcoord 只做 Bootstrap。
     * 无 Pack 旧路径：本函数仍由 EpochWorker 做 seal+drain。
     */
    if (EpochPipelinePackSendActive()) {
        return false;
    }

    /*
     * seal/send 可超前 logical：对 [logical, seal_ahead_max] 凡本地 pack 已齐且
     * physical 已越过 delay 的 epoch 尽早 seal。recv/merge 本就可处理 future；
     * 这样等 apply(N) 时 N+1 的 peer RTT/merge 可重叠。
     * apply 严格只 execute 当前 apply_epoch（可预 feed 未来 epoch 入队但不认领）。
     *
     * TxnQueue 按 epoch 分槽：drain 只 Pop 目标 epoch，禁止 PopAny 混槽，
     * 保证 seal(E) 后 epoch-end 尽快出门，避免 peer 收齐贴近 RTT。
     */
    bool progress = EpochPipelineDrainLocalQueueWindow();

    const uint64_t logical = E_NodeManager::GetLogicalEpoch();
    const uint64_t seal_max = EpochPipelineSealAheadMaxEpoch();

    for (uint64_t epoch = logical; epoch <= seal_max; ++epoch) {
        progress = EpochPipelineTrySealOneEpoch(epoch) || progress;
        /* seal 入队 epoch-end 后立刻只 drain 本 epoch */
        progress = EpochPipelineDrainLocalQueueForEpoch(epoch) || progress;
    }
    return progress;
}

bool EpochPipelinePackWorkerTick(uint32_t pack_id, uint32_t n_pack)
{
    (void)n_pack;
    if (!EpochPipelinePackSendActive()) {
        return false;
    }
    if (!EpochPipelineIsBootstrapped()) {
        return false;
    }

    bool progress = EpochPipelineDrainLocalQueueWindow();

    const uint64_t logical = E_NodeManager::GetLogicalEpoch();
    const uint64_t seal_max = EpochPipelineSealAheadMaxEpoch();

    for (uint64_t epoch = logical; epoch <= seal_max; ++epoch) {
        /* Seal 仅 Pack#0，避免多工人争 seal；其余 Pack 只 drain */
        if (pack_id == 0) {
            progress = EpochPipelineTrySealOneEpoch(epoch) || progress;
        }
        progress = EpochPipelineDrainLocalQueueForEpoch(epoch) || progress;
    }

    if (pack_id == 0 && logical > 0) {
        EpochPipelineMaybeResealEpoch(logical - 1);
    }
    return progress;
}

bool EpochPipelineSendWorkerTick(void)
{
    if (!EpochPipelinePackSendActive()) {
        return false;
    }

    /* 独占 PUB：hello 也走 Send，避免与 txn/epoch-end 并发 socket */
    EpochNetMaybePublishHello();

    EpochSendItem item;
    if (!EpochSendTryDequeue(&item)) {
        return false;
    }

    E_WireTxn txn;
    if (!E_WireDeserializeTxn(item.wire.data(), item.wire.size(), &txn)) {
        return true;
    }

    bool sent = EpochPipelineSendWire(item.wire, txn);
    if (item.is_epoch_end && !sent) {
        sent = EpochPipelineSendWire(item.wire, txn);
    }
    if (item.is_epoch_end) {
        if (!sent) {
            EpochRecvDiagLogEpochEndSend("FAIL", item.epoch, item.wire.size());
            if (!EpochTxnQueuePush(item.epoch, item.wire.data(), static_cast<uint32_t>(item.wire.size()))) {
                /* send 队列回灌：避免 epoch-end 丢失 */
                (void)EpochSendEnqueue(item.epoch, true, item.wire);
                ereport(WARNING,
                    (errmsg("epoch send: epoch-end send failed and re-queue full for epoch %lu", item.epoch)));
            }
            return true;
        }
        EpochRecvDiagLogEpochEndSend("OK", item.epoch, item.wire.size());
        EpochPerfNoteEpochEndSend(item.wire.size());
        EpochPipelineMarkSelfEpochEndSent(item.epoch);
        return true;
    }

    if (sent) {
        EpochPerfNoteWireTxnSend(txn, item.wire.size());
    }
    return true;
}

void EpochPipelineResyncPeerReceived(uint64_t epoch, uint32_t from_node)
{
    if (from_node >= EPOCH_MAX_NODES) {
        return;
    }
    auto it = g_peer_received_counts.find(epoch);
    if (it == g_peer_received_counts.end()) {
        return;
    }
    uint64_t tracked = it->second[from_node];
    uint64_t current = E_NodeManager::GetStoreReceivedPackCounters(epoch, from_node);
    if (tracked > current) {
        E_NodeManager::SetStoreReceivedPackCounters(epoch, from_node, tracked);
    }
    EpochPipelineNotePeerTxnWireProgress(epoch, from_node);
}

void EpochPipelineInit(void)
{
    g_merge_accepted_wires.Clear();
    g_epoch_wire_txns.Clear();
    g_applied_wire_txns.Clear();
    EpochApplyPoolSetTaskDoneHook(EpochPipelineOnAsyncApplyTaskDone);
    EpochMergePoolSetTaskDoneHook(EpochPipelineOnAsyncMergeTaskDone);
    g_logical_stage_flags.Clear();
    g_peer_epoch_end_received.Clear();
    g_peer_txn_wire_received.Clear();
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
        EpochApplyQueueShmemInit();
        EpochCrdtShmemInit();
        EpochMergeQueueShmemInit();
        EpochTipStagingShmemInit();
        EpochPkStagingShmemInit();
        EpochSendQueueInit();
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
    EpochSendQueueShutdown();
    EpochNetShutdown();
    g_merge_accepted_wires.Clear();
    g_epoch_wire_txns.Clear();
    g_applied_wire_txns.Clear();
    g_logical_stage_flags.Clear();
    g_peer_epoch_end_received.Clear();
    g_peer_txn_wire_received.Clear();
    g_pipeline_bootstrap_state.store(0, std::memory_order_release);
}

static bool EpochPipelineHandleDrainedLocalWire(const char* buffer, uint32_t len)
{
    std::string wire(buffer, buffer + len);
    E_WireTxn txn;
    if (!E_WireDeserializeTxn(wire.data(), wire.size(), &txn)) {
        return true;
    }

    if (EpochGetTotalNodes() <= 1) {
        if (E_WireTxnIsEpochEnd(txn)) {
            return true;
        }
        /* 本地已在 backend 经 O_RecordCommit 落盘，仅推进 epoch 计数 */
        E_NodeManager::IncCommittedTxnCounters(txn.epoch, EpochGetNodeId());
        return true;
    }

    if (E_WireTxnIsEpochEnd(txn)) {
        /* Pack→Send：epoch-end 只入 send 队列；MarkSelfEpochEndSent 在 Send 成功后 */
        if (EpochPipelinePackSendActive()) {
            if (!EpochSendEnqueue(txn.epoch, true, wire)) {
                EpochRecvDiagLogEpochEndSend("FAIL", txn.epoch, wire.size());
                if (!EpochTxnQueuePush(txn.epoch, wire.data(), len)) {
                    ereport(WARNING,
                        (errmsg("epoch: epoch-end send-enqueue failed and re-queue full for epoch %lu",
                            txn.epoch)));
                }
            }
            return true;
        }
        bool published = EpochPipelineSendWire(wire, txn);
        if (!published) {
            published = EpochPipelineSendWire(wire, txn);
        }
        if (!published) {
            EpochRecvDiagLogEpochEndSend("FAIL", txn.epoch, wire.size());
            if (!EpochTxnQueuePush(txn.epoch, wire.data(), len)) {
                ereport(WARNING,
                    (errmsg("epoch: epoch-end publish failed and re-queue full for epoch %lu",
                        txn.epoch)));
            }
            return true;
        }
        EpochRecvDiagLogEpochEndSend("OK", txn.epoch, wire.size());
        EpochPerfNoteEpochEndSend(wire.size());
        EpochPipelineMarkSelfEpochEndSent(txn.epoch);
        return true;
    }
    EpochRemoteMergeEnqueue(txn);
    EpochWireDiagLogTxnSend(txn);
    if (EpochPipelinePackSendActive()) {
        if (!EpochSendEnqueue(txn.epoch, false, wire)) {
            ereport(WARNING,
                (errmsg("epoch: send-enqueue failed epoch=%lu (local merge still queued)", txn.epoch)));
        }
    } else {
        if (EpochPipelineSendWire(wire, txn)) {
            EpochPerfNoteWireTxnSend(txn, wire.size());
        }
    }
    EpochPipelineMaybeResealEpoch(txn.epoch);
    return true;
}

static bool EpochPipelineDrainLocalQueueForEpoch(uint64_t epoch)
{
    if (epoch == 0) {
        return false;
    }
    char buffer[EPOCH_MAX_WIRE_TXN_BYTES];
    uint32_t len = 0;
    bool progress = false;
    while (EpochTxnQueuePop(epoch, buffer, sizeof(buffer), &len)) {
        progress = true;
        (void)EpochPipelineHandleDrainedLocalWire(buffer, len);
    }
    return progress;
}

static bool EpochPipelineDrainLocalQueueWindow(void)
{
    const uint64_t logical = E_NodeManager::GetLogicalEpoch();
    const uint64_t seal_max = EpochPipelineSealAheadMaxEpoch();
    bool progress = false;
    /* 先旧后新：更早 epoch 的 pack/epoch-end 优先出门 */
    uint64_t begin = logical;
    if (begin > 0) {
        begin = logical - 1;
    }
    for (uint64_t epoch = begin; epoch <= seal_max; ++epoch) {
        progress = EpochPipelineDrainLocalQueueForEpoch(epoch) || progress;
    }
    return progress;
}

/*
 * 按 txn.epoch 写入 MergeCell[epoch%slots][route]；满/失败 → ERROR（无 throttle/塞回）。
 * 必须先 Remember 再 Submit：async Claim 可能在 Submit 返回前 Finish，
 * 否则 FindWireTxn 失败 → ERROR（node3 epoch=96 wire_key=1315）。
 */
static void EpochPipelineSubmitMergeWireTxn(const E_WireTxn& txn)
{
    uint64_t wire_key = EpochPipelineWireTxnKey(txn);

    /* 持久受理去重：成功 merge 后仍保留，直到 epoch clear */
    if (!EpochConcurrent::U64SetInsert(g_merge_accepted_wires, txn.epoch, wire_key)) {
        return;
    }

    EpochPipelineRememberWireTxn(txn.epoch, txn);

    bool is_local = (txn.server_id == EpochGetNodeId());
    if (!EpochMergePoolSubmit(txn, wire_key, is_local)) {
        EpochConcurrent::U64SetErase(g_merge_accepted_wires, txn.epoch, wire_key);
        ereport(ERROR,
            (errmsg("epoch: merge submit failed epoch=%lu wire_key=%lu (serialize/inline); "
                    "increase epoch_merge_queue_capacity if async queue was full",
                static_cast<unsigned long>(txn.epoch),
                static_cast<unsigned long>(wire_key))));
    }
}

static bool EpochPipelineHandleIncomingWire(const E_WireTxn& txn, size_t wire_bytes)
{
    if (E_WireTxnIsTxnRequest(txn)) {
        EpochMarkPeerConnected(txn.server_id);
        return true;
    }
    if (E_WireTxnIsEpochEnd(txn)) {
        if (txn.epoch == 0) {
            EpochRecvDiagLogHello("RECV_HELLO", txn.server_id);
            if (txn.server_id != EpochGetNodeId() && txn.server_id < EpochGetTotalNodes()) {
                bool was_connected = EpochPeerIsConnected(txn.server_id);
                EpochMarkPeerConnected(txn.server_id);
                if (!was_connected) {
                    EpochNetPublishHello();
                }
            }
            return true;
        }
        EpochMarkPeerConnected(txn.server_id);
        EpochDynNetObserveEpochEnd(txn.epoch);
        EpochPerfNoteWireRecv(txn, wire_bytes);
        EpochRecvDiagLogEpochEndEvent("ARRIVE",
            txn.epoch,
            txn.server_id,
            txn.csn,
            E_NodeManager::GetLogicalEpoch(),
            E_NodeManager::IsNodeSendCompleted(txn.epoch, txn.server_id) ? 1 : 0,
            E_NodeManager::GetStoreShouldReceivedPackCounters(txn.epoch, txn.server_id),
            E_NodeManager::IsNodeReceiveCompleted(txn.epoch, txn.server_id) ? 1 : 0);
        EpochOnEpochEndWireReceived(txn);
        return true;
    }
    if (EpochDynNetIsProxySlowNode()) {
        return false;
    }
    EpochMarkPeerConnected(txn.server_id);
    EpochDynNetObserveWireTxn(txn);
    EpochPerfNoteWireRecv(txn, wire_bytes);
    if (EpochDynNetProxyEnabled()) {
        EpochDynNetRecordWireForProxy(txn, static_cast<int32_t>(txn.server_id));
    }
    uint64_t epoch = txn.epoch;
    uint32_t from_node = txn.server_id;
    E_NodeManager::UpdateStoreReceivedPackCounters(epoch, from_node, 1);
    g_peer_received_counts[epoch][from_node]++;
    EpochWireDiagLogTxnRecv(txn);
    (void)EpochCheckAndSetNodeReceiveCompleted(epoch, from_node);
    EpochPipelineNotePeerTxnWireProgress(epoch, from_node);
    EpochTraceMaybeLogPeerReceived(epoch);
    EpochDynNetMaybeEmitProxySnapshot(epoch);
    EpochRemoteMergeEnqueue(txn);
    return true;
}

static bool EpochPipelineRecvIncomingOnce(void)
{
    std::string wire;
    bool progress = false;

    if (EpochDynNetIsEnabled()) {
        while (EpochDynNetTryReceive(&wire)) {
            progress = true;
            E_WireTxn txn;
            if (!E_WireDeserializeTxn(wire.data(), wire.size(), &txn)) {
                continue;
            }
            (void)EpochPipelineHandleIncomingWire(txn, wire.size());
        }
        return progress;
    }

    while (EpochNetTryReceive(&wire, nullptr)) {
        progress = true;
        if (EpochDynNetHandleWireEnvelope(wire)) {
            continue;
        }
        E_WireTxn txn;
        if (!E_WireDeserializeTxn(wire.data(), wire.size(), &txn)) {
            continue;
        }
        (void)EpochPipelineHandleIncomingWire(txn, wire.size());
    }
    return progress;
}

static void EpochPipelineRecvThreadMain(void)
{
    EpochSetAuxThreadName("epochrecv");
    while (g_pipeline_recv_running.load(std::memory_order_acquire)) {
        if (!EpochPipelineRecvIncomingOnce()) {
            pg_usleep(EPOCH_PIPELINE_IDLE_US);
        }
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
            EpochDiagMaybeReport();
            EpochPerfMaybeReport();
            EpochDynNetMaybeCatchup();
            OccStagingFlusherTick();
            EpochAdvancePhysicalOnce();
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
    if (EpochMergePoolUsesInlineMerge()) {
        progress = EpochPipelineDrainRemoteMergeQueue() || progress;
        const uint64_t logical = E_NodeManager::GetLogicalEpoch();
        const uint64_t seal_max = EpochPipelineSealAheadMaxEpoch();
        for (uint64_t epoch = logical; epoch <= seal_max; ++epoch) {
            if (EpochPipelineTryMarkMergeComplete(epoch)) {
                progress = true;
            }
        }
    } else {
        progress = EpochMergePoolPollCompletions() || progress;
        EpochPipelineBootstrapMergeEpochIfNeeded();
        const uint64_t me = EpochMergePoolGetMergeEpoch();
        const uint64_t physical = E_NodeManager::GetPhysicalEpoch();
        if (me > 0 && me < physical) {
            if (EpochPipelineTryMarkMergeComplete(me)) {
                progress = true;
            }
            progress = EpochPipelineMaybeAdvanceMergeEpoch() || progress;
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
        EpochLogicalStageFlags* flags = EpochPipelineGetStageFlags(epoch);
        if (!flags->merge_complete.load(std::memory_order_acquire) ||
            flags->apply_complete.load(std::memory_order_acquire)) {
            continue;
        }
        if (EpochApplyPoolShouldThrottleDispatchForEpoch(epoch)) {
            continue;
        }
        if (EpochPipelineTryApplyStage(epoch, flags)) {
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
    bool progress = EpochApplyPoolPollCompletions();
    progress = EpochPipelineMaybeCompleteApplyAndAdvance() || progress;

    if (!EpochApplyPoolUsesInlineApply()) {
        return progress;
    }

    const uint64_t seal_max = EpochPipelineSealAheadMaxEpoch();
    const uint64_t apply_ep = EpochApplyPoolGetApplyEpoch();
    if (apply_ep == 0) {
        return progress;
    }
    E_NodeManager::EnsureEpochSlot(apply_ep);
    EpochLogicalStageFlags* flags = EpochPipelineGetStageFlags(apply_ep);
    if (!flags->apply_complete.load(std::memory_order_acquire) &&
        flags->merge_complete.load(std::memory_order_acquire)) {
        progress = EpochPipelineTryApplyStage(apply_ep, flags) || progress;
        if (!flags->apply_complete.load(std::memory_order_acquire) &&
            EpochApplyPoolShouldThrottleDispatchForEpoch(apply_ep)) {
            progress = EpochPipelinePrefillMergedApplyAhead(apply_ep, seal_max) || progress;
        }
    }
    return progress;
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
    if (!EpochMergePoolUsesInlineMerge()) {
        return EpochMergePoolPollCompletions();
    }
    return false;
}

static bool EpochPipelineEpochReadyToApply(uint64_t epoch)
{
    if (!EpochPipelineEpochMergeComplete(epoch)) {
        return false;
    }
    if (EpochPipelineCountUnappliedWireTxns(epoch) > 0) {
        return true;
    }
    if (EpochApplyPoolHasPendingWorkForEpoch(epoch)) {
        return true;
    }
    return false;
}

static bool EpochPipelineEpochHasRemoteMergeWork(uint64_t epoch)
{
    uint32_t self = EpochGetNodeId();
    uint32_t total = EpochGetTotalNodes();
    for (uint32_t n = 0; n < total; ++n) {
        if (n == self) {
            continue;
        }
        if (E_NodeManager::GetStoreShouldReceivedPackCounters(epoch, n) > 0) {
            return true;
        }
    }
    return false;
}

static void EpochPipelinePeerRecvSummary(uint64_t epoch, uint64_t* peer_should, uint64_t* peer_rcv)
{
    uint32_t self = EpochGetNodeId();
    uint64_t should = 0;
    uint64_t rcv = 0;
    for (uint32_t n = 0; n < EpochGetTotalNodes(); ++n) {
        if (n == self) {
            continue;
        }
        should += E_NodeManager::GetStoreShouldReceivedPackCounters(epoch, n);
        rcv += E_NodeManager::GetStoreReceivedPackCounters(epoch, n);
    }
    if (peer_should != nullptr) {
        *peer_should = should;
    }
    if (peer_rcv != nullptr) {
        *peer_rcv = rcv;
    }
}

static std::unordered_map<uint64_t, int> g_apply_skip_tick;
static std::mutex g_apply_skip_tick_mutex;

uint64_t EpochPipelineExpectedMergeTxnCount(uint64_t epoch)
{
    uint32_t self = EpochGetNodeId();
    uint64_t local = E_NodeManager::GetSealedPackTxnCounters(epoch, self);
    if (local == 0) {
        local = E_NodeManager::GetLocalPackTxnCounters(epoch, self);
    }
    uint64_t total = local;
    for (uint32_t peer = 0; peer < EpochGetTotalNodes(); ++peer) {
        if (peer == self) {
            continue;
        }
        total += E_NodeManager::GetStoreShouldReceivedPackCounters(epoch, peer);
    }
    return total;
}

uint64_t EpochPipelineSeenMergeTxnCount(uint64_t epoch)
{
    return E_NodeManager::GetWireMergeDoneCounters(epoch, EpochGetNodeId());
}

uint64_t EpochPipelineExpectedApplyWinnerCount(uint64_t epoch)
{
    /* merge_complete CAS 时 SetMergedTxnCounters 冻结；未关窗为 0 */
    if (!EpochPipelineEpochMergeComplete(epoch)) {
        return 0;
    }
    return E_NodeManager::GetMergedTxnCounters(epoch, EpochGetNodeId());
}

void EpochPipelineReadStageFlagsForDiag(uint64_t epoch, bool* physical_ready, bool* local_commit_ready,
    bool* self_send_sent, bool* merge_complete, bool* apply_complete)
{
    const EpochLogicalStageFlags* flags = EpochPipelineGetStageFlags(epoch);
    if (physical_ready != nullptr) {
        *physical_ready = flags->physical_ready.load(std::memory_order_acquire);
    }
    if (local_commit_ready != nullptr) {
        *local_commit_ready = flags->local_commit_ready.load(std::memory_order_acquire);
    }
    if (self_send_sent != nullptr) {
        *self_send_sent = flags->self_send_sent.load(std::memory_order_acquire);
    }
    if (merge_complete != nullptr) {
        *merge_complete = flags->merge_complete.load(std::memory_order_acquire);
    }
    if (apply_complete != nullptr) {
        *apply_complete = flags->apply_complete.load(std::memory_order_acquire);
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
    EpochRecvDiagLogApplySkip(epoch, reason, merged, committed, peer_should, peer_rcv, has_remote_rows);
}

static bool EpochPipelineTryApplyOneEpoch(uint64_t target)
{
    /*
     * 并行路径：赢家已在 merge Finish 入环；此处只认 merge_complete 后可推进。
     * PopDone 现为 no-op（finish 内联 hook），保留 PollCompletions 兼容。
     */
    bool progress = false;
    if (!EpochApplyPoolUsesInlineApply()) {
        progress = EpochApplyPoolPollCompletions();
    }
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

static void EpochPipelineMaybeResealEpoch(uint64_t epoch)
{
    if (EpochGetTotalNodes() <= 1 || epoch == 0) {
        return;
    }
    uint32_t self = EpochGetNodeId();
    if (!E_NodeManager::IsNodeSendCompleted(epoch, self)) {
        return;
    }
    EpochMaybeInvalidateStaleSeal(epoch);
    (void)EpochTrySealEpoch(epoch);
}

static void EpochPipelineTrySealEpochs(void)
{
    if (EpochGetTotalNodes() <= 1) {
        return;
    }
    if (!EpochAllPeersConnected()) {
        return;
    }

    const uint64_t logical = E_NodeManager::GetLogicalEpoch();
    const uint64_t seal_max = EpochPipelineSealAheadMaxEpoch();
    for (uint64_t epoch = logical; epoch <= seal_max; ++epoch) {
        EpochMaybeInvalidateStaleSeal(epoch);
        (void)EpochPipelineTrySealOneEpoch(epoch);
    }
    if (logical > 0) {
        EpochPipelineMaybeResealEpoch(logical - 1);
    }
}

void EpochPipelinePoll(void)
{
    (void)EpochPipelinePollSendSeal();
}

bool EpochPipelinePollSendSeal(void)
{
    /*
     * 仅无 Pack 旧路径由 EpochWorker 调用。
     * Pack 活跃时不启 epochcoord，本函数不应被热路径调用。
     */
    if (EpochPipelinePackSendActive()) {
        return false;
    }

    EpochNetMaybePublishHello();
    bool progress = EpochPipelineAdvanceMainWorkerStages();
    progress = EpochPipelinePollMergeStage() || progress;
    progress = EpochPipelinePollApplyStage() || progress;
    return progress;
}

void EpochPipelineInjectRemoteWireTxn(const E_WireTxn& txn)
{
    EpochMarkPeerConnected(txn.server_id);
    EpochDynNetObserveWireTxn(txn);
    EpochRemoteMergeEnqueue(txn);
}

bool EpochPipelineSendWire(const std::string& wire, const E_WireTxn& txn)
{
    if (EpochDynNetProxyEnabled()) {
        return EpochDynNetPublishWireTxn(wire, txn);
    }
    return EpochNetPublish(wire);
}

bool EpochPipelinePublishWire(const std::string& wire, const E_WireTxn& txn)
{
    return EpochPipelineSendWire(wire, txn);
}
