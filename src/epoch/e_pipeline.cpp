#include "postgres.h"

#include "epoch/e_pipeline.h"
#include "epoch/e_apply.h"
#include "epoch/e_abort_csn.h"
#include "epoch/e_config.h"
#include "epoch/e_crdt.h"
#include "epoch/e_epoch.h"
#include "epoch/e_net.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_statistician.h"
#include "epoch/e_shmem.h"
#include "epoch/e_txn_queue.h"
#include "epoch/e_txn_wire.h"

#include "neu_concurrency_tools/blockingconcurrentqueue.h"

#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <algorithm>

static moodycamel::BlockingConcurrentQueue<std::string> g_local_wire_queue;
static std::unordered_map<uint64_t, E_CrdtMerger> g_epoch_mergers;
static std::mutex g_merger_mutex;
static std::unordered_map<uint64_t, std::unordered_set<uint64_t>> g_seen_remote_txns;

struct EpochWireCacheSlot {
    uint64_t epoch_id;
    std::vector<std::string> wires;
};

static std::vector<EpochWireCacheSlot> g_wire_cache;
static std::mutex g_wire_cache_mutex;

static void EpochWireCacheInit(void)
{
    uint32_t slots = EpochGetCacheSlots();
    if (slots == 0) {
        slots = EPOCH_DEFAULT_CACHE_SLOTS;
    }
    std::lock_guard<std::mutex> lock(g_wire_cache_mutex);
    g_wire_cache.assign(slots, EpochWireCacheSlot{0, {}});
}

static void EpochWireCacheStore(uint64_t epoch, const std::string& wire)
{
    std::lock_guard<std::mutex> lock(g_wire_cache_mutex);
    uint32_t slots = EpochGetCacheSlots();
    if (slots == 0) {
        slots = EPOCH_DEFAULT_CACHE_SLOTS;
    }
    uint32_t slot = static_cast<uint32_t>(epoch % slots);
    if (slot >= g_wire_cache.size()) {
        return;
    }
    EpochWireCacheSlot& cache_slot = g_wire_cache[slot];
    if (cache_slot.epoch_id != epoch) {
        cache_slot.epoch_id = epoch;
        cache_slot.wires.clear();
    }
    cache_slot.wires.push_back(wire);
}

void EpochPipelineRebroadcastCachedTxns(uint64_t epoch)
{
    std::vector<std::string> copy;
    {
        std::lock_guard<std::mutex> lock(g_wire_cache_mutex);
        if (g_wire_cache.empty()) {
            return;
        }
        uint32_t slots = EpochGetCacheSlots();
        if (slots == 0) {
            slots = EPOCH_DEFAULT_CACHE_SLOTS;
        }
        uint32_t slot = static_cast<uint32_t>(epoch % slots);
        if (slot >= g_wire_cache.size()) {
            return;
        }
        const EpochWireCacheSlot& cache_slot = g_wire_cache[slot];
        if (cache_slot.epoch_id != epoch || cache_slot.wires.empty()) {
            return;
        }
        copy = cache_slot.wires;
    }
    for (const auto& wire : copy) {
        (void)EpochNetPublish(wire);
    }
}

void EpochPipelineInit(void)
{
    g_epoch_mergers.clear();
    g_seen_remote_txns.clear();
    EpochWireCacheInit();
    if (g_enable_epoch_multimaster) {
        /* worker 线程需 reattach 共享内存中的静态指针 */
        EpochShmemInit();
        EpochTxnQueueShmemInit(EPOCH_TXN_QUEUE_CAPACITY);
    }
    (void)EpochNetInit();
}

void EpochPipelineShutdown(void)
{
    EpochNetShutdown();
    std::lock_guard<std::mutex> lock(g_merger_mutex);
    g_epoch_mergers.clear();
    g_seen_remote_txns.clear();
}

static bool EpochWireIsEpochEnd(const E_WireTxn& txn)
{
    return txn.start_epoch == 0 && txn.rows.empty() &&
           txn.csn != EPOCH_WIRE_TXN_REQUEST_CSN;
}

static bool EpochWireIsTxnRequest(const E_WireTxn& txn)
{
    return txn.start_epoch == 0 && txn.rows.empty() &&
           txn.csn == EPOCH_WIRE_TXN_REQUEST_CSN && txn.commit_epoch > 0;
}

static bool EpochPipelineMergeWireTxn(const E_WireTxn& txn);
static bool EpochPipelineAllNodesSendCompleted(uint64_t epochId);

static void EpochPipelineRecordMergeAborts(uint64_t epoch, const E_WireTxn& txn,
    const E_CrdtMergeOutcome& outcome)
{
    for (const auto& aborted : outcome.aborted_txns) {
        EpochAbortCsnInsert(epoch, aborted.first, aborted.second);
    }
    /* GeoGauss MergeThread: if (!result) abort_transcation_csn_set.insert(csn_temp, csn_temp) */
    if (!outcome.txn_accepted) {
        EpochAbortCsnInsert(epoch, txn.csn, txn.server_id);
    }
}

static void EpochPipelineDrainLocalQueue(void)
{
    char buffer[EPOCH_MAX_WIRE_TXN_BYTES];
    uint32_t len = 0;
    while (EpochTxnQueuePop(buffer, sizeof(buffer), &len)) {
        std::string wire(buffer, buffer + len);
        E_WireTxn txn;
        if (!E_WireDeserializeTxn(wire.data(), wire.size(), &txn)) {
            continue;
        }

        if (EpochGetTotalNodes() <= 1) {
            if (EpochWireIsEpochEnd(txn)) {
                continue;
            }
            /* 本地已在 backend 经 O_RecordCommit 落盘，仅推进 epoch 计数 */
            E_NodeManager::IncCommittedTxnCounters(txn.commit_epoch, EpochGetNodeId());
            continue;
        }

        if (EpochWireIsEpochEnd(txn)) {
            (void)EpochNetPublish(wire);
            continue;
        }
        EpochWireCacheStore(txn.commit_epoch, wire);
        EpochPipelineMergeWireTxn(txn);
        EpochWireDiagLogTxnSend(txn);
        (void)EpochNetPublish(wire);
    }
}

static bool EpochPipelineMergeWireTxn(const E_WireTxn& txn)
{
    /*
     * merge 只在内存里做 CRDT 合并。算 pk_key 时会 heap_open 读 catalog，
     * epoch worker 里不需要也不应该包一层事务——无写操作，Commit 会走
     * after-trigger 提交流程并在 replica 下断言失败（trigger.cpp:4697）。
     */
    bool any_new = false;
    std::lock_guard<std::mutex> lock(g_merger_mutex);
    E_CrdtMerger& merger = g_epoch_mergers[txn.commit_epoch];
    E_CrdtMergeOutcome outcome = merger.MergeWireTxn(txn);
    EpochPipelineRecordMergeAborts(txn.commit_epoch, txn, outcome);
    if (outcome.txn_accepted) {
        for (const auto& row : txn.rows) {
            if (row.op_type != EPOCH_OP_READ) {
                any_new = true;
                break;
            }
        }
    }
    if (any_new) {
        E_NodeManager::IncMergedTxnCounters(txn.commit_epoch, EpochGetNodeId());
    }
    return any_new;
}

static void EpochPipelineOnRemoteTxnReceived(const E_WireTxn& txn)
{
    uint64_t epoch = txn.commit_epoch;
    uint32_t from_node = txn.server_id;
    uint64_t dedupe_key = txn.csn ^ (static_cast<uint64_t>(txn.server_id) << 48);
    bool any_new = false;
    {
        std::lock_guard<std::mutex> lock(g_merger_mutex);
        std::unordered_set<uint64_t>& seen = g_seen_remote_txns[epoch];
        if (!seen.insert(dedupe_key).second) {
            return;
        }
        E_CrdtMerger& merger = g_epoch_mergers[epoch];
        E_CrdtMergeOutcome outcome = merger.MergeWireTxn(txn);
        EpochPipelineRecordMergeAborts(epoch, txn, outcome);
        if (outcome.txn_accepted) {
            for (const auto& row : txn.rows) {
                if (row.op_type != EPOCH_OP_READ) {
                    any_new = true;
                    break;
                }
            }
        }
        if (any_new) {
            E_NodeManager::IncMergedTxnCounters(epoch, EpochGetNodeId());
        }
    }
    /*
     * epoch-end 的 should_receive 按 pack 计数；须对每个唯一远端 wire 计 receive，
     * 不能仅在 CRDT 接受行时计数，否则 merge 输家/空事务会导致 recv 永不完 → logical 卡死。
     */
    E_NodeManager::UpdateStoreReceivedPackCounters(epoch, from_node, 1);
    EpochWireDiagLogTxnRecv(txn);
    (void)EpochCheckAndSetNodeReceiveCompleted(epoch, from_node);
}

static void EpochPipelineTryEnterRemoteExecPhase(void)
{
    if (EpochGetTotalNodes() <= 1) {
        return;
    }

    uint64_t target = E_NodeManager::GetLogicalEpoch();
    if (!EpochPipelineAllNodesSendCompleted(target)) {
        return;
    }
    if (!EpochIsAllNodesReceiveCompleted(target)) {
        return;
    }
    if (E_NodeManager::IsRemoteExeced()) {
        return;
    }

    /*
     * GeoGauss 在 merge 全部完成、进入 commit 阶段才 SetRecordCommitted(false)
     *（mot_internal.cpp:3257）。不可在 send/recv 完成时置 false，否则 START gate
     * 长时间阻塞新连接，TPCC 只能连上极少数终端（其余 backend 在 gate 自旋占满连接池）。
     */
    E_NodeManager::SetRemoteExeced(true);
}

static void EpochPipelineTrySignalRemoteRecordCommitted(void)
{
    if (EpochGetTotalNodes() <= 1) {
        return;
    }
    if (!E_NodeManager::IsRemoteExeced()) {
        return;
    }

    uint64_t target = E_NodeManager::GetLogicalEpoch();
    uint64_t merged = E_NodeManager::GetMergedTxnCounters(target, EpochGetNodeId());
    uint64_t committed = E_NodeManager::GetCommittedTxnCounters(target, EpochGetNodeId());
    if (merged > committed) {
        return;
    }
    /* GeoGauss：merge apply 完成且 commit 路径就绪后，END 路径可继续 */
    if (!EpochIsRemoteMergeReady(target)) {
        return;
    }
    E_NodeManager::SetRemoteRecordCommitted(true);
}

static void EpochPipelineConsumeRemote(void)
{
    std::string wire;
    while (EpochNetTryReceive(&wire)) {
        E_WireTxn txn;
        if (!E_WireDeserializeTxn(wire.data(), wire.size(), &txn)) {
            continue;
        }
        if (EpochWireIsTxnRequest(txn)) {
            EpochMarkPeerConnected(txn.server_id);
            EpochHandleTxnRequest(txn);
            continue;
        }
        if (EpochWireIsEpochEnd(txn)) {
            if (txn.commit_epoch == 0) {
                EpochRecvDiagLogHello("RECV_HELLO", txn.server_id);
                EpochMarkPeerConnected(txn.server_id);
                continue;
            }
            EpochMarkPeerConnected(txn.server_id);
            EpochOnEpochEndWireReceived(txn);
            continue;
        }
        EpochMarkPeerConnected(txn.server_id);
        EpochPipelineOnRemoteTxnReceived(txn);
    }
}

static void EpochPipelineTryMergeLocal(void)
{
    std::string wire;
    while (g_local_wire_queue.try_dequeue(wire)) {
        E_WireTxn txn;
        if (!E_WireDeserializeTxn(wire.data(), wire.size(), &txn)) {
            continue;
        }
        if (EpochWireIsEpochEnd(txn)) {
            continue;
        }
        EpochPipelineMergeWireTxn(txn);
    }
}

static bool EpochPipelineEpochReadyToApply(uint64_t epoch)
{
    if (!EpochIsAllNodesReceiveCompleted(epoch)) {
        return false;
    }
    uint64_t merged = E_NodeManager::GetMergedTxnCounters(epoch, EpochGetNodeId());
    uint64_t committed = E_NodeManager::GetCommittedTxnCounters(epoch, EpochGetNodeId());
    return merged > committed || (merged == 0 && committed == 0);
}

static void EpochPipelineMarkEpochCommitted(uint64_t epoch)
{
    uint64_t merged = E_NodeManager::GetMergedTxnCounters(epoch, EpochGetNodeId());
    while (E_NodeManager::GetCommittedTxnCounters(epoch, EpochGetNodeId()) < merged) {
        E_NodeManager::IncCommittedTxnCounters(epoch, EpochGetNodeId());
    }
    if (merged == 0 && E_NodeManager::GetCommittedTxnCounters(epoch, EpochGetNodeId()) == 0) {
        E_NodeManager::IncCommittedTxnCounters(epoch, EpochGetNodeId());
        E_NodeManager::IncCommittedTxnCounters(epoch, EpochGetNodeId());
        /* 空 epoch：用 committed=0 语义，Inc 两次再 Dec 不划算；直接视为已提交 */
    }
}

static bool EpochPipelineAllNodesSendCompleted(uint64_t epochId)
{
    uint32_t total = EpochGetTotalNodes();
    for (uint32_t node = 0; node < total; ++node) {
        if (node == EpochGetNodeId()) {
            if (!E_NodeManager::IsNodeSendCompleted(epochId, node)) {
                return false;
            }
            continue;
        }
        if (!EpochPeerIsConnected(node)) {
            return false;
        }
        if (!E_NodeManager::IsNodeSendCompleted(epochId, node)) {
            return false;
        }
    }
    return true;
}

static void EpochPipelineTrySignalRemoteMergeReady(void)
{
    EpochPipelineTryEnterRemoteExecPhase();
    EpochPipelineTrySignalRemoteRecordCommitted();
}

static bool EpochPipelineTryApplyOneEpoch(uint64_t target)
{
    if (!EpochIsAllNodesReceiveCompleted(target)) {
        return false;
    }
    if (!EpochPipelineEpochReadyToApply(target)) {
        return false;
    }

    bool has_rows = false;
    E_WireTxn merged;
    {
        std::lock_guard<std::mutex> lock(g_merger_mutex);
        auto it = g_epoch_mergers.find(target);
        if (it != g_epoch_mergers.end() && !it->second.IsEmpty()) {
            has_rows = it->second.BuildMergedTxn(target, 0, EPOCH_MERGED_SERVER_ID, &merged);
            g_epoch_mergers.erase(it);
        } else if (it != g_epoch_mergers.end()) {
            g_epoch_mergers.erase(it);
        }
    }

    if (has_rows) {
        uint64_t row_count = merged.rows.size();
        bool apply_ok = E_ApplyWireTxn(merged);
        EpochRecvDiagOnApply(target, row_count, apply_ok);
    }

    uint64_t merged_cnt = E_NodeManager::GetMergedTxnCounters(target, EpochGetNodeId());
    while (E_NodeManager::GetCommittedTxnCounters(target, EpochGetNodeId()) < merged_cnt) {
        E_NodeManager::IncCommittedTxnCounters(target, EpochGetNodeId());
    }
    return true;
}

static bool EpochPipelineTryApplyReadyEpoch(uint64_t epoch)
{
    if (!EpochIsAllNodesReceiveCompleted(epoch)) {
        return false;
    }
    if (!EpochPipelineEpochReadyToApply(epoch)) {
        return false;
    }
    /*
     * 当前 logical 关窗 epoch 须等 physical delay；已关窗的历史 epoch 只要求 recv 完成即可 apply，
     * 否则 logical 推进后 merger 被丢弃，对端写入永远无法落盘到本节点。
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
    static constexpr int kMaxApplyBatch = 64;
    for (int batch = 0; batch < kMaxApplyBatch; ++batch) {
        std::vector<uint64_t> epochs;
        uint64_t logical = E_NodeManager::GetLogicalEpoch();
        uint64_t lookback = EpochGetPhysLogicalGapLimit() + 32;
        if (lookback < 256) {
            lookback = 256;
        }
        uint64_t start = (logical > lookback) ? (logical - lookback) : 1;
        for (uint64_t epoch = start; epoch <= logical; ++epoch) {
            epochs.push_back(epoch);
        }
        {
            std::lock_guard<std::mutex> lock(g_merger_mutex);
            for (const auto& kv : g_epoch_mergers) {
                epochs.push_back(kv.first);
            }
        }
        std::sort(epochs.begin(), epochs.end());
        epochs.erase(std::unique(epochs.begin(), epochs.end()), epochs.end());

        bool progress = false;
        for (uint64_t epoch : epochs) {
            if (EpochPipelineTryApplyReadyEpoch(epoch)) {
                progress = true;
                break;
            }
        }
        if (!progress) {
            break;
        }
    }
}

static void EpochPipelineTrySealEpochs(void)
{
    if (EpochGetTotalNodes() <= 1) {
        return;
    }

    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    if (!E_NodeManager::IsNodeSendCompleted(logical, EpochGetNodeId())) {
        (void)EpochTrySealEpoch(logical);
    }
}

void EpochPipelinePoll(void)
{
    EpochNetMaybePublishHello();
    EpochMaybeRequestMissingTxns();
    EpochPipelineDrainLocalQueue();
    if (EpochGetTotalNodes() > 1) {
        EpochPipelineConsumeRemote();
        EpochPipelineTryMergeLocal();
        EpochPipelineTrySignalRemoteMergeReady();
        EpochPipelineTrySealEpochs();
        EpochPipelineApplyReadyEpochs();
        EpochPipelineDrainLocalQueue();
    } else {
        EpochPipelineTrySealEpochs();
    }
}
