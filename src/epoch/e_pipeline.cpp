#include "postgres.h"

#include "epoch/e_pipeline.h"
#include "epoch/e_dyn_net.h"
#include "epoch/e_apply.h"
#include "epoch/e_abort_csn.h"
#include "epoch/e_config.h"
#include "epoch/e_crdt.h"
#include "epoch/e_epoch.h"
#include "epoch/e_net.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_statistician.h"
#include "epoch/e_trace.h"
#include "epoch/e_shmem.h"
#include "epoch/e_txn_queue.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_wire_envelope.h"
#include "epoch/e_perf.h"

#include "dn_api.h"
#include "dn_config.h"
#include "dn_types.h"
#include "occ/o_staging.h"

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
/* 按 epoch/peer 累计已收 wire txn；slot 被 Clear 后 dedupe 不重计，靠此恢复 received */
static std::unordered_map<uint64_t, std::array<uint64_t, EPOCH_MAX_NODES>> g_peer_received_counts;

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
        E_WireTxn txn;
        if (E_WireDeserializeTxn(wire.data(), wire.size(), &txn)) {
            (void)EpochPipelinePublishWire(wire, txn);
        } else {
            (void)EpochNetPublish(wire);
        }
    }
}

bool EpochPipelineHasWireCache(uint64_t epoch)
{
    std::lock_guard<std::mutex> lock(g_wire_cache_mutex);
    if (g_wire_cache.empty()) {
        return false;
    }
    uint32_t slots = EpochGetCacheSlots();
    if (slots == 0) {
        slots = EPOCH_DEFAULT_CACHE_SLOTS;
    }
    uint32_t slot = static_cast<uint32_t>(epoch % slots);
    if (slot >= g_wire_cache.size()) {
        return false;
    }
    const EpochWireCacheSlot& cache_slot = g_wire_cache[slot];
    return cache_slot.epoch_id == epoch && !cache_slot.wires.empty();
}

uint64_t EpochPipelineCachedPackCount(uint64_t epoch)
{
    std::lock_guard<std::mutex> lock(g_wire_cache_mutex);
    if (g_wire_cache.empty()) {
        return 0;
    }
    uint32_t slots = EpochGetCacheSlots();
    if (slots == 0) {
        slots = EPOCH_DEFAULT_CACHE_SLOTS;
    }
    uint32_t slot = static_cast<uint32_t>(epoch % slots);
    if (slot >= g_wire_cache.size()) {
        return 0;
    }
    const EpochWireCacheSlot& cache_slot = g_wire_cache[slot];
    if (cache_slot.epoch_id != epoch) {
        return 0;
    }
    return cache_slot.wires.size();
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
    OccStagingModuleShutdown();
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
static void EpochPipelineMaybeResealEpoch(uint64_t epoch);

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
            (void)EpochPipelinePublishWire(wire, txn);
            EpochPerfNoteEpochEndSend(wire.size());
            continue;
        }
        EpochWireCacheStore(txn.commit_epoch, wire);
        EpochPipelineMergeWireTxn(txn);
        EpochWireDiagLogTxnSend(txn);
        (void)EpochPipelinePublishWire(wire, txn);
        EpochPerfNoteWireTxnSend(txn, wire.size());
        EpochPipelineMaybeResealEpoch(txn.commit_epoch);
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
    uint64_t dedupe_key = txn.csn ^ (static_cast<uint64_t>(txn.server_id) << 48);
    std::lock_guard<std::mutex> lock(g_merger_mutex);
    g_seen_remote_txns[txn.commit_epoch].insert(dedupe_key);
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

    if (EpochDynNetProxyEnabled()) {
        EpochDynNetRecordWireForProxy(txn, static_cast<int32_t>(from_node));
    }

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
    g_peer_received_counts[epoch][from_node]++;
    EpochWireDiagLogTxnRecv(txn);
    (void)EpochCheckAndSetNodeReceiveCompleted(epoch, from_node);
    EpochTraceMaybeLogPeerReceived(epoch);
    EpochDynNetMaybeEmitProxySnapshot(epoch);
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
    if (!EpochIsAllPeersReceiveCompleted(target)) {
        return;
    }
    if (E_NodeManager::IsRemoteExeced()) {
        return;
    }

    /*
     * GeoGauss 在 merge 全部完成、进入 commit 阶段 SetRemoteExeced(true)。
     * 只检查对端 recv（本端 seal 后才 recv_done）；勿置 record_committed=false，
     * 否则 START gate 阻塞新连接。
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

    if (EpochDynNetIsEnabled()) {
        while (EpochDynNetTryReceive(&wire)) {
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
                    if (txn.server_id != EpochGetNodeId() && txn.server_id < EpochGetTotalNodes()) {
                        bool was_connected = EpochPeerIsConnected(txn.server_id);
                        EpochMarkPeerConnected(txn.server_id);
                        if (!was_connected) {
                            EpochNetPublishHello();
                        }
                    }
                    continue;
                }
                EpochMarkPeerConnected(txn.server_id);
                EpochDynNetObserveEpochEnd(txn.commit_epoch);
                EpochPerfNoteWireRecv(txn, wire.size());
                EpochOnEpochEndWireReceived(txn);
                continue;
            }
            if (EpochDynNetIsProxySlowNode()) {
                continue;
            }
            EpochMarkPeerConnected(txn.server_id);
            EpochDynNetObserveWireTxn(txn);
            EpochPerfNoteWireRecv(txn, wire.size());
            EpochPipelineOnRemoteTxnReceived(txn);
        }
        return;
    }

    while (EpochNetTryReceive(&wire, nullptr)) {
        if (EpochDynNetHandleWireEnvelope(wire)) {
            continue;
        }
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
                if (txn.server_id != EpochGetNodeId() && txn.server_id < EpochGetTotalNodes()) {
                    bool was_connected = EpochPeerIsConnected(txn.server_id);
                    EpochMarkPeerConnected(txn.server_id);
                    /*
                     * 仅在首次连通时回发 hello，避免双向 SYN-ACK 形成 hello 风暴。
                     */
                    if (!was_connected) {
                        EpochNetPublishHello();
                    }
                }
                continue;
            }
            EpochMarkPeerConnected(txn.server_id);
            EpochDynNetObserveEpochEnd(txn.commit_epoch);
            EpochPerfNoteWireRecv(txn, wire.size());
            EpochOnEpochEndWireReceived(txn);
            continue;
        }
        if (EpochDynNetIsProxySlowNode()) {
            continue;
        }
        EpochMarkPeerConnected(txn.server_id);
        EpochDynNetObserveWireTxn(txn);
        EpochPerfNoteWireRecv(txn, wire.size());
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
    uint64_t merged = E_NodeManager::GetMergedTxnCounters(epoch, EpochGetNodeId());
    uint64_t committed = E_NodeManager::GetCommittedTxnCounters(epoch, EpochGetNodeId());
    {
        std::lock_guard<std::mutex> lock(g_merger_mutex);
        auto it = g_epoch_mergers.find(epoch);
        if (it != g_epoch_mergers.end() && !it->second.IsEmpty() &&
            it->second.HasRemoteRows(EpochGetNodeId())) {
            return true;
        }
    }
    if (merged > committed) {
        return true;
    }
    if (merged == 0 && committed == 0) {
        return true;
    }
    return false;
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

static bool EpochPipelineEpochRecvReadyForApply(uint64_t epoch)
{
    if (EpochIsAsyncMultimasterMode()) {
        uint32_t self = EpochGetNodeId();
        uint64_t merged = E_NodeManager::GetMergedTxnCounters(epoch, self);
        uint64_t committed = E_NodeManager::GetCommittedTxnCounters(epoch, self);
        if (merged > committed) {
            return true;
        }
    }
    if (!EpochIsAllPeersReceiveCompleted(epoch)) {
        return false;
    }
    if (EpochPipelineEpochHasRemoteMergeWork(epoch)) {
        return true;
    }
    return EpochIsAllNodesReceiveCompleted(epoch);
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

static void EpochPipelineMaybeLogApplySkip(uint64_t epoch, const char* reason, uint64_t merged,
    uint64_t committed, uint64_t peer_should, uint64_t peer_rcv, int has_remote_rows)
{
    int& tick = g_apply_skip_tick[epoch];
    if (++tick % 300 != 1) {
        return;
    }
    EpochRecvDiagLogApplySkip(epoch, reason, merged, committed, peer_should, peer_rcv, has_remote_rows);
}

static bool EpochPipelineTryApplyOneEpoch(uint64_t target)
{
    uint64_t peer_should = 0;
    uint64_t peer_rcv = 0;
    EpochPipelinePeerRecvSummary(target, &peer_should, &peer_rcv);
    uint64_t merged_cnt = E_NodeManager::GetMergedTxnCounters(target, EpochGetNodeId());
    uint64_t committed_cnt = E_NodeManager::GetCommittedTxnCounters(target, EpochGetNodeId());
    bool remote_work = EpochPipelineEpochHasRemoteMergeWork(target);
    int has_remote_rows = 0;

    if (!EpochPipelineEpochRecvReadyForApply(target)) {
        if (peer_should > 0 || peer_rcv > 0) {
            EpochPipelineMaybeLogApplySkip(target, "recv_not_ready", merged_cnt, committed_cnt, peer_should,
                peer_rcv, has_remote_rows);
        }
        return false;
    }
    if (!EpochPipelineEpochReadyToApply(target)) {
        EpochPipelineMaybeLogApplySkip(target, "already_committed", merged_cnt, committed_cnt, peer_should,
            peer_rcv, has_remote_rows);
        return false;
    }

    EpochTraceMaybeLogPeerReceived(target);
    EpochTraceMaybeLogMergeSummary(target, peer_rcv, merged_cnt);

    bool has_rows = false;
    E_WireTxn merged;
    {
        std::lock_guard<std::mutex> lock(g_merger_mutex);
        auto it = g_epoch_mergers.find(target);
        if (it != g_epoch_mergers.end() && !it->second.IsEmpty()) {
            has_remote_rows = it->second.HasRemoteRows(EpochGetNodeId()) ? 1 : 0;
            has_rows = it->second.BuildMergedTxn(target, 0, EPOCH_MERGED_SERVER_ID, &merged);
        }
    }

    if (remote_work && peer_rcv > 0 && !has_rows) {
        /*
         * merged 计数含 CRDT 输家/无 heap 行事务时 BuildMergedTxn 为空，但仍须
         * 对齐 committed，否则 merged>committed 永久 commit_path_incomplete。
         */
        if (merged_cnt <= committed_cnt) {
            const char* issue = (has_remote_rows != 0) ? "build_empty" : "merge_empty";
            EpochRecvDiagLogApplyIssue(target, issue, merged_cnt, peer_rcv);
            return false;
        }
    } else if (has_rows) {
        uint64_t row_count = merged.rows.size();
        bool apply_ok = E_ApplyWireTxn(merged);
        EpochRecvDiagOnApply(target, row_count, apply_ok);
        if (!apply_ok && !EpochIsAsyncMultimasterMode()) {
            return false;
        }
    }

    {
        std::lock_guard<std::mutex> lock(g_merger_mutex);
        g_epoch_mergers.erase(target);
    }

    while (E_NodeManager::GetCommittedTxnCounters(target, EpochGetNodeId()) < merged_cnt) {
        E_NodeManager::IncCommittedTxnCounters(target, EpochGetNodeId());
    }
    EpochTraceLogApplyDone(target, merged_cnt);
    return true;
}

bool EpochPipelineTryApplyMergedEpoch(uint64_t epoch)
{
    if (EpochGetTotalNodes() <= 1) {
        return true;
    }
    /*
     * 由 TryAdvanceLogical 在 self seal / send 完成后调用：recv 已齐且 physical 已在
     * advance 入口校验，此处不再挡 physical_delay。
     */
    return EpochPipelineTryApplyOneEpoch(epoch);
}

static bool EpochPipelineTryApplyReadyEpoch(uint64_t epoch)
{
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

    uint64_t logical = E_NodeManager::GetLogicalEpoch();
    EpochMaybeInvalidateStaleSeal(logical);
    (void)EpochTrySealEpoch(logical);
    if (logical > 0) {
        EpochPipelineMaybeResealEpoch(logical - 1);
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
        uint64_t logical = E_NodeManager::GetLogicalEpoch();
        if (logical > 1) {
            EpochDynNetGcBeforeEpoch(logical - 1);
        }
    } else {
        EpochPipelineTrySealEpochs();
    }
}

void EpochPipelineInjectRemoteWireTxn(const E_WireTxn& txn)
{
    EpochMarkPeerConnected(txn.server_id);
    EpochDynNetObserveWireTxn(txn);
    EpochPipelineOnRemoteTxnReceived(txn);
}

bool EpochPipelinePublishWire(const std::string& wire, const E_WireTxn& txn)
{
    if (EpochDynNetProxyEnabled()) {
        return EpochDynNetPublishWireTxn(wire, txn);
    }
    return EpochNetPublish(wire);
}
