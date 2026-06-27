#include "dn_api.h"

#include "dn_config.h"
#include "dn_proxy_merge.h"

#include <memory>

DnRuntime &DnRuntime::Instance()
{
    static DnRuntime instance;
    return instance;
}

DnRuntime::DnRuntime() : m_initialized(false) {}

void DnRuntime::Init(const DnNetworkConfig &config)
{
    m_config = config;
    m_stats = DnNetworkStats{};

    m_shaper = std::make_unique<DnTrafficShaper>(&m_config);
    m_net = std::make_unique<DnNetworkModule>(config.local_node_id, m_shaper.get(), &m_config);
    m_observer = std::make_unique<DnEpochObserver>(&m_config);
    m_epochMgr = std::make_unique<DnEpochManager>(config.local_node_id, &m_config, m_observer.get());
    m_partialRegistry = std::make_unique<DnPartialEpochRegistry>(&m_config);
    m_slowMeta = std::make_unique<DnSlowNodeMetadataStore>();

    m_initialized = true;
}

void DnRuntime::Shutdown()
{
    m_slowMeta.reset();
    m_partialRegistry.reset();
    m_epochMgr.reset();
    m_observer.reset();
    m_net.reset();
    m_shaper.reset();
    {
        std::lock_guard<std::mutex> lock(m_peerLogMutex);
        m_peerLogs.clear();
        m_peerLogSyncFn = nullptr;
    }
    m_dispatchSideEffectFn = nullptr;
    m_initialized = false;
}

bool DnRuntime::IsInitialized() const
{
    return m_initialized;
}

const DnNetworkConfig &DnRuntime::GetConfig() const
{
    return m_config;
}

DnNetworkStats DnRuntime::GetStats() const
{
    DnNetworkStats stats = m_stats;
    if (m_net != nullptr) {
        stats.tx_queue_size = m_net->GetTxQueueSize();
        stats.rx_queue_size = m_net->GetRxQueueSize();
        stats.dropped_tx = static_cast<uint64_t>(m_net->GetDroppedTx());
        stats.dropped_rx = static_cast<uint64_t>(m_net->GetDroppedRx());
    }
    if (m_observer != nullptr) {
        stats.max_observed_epoch = m_observer->GetMaxObservedEpoch();
    }
    return stats;
}

DnNetworkModule *DnRuntime::Net()
{
    return m_net.get();
}

DnTrafficShaper *DnRuntime::Shaper()
{
    return m_shaper.get();
}

DnEpochManager *DnRuntime::EpochMgr()
{
    return m_epochMgr.get();
}

DnEpochObserver *DnRuntime::Observer()
{
    return m_observer.get();
}

DnPartialEpochRegistry *DnRuntime::PartialRegistry()
{
    return m_partialRegistry.get();
}

DnSlowNodeMetadataStore *DnRuntime::SlowMetaStore()
{
    return m_slowMeta.get();
}

void DnRuntime::OnAppendReceived(int from_node, const DnAppendEntriesRequest &req)
{
    if (!m_initialized || m_observer == nullptr) {
        return;
    }
    m_epochMgr->RecordObservedEpochFromAppend(req.epoch, from_node, m_config.local_node_id);

    if (req.IsMetadataOnly() && DnIsConfiguredSlow(m_config, m_config.local_node_id) && m_slowMeta != nullptr) {
        m_slowMeta->RecordMetadata(req.epoch, from_node, req.term, req.payload_hash, req.payload_size);
        m_stats.rx_append_meta++;
    } else if (!req.entries.empty()) {
        m_stats.rx_append_full++;
    }
}

DnProposalStrategy DnRuntime::ConsultProposal(int32_t local_proposal_epoch)
{
    if (!m_initialized || m_epochMgr == nullptr || m_net == nullptr) {
        return DnProposalStrategy{};
    }
    bool is_slow = DnIsConfiguredSlow(m_config, m_config.local_node_id);
    DnProposalStrategy s =
        m_epochMgr->ConsultProposalStrategy(local_proposal_epoch, static_cast<int32_t>(m_net->GetTxQueueSize()), is_slow);
    m_stats.last_lag = s.lag;
    return s;
}

std::map<int32_t, int32_t> DnRuntime::ExecuteCatchupJump(int32_t jump_target_epoch)
{
    if (!m_initialized || m_net == nullptr) {
        return {};
    }
    return m_net->PredictiveEpochRewrite(jump_target_epoch);
}

bool DnRuntime::TryFastPathMerge(int32_t epoch, DnPartialEpochState *out)
{
    if (!m_initialized || m_partialRegistry == nullptr || !m_config.enable_proxy) {
        return false;
    }
    if (!DnIsFastNode(m_config, m_config.local_node_id)) {
        return false;
    }
    return m_partialRegistry->TryFastPathMerge(epoch, out);
}

DnPartialSnapshotMessage DnRuntime::BuildProxySnapshot(int32_t epoch)
{
    DnPartialSnapshotMessage empty;
    if (!m_initialized || m_partialRegistry == nullptr) {
        return empty;
    }
    const DnPartialEpochState *pes = m_partialRegistry->GetState(epoch);
    if (pes == nullptr || !pes->fast_consensus_done) {
        return empty;
    }
    if (!m_partialRegistry->ShouldSendProxy(epoch, m_config.local_node_id)) {
        return empty;
    }
    DnPartialSnapshotMessage snap =
        DnProxyMerge::BuildPartialSnapshot(m_config.local_node_id, *pes);
    m_partialRegistry->MarkProxySent(epoch);
    m_stats.rx_snapshot++;
    return snap;
}

DnSnapshotValidationResult DnRuntime::ValidateSnapshot(const DnPartialSnapshotMessage &snap,
                                                       std::vector<DnTransaction> *out_txns)
{
    if (!m_initialized || m_slowMeta == nullptr) {
        return DnSnapshotValidationResult::MISSING_META;
    }
    return DnSnapshotValidator::Validate(snap, *m_slowMeta, out_txns);
}

std::unique_ptr<DnEpochBatch> DnRuntime::TryCollectGlobalBatch(
    const std::unordered_map<int32_t, DnPeerLogState> &logs)
{
    if (!m_initialized || m_epochMgr == nullptr) {
        return nullptr;
    }
    return m_epochMgr->TryCollectBatch(logs);
}

DnPartialSnapshotMessage DnRuntime::ProcessFollowUp(int32_t epoch, bool try_fast_merge)
{
    DnPartialSnapshotMessage empty;
    if (!m_initialized || !m_config.enable_proxy) {
        return empty;
    }

    if (try_fast_merge && DnIsFastNode(m_config, m_config.local_node_id)) {
        TryFastPathMerge(epoch, nullptr);
    }

    if (DnIsFastNode(m_config, m_config.local_node_id)) {
        return BuildProxySnapshot(epoch);
    }
    return empty;
}

void DnRuntime::GcEpochState(int32_t completed_epoch)
{
    if (!m_initialized) {
        return;
    }
    if (m_partialRegistry != nullptr) {
        m_partialRegistry->GcBeforeEpoch(completed_epoch);
    }
    if (m_slowMeta != nullptr) {
        m_slowMeta->GcBeforeEpoch(completed_epoch);
    }
}

std::unordered_map<int32_t, DnPeerLogState> *DnRuntime::PeerLogsMutable()
{
    return &m_peerLogs;
}

const std::unordered_map<int32_t, DnPeerLogState> &DnRuntime::PeerLogs() const
{
    return m_peerLogs;
}

void DnRuntime::SetPeerLogSyncFn(DnPeerLogSyncFn fn)
{
    std::lock_guard<std::mutex> lock(m_peerLogMutex);
    m_peerLogSyncFn = std::move(fn);
}

void DnRuntime::SetDispatchSideEffectFn(DnDispatchSideEffectFn fn)
{
    m_dispatchSideEffectFn = std::move(fn);
}

const DnDispatchSideEffectFn &DnRuntime::GetDispatchSideEffectFn() const
{
    return m_dispatchSideEffectFn;
}

void DnRuntime::NotifyPeerLogApplied(const DnPartialSnapshotMessage &snap,
                                     const DnSlowNodeApplyResult &result)
{
    if (!result.applied || !m_peerLogSyncFn) {
        return;
    }
    std::lock_guard<std::mutex> lock(m_peerLogMutex);
    if (!m_peerLogSyncFn) {
        return;
    }
    for (const auto &contrib : snap.contributions) {
        int32_t origin = contrib.origin_node_id;
        int32_t commit_index = 0;
        auto it = m_peerLogs.find(origin);
        if (it != m_peerLogs.end()) {
            commit_index = it->second.commit_index.load(std::memory_order_acquire);
        }
        size_t txn_count = contrib.committed_txns.size();
        m_peerLogSyncFn(snap.epoch, origin, commit_index, txn_count);
    }
}

void DnRuntime::ProcessDispatchFollowUp(DnNetworkModule *net, const DnDispatchResult &dr)
{
    (void)net;
    if (!m_initialized) {
        return;
    }
    if (dr.need_try_fast_merge && dr.fast_merge_epoch > 0) {
        TryFastPathMerge(dr.fast_merge_epoch, nullptr);
    }
}

void DnInit(const DnNetworkConfig *config)
{
    if (config == nullptr) {
        DnNetworkConfig defaults;
        DnRuntime::Instance().Init(defaults);
        return;
    }
    DnRuntime::Instance().Init(*config);
}

void DnShutdown()
{
    DnRuntime::Instance().Shutdown();
}

bool DnIsInitialized()
{
    return DnRuntime::Instance().IsInitialized();
}
