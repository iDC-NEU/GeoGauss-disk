//
// Unified facade for heterogeneous network replication (dynamic-net).
//
// Integration points (not wired yet):
//   - DnOnAppendReceived()     network layer → epoch observer
//   - DnConsultProposal()      epoch timer → FIXED/CATCHUP + Jump
//   - DnPredictiveRewrite()    after Jump → rewrite send queue
//   - DnTryFastPathMerge()     fast node commit notify → CRDT merge
//   - DnBuildAndSendProxy()    aggregator → PartialSnapshot to slow nodes
//   - DnValidateSnapshot()     slow node receive path
//

#ifndef DYNAMIC_NET_DN_API_H
#define DYNAMIC_NET_DN_API_H

#include "dn_config.h"
#include "dn_epoch_manager.h"
#include "dn_message_dispatch.h"
#include "dn_network_module.h"
#include "dn_partial_epoch_state.h"
#include "dn_peer_log.h"
#include "dn_proxy_merge.h"
#include "dn_slow_node_apply.h"
#include "dn_snapshot_validator.h"
#include "dn_traffic_shaper.h"
#include "dn_types.h"

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>

using DnPeerLogSyncFn =
    std::function<void(int32_t epoch, int32_t origin_node_id, int32_t commit_index, size_t txn_count)>;
using DnDispatchSideEffectFn = std::function<void(const DnIncomingPacket &, const DnDispatchResult &)>;

class DnRuntime {
public:
    static DnRuntime &Instance();

    void Init(const DnNetworkConfig &config);
    void Shutdown();
    bool IsInitialized() const;

    const DnNetworkConfig &GetConfig() const;
    DnNetworkStats GetStats() const;

    /* Network */
    DnNetworkModule *Net();
    DnTrafficShaper *Shaper();

    /* Epoch advancement */
    DnEpochManager *EpochMgr();
    DnEpochObserver *Observer();

    /* Proxy / merge */
    DnPartialEpochRegistry *PartialRegistry();
    DnSlowNodeMetadataStore *SlowMetaStore();

    /* High-level hooks */
    void OnAppendReceived(int from_node, const DnAppendEntriesRequest &req);
    DnProposalStrategy ConsultProposal(int32_t local_proposal_epoch);
    std::map<int32_t, int32_t> ExecuteCatchupJump(int32_t jump_target_epoch);
    bool TryFastPathMerge(int32_t epoch, DnPartialEpochState *out = nullptr);
    DnPartialSnapshotMessage BuildProxySnapshot(int32_t epoch);
    DnSnapshotValidationResult ValidateSnapshot(const DnPartialSnapshotMessage &snap,
                                                std::vector<DnTransaction> *out_txns = nullptr);

    /*
     * Post-dispatch orchestration: fast merge → proxy snapshot if aggregator.
     * Returns proxy snapshot when built (empty epoch means none).
     */
    DnPartialSnapshotMessage ProcessFollowUp(int32_t epoch, bool try_fast_merge);

    void GcEpochState(int32_t completed_epoch);

    std::unique_ptr<DnEpochBatch> TryCollectGlobalBatch(const std::unordered_map<int32_t, DnPeerLogState> &logs);

    /* Peer log registry (Multi-Raft barrier state) */
    std::unordered_map<int32_t, DnPeerLogState> *PeerLogsMutable();
    const std::unordered_map<int32_t, DnPeerLogState> &PeerLogs() const;

    void SetPeerLogSyncFn(DnPeerLogSyncFn fn);
    void SetDispatchSideEffectFn(DnDispatchSideEffectFn fn);
    const DnDispatchSideEffectFn &GetDispatchSideEffectFn() const;

    void NotifyPeerLogApplied(const DnPartialSnapshotMessage &snap, const DnSlowNodeApplyResult &result);

    void ProcessDispatchFollowUp(DnNetworkModule *net, const DnDispatchResult &dr);

private:
    DnRuntime();
    bool m_initialized;
    DnNetworkConfig m_config;
    DnNetworkStats m_stats;

    std::unique_ptr<DnTrafficShaper> m_shaper;
    std::unique_ptr<DnNetworkModule> m_net;
    std::unique_ptr<DnEpochObserver> m_observer;
    std::unique_ptr<DnEpochManager> m_epochMgr;
    std::unique_ptr<DnPartialEpochRegistry> m_partialRegistry;
    std::unique_ptr<DnSlowNodeMetadataStore> m_slowMeta;

    mutable std::mutex m_peerLogMutex;
    std::unordered_map<int32_t, DnPeerLogState> m_peerLogs;
    DnPeerLogSyncFn m_peerLogSyncFn;
    DnDispatchSideEffectFn m_dispatchSideEffectFn;
};

/* C-style helpers for future epoch/gausskernel integration */
#ifdef __cplusplus
extern "C" {
#endif

void DnInit(const DnNetworkConfig *config);
void DnShutdown();
bool DnIsInitialized();

#ifdef __cplusplus
}
#endif

#endif /* DYNAMIC_NET_DN_API_H */
