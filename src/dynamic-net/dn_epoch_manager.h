//
// Global epoch barrier + FIXED/CATCHUP proposal strategy (dynamic-net).
//

#ifndef DYNAMIC_NET_DN_EPOCH_MANAGER_H
#define DYNAMIC_NET_DN_EPOCH_MANAGER_H

#include "dn_config.h"
#include "dn_epoch_observer.h"
#include "dn_peer_log.h"
#include "dn_types.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

class DnEpochManager {
public:
    DnEpochManager(int node_id, const DnNetworkConfig *config, DnEpochObserver *observer);

    void Configure(const DnNetworkConfig *config);

    void RecordObservedEpochFromAppend(int32_t sender_epoch, int from_node, int to_node);

    /*
     * Timer tick: consult whether to NORMAL +1 or JUMP for slow node.
     */
    DnProposalStrategy ConsultProposalStrategy(int32_t current_local_proposal_epoch,
                                               int32_t current_net_queue_size, bool is_slow_node);

    /*
     * Try collect epoch batch when all channels satisfy barrier for global_epoch.
     */
    std::unique_ptr<DnEpochBatch> TryCollectBatch(const std::unordered_map<int32_t, DnPeerLogState> &logs);

    uint64_t GetGlobalEpoch() const;
    const std::vector<int32_t> &GetExecuteIndexes() const;

    void Reset();

private:
    bool IsBarrierReady(uint64_t target_epoch, const std::unordered_map<int32_t, DnPeerLogState> &logs);

    int m_nodeId;
    const DnNetworkConfig *m_config;
    DnEpochObserver *m_observer;

    std::atomic<uint64_t> m_globalEpoch;
    std::vector<int32_t> m_executeIndexes;
    mutable std::mutex m_executionMutex;
};

#endif /* DYNAMIC_NET_DN_EPOCH_MANAGER_H */
