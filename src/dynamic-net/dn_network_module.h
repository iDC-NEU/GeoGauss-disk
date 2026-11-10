//
// Virtual network module: queued send/receive + predictive epoch rewrite.
//

#ifndef DYNAMIC_NET_DN_NETWORK_MODULE_H
#define DYNAMIC_NET_DN_NETWORK_MODULE_H

#include "dn_config.h"
#include "dn_traffic_shaper.h"
#include "dn_types.h"

#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <queue>
#include <vector>

struct DnOutgoingPacket {
    long long release_time_ms;
    DnNetworkPacket packet;
    int target_node_id;
    bool is_reply;
};

struct DnIncomingPacket {
    long long deliver_at_ms;
    DnNetworkPacket packet;
    int from_node_id;
    bool is_high_priority;
};

using DnNowMsFn = std::function<long long()>;
using DnPacketSizeFn = std::function<size_t(const DnNetworkPacket &)>;

class DnNetworkModule {
public:
    DnNetworkModule(int node_id, DnTrafficShaper *shaper, const DnNetworkConfig *config);

    void SetClock(DnNowMsFn now_fn);
    void SetPacketSizeFn(DnPacketSizeFn size_fn);

    void SendMessage(const DnNetworkPacket &pkt, int target_node_id, bool is_reply = false);
    std::vector<DnOutgoingPacket> PopReadyOutgoingPackets();

    /*
     * CATCHUP: rewrite pending data packets' epoch fields in-place.
     * Returns map<log_index, new_epoch> for local log synchronization.
     */
    std::map<int32_t, int32_t> PredictiveEpochRewrite(int32_t base_epoch);

    size_t GetTxQueueSize() const;
    size_t GetRxQueueSize() const;
    long long GetDroppedTx() const;
    long long GetDroppedRx() const;

    void EnqueueIncoming(const DnNetworkPacket &pkt, int from_node_id, size_t raw_size_bytes,
                         long long arrival_at_nic_ms);
    std::vector<DnIncomingPacket> PopReadyIncomingPackets();

    long long NowMs() const;

private:
    bool IsHighPriority(const DnNetworkPacket &pkt) const;
    bool IsDroppable(const DnNetworkPacket &pkt) const;
    size_t PacketSize(const DnNetworkPacket &pkt) const;

    int m_nodeId;
    DnTrafficShaper *m_shaper;
    const DnNetworkConfig *m_config;
    DnNowMsFn m_nowFn;
    DnPacketSizeFn m_sizeFn;

    mutable std::mutex m_mutex;
    std::deque<DnOutgoingPacket> m_dataQueue;
    std::deque<DnOutgoingPacket> m_controlQueue;

    struct IncomingCmp {
        bool operator()(const DnIncomingPacket &a, const DnIncomingPacket &b) const
        {
            return a.deliver_at_ms > b.deliver_at_ms;
        }
    };
    std::priority_queue<DnIncomingPacket, std::vector<DnIncomingPacket>, IncomingCmp> m_incomingCtrl;
    std::priority_queue<DnIncomingPacket, std::vector<DnIncomingPacket>, IncomingCmp> m_incomingData;

    long long m_droppedTx;
    long long m_droppedRx;
};

#endif /* DYNAMIC_NET_DN_NETWORK_MODULE_H */
