//
// Observed epoch tracking with link propagation compensation (CATCHUP).
//

#ifndef DYNAMIC_NET_DN_EPOCH_OBSERVER_H
#define DYNAMIC_NET_DN_EPOCH_OBSERVER_H

#include "dn_config.h"

#include <atomic>
#include <cstdint>

class DnEpochObserver {
public:
    explicit DnEpochObserver(const DnNetworkConfig *config);

    void Configure(const DnNetworkConfig *config);

    /*
     * Called when AppendEntries is received.
     * projected = sender_epoch + ceil(one_way_latency / epoch_duration_ms)
     */
    void RecordObservedEpoch(int32_t sender_epoch, int from_node, int to_node);

    void RecordObservedEpochDirect(int32_t projected_epoch);

    int32_t GetMaxObservedEpoch() const;

    void Reset();

private:
    const DnNetworkConfig *m_config;
    std::atomic<int32_t> m_maxObservedEpoch;

    int32_t CompensateEpoch(int32_t sender_epoch, int from_node, int to_node) const;
};

#endif /* DYNAMIC_NET_DN_EPOCH_OBSERVER_H */
