//
// Observed epoch tracking implementation.
//

#include "dn_epoch_observer.h"

#include <algorithm>
#include <cmath>
#include <vector>

namespace {

/* Default latency matrix when not wired to DnTrafficShaper */
int DefaultLatencyMs(int from, int to, int node_count)
{
    if (from == to) {
        return 1;
    }
    if (from < 0 || to < 0 || from >= node_count || to >= node_count) {
        return 50;
    }
    return (from == 0 || to == 0) ? 100 : 50;
}

}  // namespace

DnEpochObserver::DnEpochObserver(const DnNetworkConfig *config)
    : m_config(config), m_maxObservedEpoch(0)
{
}

void DnEpochObserver::Configure(const DnNetworkConfig *config)
{
    m_config = config;
}

int32_t DnEpochObserver::CompensateEpoch(int32_t sender_epoch, int from_node, int to_node) const
{
    if (m_config == nullptr || m_config->epoch_duration_ms <= 0) {
        return sender_epoch;
    }
    int latency = DefaultLatencyMs(from_node, to_node, m_config->node_count);
    int offset = static_cast<int>(std::ceil(static_cast<double>(latency) / m_config->epoch_duration_ms));
    return sender_epoch + offset;
}

void DnEpochObserver::RecordObservedEpoch(int32_t sender_epoch, int from_node, int to_node)
{
    int32_t projected = CompensateEpoch(sender_epoch, from_node, to_node);
    RecordObservedEpochDirect(projected);
}

void DnEpochObserver::RecordObservedEpochDirect(int32_t projected_epoch)
{
    int32_t current = m_maxObservedEpoch.load(std::memory_order_relaxed);
    while (projected_epoch > current) {
        if (m_maxObservedEpoch.compare_exchange_weak(current, projected_epoch, std::memory_order_relaxed)) {
            break;
        }
    }
}

int32_t DnEpochObserver::GetMaxObservedEpoch() const
{
    return m_maxObservedEpoch.load(std::memory_order_acquire);
}

void DnEpochObserver::Reset()
{
    m_maxObservedEpoch.store(0, std::memory_order_relaxed);
}
