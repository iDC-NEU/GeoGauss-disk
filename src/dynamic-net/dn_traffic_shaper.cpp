//
// Bandwidth / latency shaping implementation.
//

#include "dn_traffic_shaper.h"

#include <algorithm>
#include <cmath>

DnTrafficShaper::DnTrafficShaper(const DnNetworkConfig *config)
    : m_config(config),
      m_lastTxFinishCtrl(0),
      m_lastTxFinishData(0),
      m_lastRxFinishCtrl(0),
      m_lastRxFinishData(0)
{
    if (m_config != nullptr && m_config->node_count > 0) {
        m_latencyMatrix.assign(m_config->node_count, std::vector<int>(m_config->node_count, 50));
        for (int i = 0; i < m_config->node_count; ++i) {
            m_latencyMatrix[i][i] = 1;
        }
    }
}

void DnTrafficShaper::Configure(const DnNetworkConfig *config)
{
    m_config = config;
}

void DnTrafficShaper::SetLatencyMatrix(const std::vector<std::vector<int>> &matrix)
{
    m_latencyMatrix = matrix;
}

long long DnTrafficShaper::BandwidthCostMs(size_t bytes, double bps)
{
    if (bps <= 0.0 || bytes == 0) {
        return 0;
    }
    double seconds = (static_cast<double>(bytes) * 8.0) / bps;
    return static_cast<long long>(std::ceil(seconds * 1000.0));
}

double DnTrafficShaper::EffectiveUploadBps() const
{
    if (m_config == nullptr) {
        return 0.0;
    }
    if (!DnIsConfiguredSlow(*m_config, m_config->local_node_id)) {
        return 0.0;
    }
    switch (m_config->bandwidth_mode) {
        case DnBandwidthLimitMode::UP_LIMIT:
        case DnBandwidthLimitMode::BI_LIMIT:
            return m_config->upload_bandwidth_bps;
        default:
            return 0.0;
    }
}

double DnTrafficShaper::EffectiveDownloadBps() const
{
    if (m_config == nullptr) {
        return 0.0;
    }
    if (!DnIsConfiguredSlow(*m_config, m_config->local_node_id)) {
        return 0.0;
    }
    switch (m_config->bandwidth_mode) {
        case DnBandwidthLimitMode::DOWN_LIMIT:
        case DnBandwidthLimitMode::BI_LIMIT:
            return m_config->download_bandwidth_bps;
        default:
            return 0.0;
    }
}

bool DnTrafficShaper::ShouldLimitTx() const
{
    return EffectiveUploadBps() > 0.0;
}

bool DnTrafficShaper::ShouldLimitRx() const
{
    return EffectiveDownloadBps() > 0.0;
}

long long DnTrafficShaper::CalculateTxCost(size_t packet_size, bool is_high_priority) const
{
    (void)is_high_priority;
    return BandwidthCostMs(packet_size, EffectiveUploadBps());
}

long long DnTrafficShaper::CalculateLatency(int from_node, int to_node) const
{
    if (from_node < 0 || to_node < 0) {
        return 50;
    }
    if (from_node >= static_cast<int>(m_latencyMatrix.size())) {
        return 50;
    }
    if (to_node >= static_cast<int>(m_latencyMatrix[from_node].size())) {
        return 50;
    }
    return m_latencyMatrix[from_node][to_node];
}

void DnTrafficShaper::UpdateBusyState(long long finish_time_ms, bool is_high_priority)
{
    if (is_high_priority) {
        m_lastTxFinishCtrl = std::max(m_lastTxFinishCtrl, finish_time_ms);
    } else {
        m_lastTxFinishData = std::max(m_lastTxFinishData, finish_time_ms);
    }
}

long long DnTrafficShaper::GetLastFinishTime(bool is_high_priority) const
{
    return is_high_priority ? m_lastTxFinishCtrl : m_lastTxFinishData;
}

long long DnTrafficShaper::GetRxQueueAheadMs(long long arrival_time_ms, bool is_high_priority) const
{
    long long last = is_high_priority ? m_lastRxFinishCtrl : m_lastRxFinishData;
    return std::max(0LL, last - arrival_time_ms);
}

long long DnTrafficShaper::CalculateRxFinishTime(long long arrival_time_ms, size_t packet_size,
                                                   bool is_high_priority)
{
    long long start = std::max(arrival_time_ms, is_high_priority ? m_lastRxFinishCtrl : m_lastRxFinishData);
    long long cost = BandwidthCostMs(packet_size, EffectiveDownloadBps());
    long long finish = start + cost;
    if (is_high_priority) {
        m_lastRxFinishCtrl = finish;
    } else {
        m_lastRxFinishData = finish;
    }
    return finish;
}
