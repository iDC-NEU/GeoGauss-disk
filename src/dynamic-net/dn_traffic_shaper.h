//
// Bandwidth / latency shaping for heterogeneous network simulation.
//

#ifndef DYNAMIC_NET_DN_TRAFFIC_SHAPER_H
#define DYNAMIC_NET_DN_TRAFFIC_SHAPER_H

#include "dn_config.h"

#include <cstddef>
#include <cstdint>
#include <vector>

class DnTrafficShaper {
public:
    explicit DnTrafficShaper(const DnNetworkConfig *config);

    void Configure(const DnNetworkConfig *config);
    void SetLatencyMatrix(const std::vector<std::vector<int>> &matrix);

    long long CalculateTxCost(size_t packet_size, bool is_high_priority) const;
    long long CalculateLatency(int from_node, int to_node) const;
    void UpdateBusyState(long long finish_time_ms, bool is_high_priority);
    long long GetLastFinishTime(bool is_high_priority) const;
    long long GetRxQueueAheadMs(long long arrival_time_ms, bool is_high_priority) const;
    long long CalculateRxFinishTime(long long arrival_time_ms, size_t packet_size, bool is_high_priority);

    bool ShouldLimitTx() const;
    bool ShouldLimitRx() const;

private:
    const DnNetworkConfig *m_config;
    long long m_lastTxFinishCtrl;
    long long m_lastTxFinishData;
    long long m_lastRxFinishCtrl;
    long long m_lastRxFinishData;
    std::vector<std::vector<int>> m_latencyMatrix;

    double EffectiveUploadBps() const;
    double EffectiveDownloadBps() const;
    static long long BandwidthCostMs(size_t bytes, double bps);
};

#endif /* DYNAMIC_NET_DN_TRAFFIC_SHAPER_H */
