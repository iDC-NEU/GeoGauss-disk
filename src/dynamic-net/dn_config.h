//
// Configuration for heterogeneous network replication (dynamic-net).
//

#ifndef DYNAMIC_NET_DN_CONFIG_H
#define DYNAMIC_NET_DN_CONFIG_H

#include <cstdint>
#include <vector>

enum class DnEpochAdvancement {
    FIXED,    /* Timer 周期严格 +1，不感知网络 */
    CATCHUP   /* 观测网络 Epoch + Jump + predictive rewrite */
};

enum class DnBandwidthLimitMode {
    UNLIMIT,
    UP_LIMIT,    /* 慢节点上行受限 */
    DOWN_LIMIT,  /* 慢节点下行受限 */
    BI_LIMIT     /* 慢节点上下行同时受限 */
};

struct DnNetworkConfig {
    int node_count;
    int local_node_id;
    DnEpochAdvancement advancement;
    bool enable_proxy;

    int epoch_duration_ms;
    int lag_tolerance;          /* CATCHUP: projected_max - local > threshold → Jump */
    int queue_size_tolerance; /* CATCHUP: tx queue > threshold → Jump */
    int catchup_factor_k;       /* Jump target = projected_max + k */

    int rtt_estimate_ms;
    int peer_timeout_ms;

    long long max_tx_queue_ahead_ms;
    long long max_ctrl_queue_ahead_ms;
    long long max_rx_queue_ahead_ms;

    DnBandwidthLimitMode bandwidth_mode;
    std::vector<int> slow_node_ids;
    double upload_bandwidth_bps;   /* 0 = unlimited */
    double download_bandwidth_bps; /* 0 = unlimited */

    DnNetworkConfig()
        : node_count(3),
          local_node_id(0),
          advancement(DnEpochAdvancement::FIXED),
          enable_proxy(false),
          epoch_duration_ms(10),
          lag_tolerance(5),
          queue_size_tolerance(20),
          catchup_factor_k(3),
          rtt_estimate_ms(200),
          peer_timeout_ms(600),
          max_tx_queue_ahead_ms(800),
          max_ctrl_queue_ahead_ms(3000),
          max_rx_queue_ahead_ms(800),
          bandwidth_mode(DnBandwidthLimitMode::UNLIMIT),
          upload_bandwidth_bps(0),
          download_bandwidth_bps(0)
    {
    }
};

inline bool DnIsConfiguredSlow(const DnNetworkConfig &cfg, int node_id)
{
    for (int id : cfg.slow_node_ids) {
        if (id == node_id) {
            return true;
        }
    }
    return false;
}

inline bool DnIsFastNode(const DnNetworkConfig &cfg, int node_id)
{
    return !DnIsConfiguredSlow(cfg, node_id);
}

#endif /* DYNAMIC_NET_DN_CONFIG_H */
