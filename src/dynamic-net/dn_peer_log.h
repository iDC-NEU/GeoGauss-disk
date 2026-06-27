//
// Peer log state for Multi-Raft epoch barrier (dynamic-net).
//

#ifndef DYNAMIC_NET_DN_PEER_LOG_H
#define DYNAMIC_NET_DN_PEER_LOG_H

#include "dn_types.h"

#include <atomic>
#include <map>
#include <unordered_map>
#include <vector>

struct DnPeerLogState {
    std::vector<DnRaftEntry> entries;
    std::map<int32_t, DnRaftEntry> pending_entries;
    std::atomic<int32_t> commit_index;
    int32_t pending_commit_index;
    int32_t log_start_index;
    int32_t active_leader;
    int32_t channel_cur_term;
    long long last_heartbeat_time_ms;

    std::unordered_map<int32_t, int32_t> match_index;
    std::unordered_map<int32_t, int32_t> next_index;

    DnPeerLogState();
    DnPeerLogState(DnPeerLogState &&other) noexcept;
    DnPeerLogState &operator=(DnPeerLogState &&other) noexcept;
    DnPeerLogState(const DnPeerLogState &) = delete;
    DnPeerLogState &operator=(const DnPeerLogState &) = delete;
};

#endif /* DYNAMIC_NET_DN_PEER_LOG_H */
