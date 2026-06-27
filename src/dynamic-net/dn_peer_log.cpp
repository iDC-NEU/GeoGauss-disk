#include "dn_peer_log.h"

DnPeerLogState::DnPeerLogState()
    : commit_index(0),
      pending_commit_index(0),
      log_start_index(0),
      active_leader(-1),
      channel_cur_term(1),
      last_heartbeat_time_ms(0)
{
}

DnPeerLogState::DnPeerLogState(DnPeerLogState &&other) noexcept
    : entries(std::move(other.entries)),
      pending_entries(std::move(other.pending_entries)),
      commit_index(other.commit_index.load(std::memory_order_relaxed)),
      pending_commit_index(other.pending_commit_index),
      log_start_index(other.log_start_index),
      active_leader(other.active_leader),
      channel_cur_term(other.channel_cur_term),
      last_heartbeat_time_ms(other.last_heartbeat_time_ms),
      match_index(std::move(other.match_index)),
      next_index(std::move(other.next_index))
{
}

DnPeerLogState &DnPeerLogState::operator=(DnPeerLogState &&other) noexcept
{
    if (this == &other) {
        return *this;
    }
    entries = std::move(other.entries);
    pending_entries = std::move(other.pending_entries);
    commit_index.store(other.commit_index.load(std::memory_order_relaxed), std::memory_order_relaxed);
    pending_commit_index = other.pending_commit_index;
    log_start_index = other.log_start_index;
    active_leader = other.active_leader;
    channel_cur_term = other.channel_cur_term;
    last_heartbeat_time_ms = other.last_heartbeat_time_ms;
    match_index = std::move(other.match_index);
    next_index = std::move(other.next_index);
    return *this;
}
