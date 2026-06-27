#include "dn_partial_epoch_state.h"
#include "dn_config.h"
#include "dn_hash.h"

DnPartialEpochRegistry::DnPartialEpochRegistry(const DnNetworkConfig *config) : m_config(config)
{
    if (m_config != nullptr) {
        for (int i = 0; i < m_config->node_count; ++i) {
            if (DnIsFastNode(*m_config, i)) {
                m_fastNodeIds.push_back(i);
            }
        }
    }
}

void DnPartialEpochRegistry::Configure(const DnNetworkConfig *config)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_config = config;
    m_fastNodeIds.clear();
    if (m_config != nullptr) {
        for (int i = 0; i < m_config->node_count; ++i) {
            if (DnIsFastNode(*m_config, i)) {
                m_fastNodeIds.push_back(i);
            }
        }
    }
}

void DnPartialEpochRegistry::RecordFastNodeEntry(int32_t epoch, int32_t channel_id, const DnRaftEntry &entry)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    DnPartialEpochState &pes = m_epochs[epoch];
    pes.epoch = epoch;
    pes.fast_node_entries[channel_id] = entry;
}

void DnPartialEpochRegistry::AppendFastNodeEntry(int32_t epoch, int32_t channel_id,
                                                 const DnRaftEntry &entry)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    DnPartialEpochState &pes = m_epochs[epoch];
    pes.epoch = epoch;
    DnRaftEntry &slot = pes.fast_node_entries[channel_id];
    if (slot.txns.empty()) {
        slot = entry;
        slot.epoch = epoch;
        return;
    }
    slot.epoch = epoch;
    slot.type = DnEntryType::WRITE;
    for (const auto &txn : entry.txns) {
        slot.txns.push_back(txn);
    }
}

bool DnPartialEpochRegistry::TryFastPathMerge(int32_t epoch, DnPartialEpochState *out_state)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_epochs.find(epoch);
    if (it == m_epochs.end()) {
        return false;
    }
    DnPartialEpochState &pes = it->second;
    if (pes.fast_consensus_done) {
        if (out_state != nullptr) {
            *out_state = pes;
        }
        return true;
    }

    if (pes.fast_node_entries.size() < m_fastNodeIds.size()) {
        return false;
    }

    DnCrdtMergeResult merged = m_merger.MergeFastNodeEntries(epoch, m_fastNodeIds, pes.fast_node_entries);
    pes.merged_txns = std::move(merged.merged_txns);
    pes.dropped_hashes = std::move(merged.dropped_hashes_by_node);
    pes.fast_node_entries = std::move(merged.filtered_entries);
    pes.merged_hash = DnHashMergedTxns(pes.merged_txns);
    pes.fast_consensus_done = true;

    DnFastHashTracker &tracker = m_hashTrackers[epoch];
    tracker.epoch = epoch;
    if (m_config != nullptr) {
        tracker.node_hashes[m_config->local_node_id] = pes.merged_hash;
    }

    if (out_state != nullptr) {
        *out_state = pes;
    }
    return true;
}

bool DnPartialEpochRegistry::ShouldSendProxy(int32_t epoch, int32_t local_node_id) const
{
    if (m_config == nullptr || !m_config->enable_proxy) {
        return false;
    }
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_epochs.find(epoch);
    if (it == m_epochs.end() || !it->second.fast_consensus_done || it->second.proxy_sent) {
        return false;
    }
    return DnGetAggregatorId(epoch, m_config->node_count) == local_node_id;
}

void DnPartialEpochRegistry::MarkProxySent(int32_t epoch)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_epochs[epoch].proxy_sent = true;
}

DnPartialEpochState *DnPartialEpochRegistry::GetState(int32_t epoch)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_epochs.find(epoch);
    if (it == m_epochs.end()) {
        return nullptr;
    }
    return &it->second;
}

const DnPartialEpochState *DnPartialEpochRegistry::GetState(int32_t epoch) const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_epochs.find(epoch);
    if (it == m_epochs.end()) {
        return nullptr;
    }
    return &it->second;
}

void DnPartialEpochRegistry::RecordFastHash(int32_t epoch, int32_t node_id, uint64_t hash, int majority)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    DnFastHashTracker &tracker = m_hashTrackers[epoch];
    tracker.epoch = epoch;
    tracker.node_hashes[node_id] = hash;

    std::unordered_map<uint64_t, int> counts;
    for (const auto &kv : tracker.node_hashes) {
        counts[kv.second]++;
    }
    for (const auto &kv : counts) {
        if (kv.second >= majority) {
            tracker.quorum_reached = true;
            break;
        }
    }
}

bool DnPartialEpochRegistry::IsHashQuorumReached(int32_t epoch) const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_hashTrackers.find(epoch);
    if (it == m_hashTrackers.end()) {
        return false;
    }
    return it->second.quorum_reached;
}

void DnPartialEpochRegistry::GcBeforeEpoch(int32_t epoch)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_epochs.begin();
    while (it != m_epochs.end()) {
        if (it->first < epoch) {
            it = m_epochs.erase(it);
        } else {
            ++it;
        }
    }
    auto hit = m_hashTrackers.begin();
    while (hit != m_hashTrackers.end()) {
        if (hit->first < epoch) {
            hit = m_hashTrackers.erase(hit);
        } else {
            ++hit;
        }
    }
}

int DnGetAggregatorId(int32_t epoch, int node_count)
{
    if (node_count <= 0) {
        return 0;
    }
    return epoch % node_count;
}
