//
// Per-epoch partial state for fast-node merge and proxy snapshot (dynamic-net).
//

#ifndef DYNAMIC_NET_DN_PARTIAL_EPOCH_STATE_H
#define DYNAMIC_NET_DN_PARTIAL_EPOCH_STATE_H

#include "dn_config.h"
#include "dn_crdt_merge.h"
#include "dn_types.h"

#include <map>
#include <mutex>
#include <unordered_map>
#include <vector>

int DnGetAggregatorId(int32_t epoch, int node_count);

struct DnPartialEpochState {
    int32_t epoch;
    std::unordered_map<int32_t, DnRaftEntry> fast_node_entries;
    std::vector<DnTransaction> merged_txns;
    std::unordered_map<int32_t, std::vector<uint64_t>> dropped_hashes;
    bool fast_consensus_done;
    bool proxy_sent;
    uint64_t merged_hash;

    DnPartialEpochState() : epoch(0), fast_consensus_done(false), proxy_sent(false), merged_hash(0) {}
};

struct DnFastHashTracker {
    int32_t epoch;
    std::unordered_map<int32_t, uint64_t> node_hashes;
    bool quorum_reached;

    DnFastHashTracker() : epoch(0), quorum_reached(false) {}
};

class DnPartialEpochRegistry {
public:
    explicit DnPartialEpochRegistry(const DnNetworkConfig *config);

    void Configure(const DnNetworkConfig *config);

    void RecordFastNodeEntry(int32_t epoch, int32_t channel_id, const DnRaftEntry &entry);

    /* Append txn rows into an existing per-channel entry (epoch wire path). */
    void AppendFastNodeEntry(int32_t epoch, int32_t channel_id, const DnRaftEntry &entry);

    /*
     * When all fast nodes contributed, run deterministic CRDT merge.
     * Returns true if merge completed this call.
     */
    bool TryFastPathMerge(int32_t epoch, DnPartialEpochState *out_state);

    bool ShouldSendProxy(int32_t epoch, int32_t local_node_id) const;

    void MarkProxySent(int32_t epoch);

    DnPartialEpochState *GetState(int32_t epoch);
    const DnPartialEpochState *GetState(int32_t epoch) const;

    void RecordFastHash(int32_t epoch, int32_t node_id, uint64_t hash, int majority);
    bool IsHashQuorumReached(int32_t epoch) const;

    void GcBeforeEpoch(int32_t epoch);

private:
    const DnNetworkConfig *m_config;
    DnCrdtMerger m_merger;
    mutable std::mutex m_mutex;
    std::map<int32_t, DnPartialEpochState> m_epochs;
    std::unordered_map<int32_t, DnFastHashTracker> m_hashTrackers;
    std::vector<int32_t> m_fastNodeIds;
};

#endif /* DYNAMIC_NET_DN_PARTIAL_EPOCH_STATE_H */
