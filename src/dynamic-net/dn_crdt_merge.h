//
// Deterministic CRDT-style conflict resolution (LWW per row key).
//

#ifndef DYNAMIC_NET_DN_CRDT_MERGE_H
#define DYNAMIC_NET_DN_CRDT_MERGE_H

#include "dn_types.h"

#include <string>
#include <unordered_map>
#include <vector>

struct DnCrdtMergeResult {
    std::vector<DnTransaction> merged_txns;
    std::unordered_map<int32_t, std::vector<uint64_t>> dropped_hashes_by_node;
    std::unordered_map<int32_t, DnRaftEntry> filtered_entries;
    std::vector<bool> survival_mask;

    DnCrdtMergeResult() {}
};

class DnCrdtMerger {
public:
    /*
     * Resolve conflicts among candidate txns from fast nodes for one epoch.
     * Returns survival_mask aligned with all_candidate_txns input order.
     */
    std::vector<bool> ResolveEpoch(int32_t epoch, const std::vector<DnTransaction> &all_candidate_txns) const;

    /*
     * Full deterministic merge: filter entries per origin node, collect merged txns + dropped hashes.
     */
    DnCrdtMergeResult MergeFastNodeEntries(int32_t epoch, const std::vector<int32_t> &fast_node_ids,
                                           const std::unordered_map<int32_t, DnRaftEntry> &fast_node_entries);

private:
    struct RowWinner {
        uint64_t tie_break;
        size_t txn_index;
    };

    static uint64_t MakeTieBreak(int32_t epoch, uint32_t origin_node, uint64_t client_txn_id);
    static bool TxnWins(uint64_t tie_a, uint64_t tie_b);
};

#endif /* DYNAMIC_NET_DN_CRDT_MERGE_H */
