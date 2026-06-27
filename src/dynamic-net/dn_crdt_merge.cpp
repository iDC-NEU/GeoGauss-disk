#include "dn_crdt_merge.h"
#include "dn_hash.h"

#include <algorithm>

uint64_t DnCrdtMerger::MakeTieBreak(int32_t epoch, uint32_t origin_node, uint64_t client_txn_id)
{
    return (static_cast<uint64_t>(epoch) << 32) ^ (static_cast<uint64_t>(origin_node) << 16) ^ client_txn_id;
}

bool DnCrdtMerger::TxnWins(uint64_t tie_a, uint64_t tie_b)
{
    return tie_a > tie_b;
}

std::vector<bool> DnCrdtMerger::ResolveEpoch(int32_t epoch,
                                             const std::vector<DnTransaction> &all_candidate_txns) const
{
    std::vector<bool> mask(all_candidate_txns.size(), true);
    std::unordered_map<std::string, RowWinner> row_winners;

    for (size_t i = 0; i < all_candidate_txns.size(); ++i) {
        const DnTransaction &txn = all_candidate_txns[i];
        uint64_t tie = MakeTieBreak(epoch, txn.origin_node_id, txn.client_txn_id);
        bool txn_survives = false;

        for (const auto &row : txn.rows) {
            auto it = row_winners.find(row.key);
            if (it == row_winners.end()) {
                row_winners[row.key] = RowWinner{tie, i};
                txn_survives = true;
            } else if (TxnWins(tie, it->second.tie_break)) {
                mask[it->second.txn_index] = false;
                it->second = RowWinner{tie, i};
                txn_survives = true;
            } else {
                txn_survives = false;
                break;
            }
        }

        if (!txn.rows.empty()) {
            mask[i] = txn_survives;
        }
    }
    return mask;
}

DnCrdtMergeResult DnCrdtMerger::MergeFastNodeEntries(
    int32_t epoch, const std::vector<int32_t> &fast_node_ids,
    const std::unordered_map<int32_t, DnRaftEntry> &fast_node_entries)
{
    DnCrdtMergeResult result;
    std::vector<DnTransaction> all_candidates;
    struct Locator {
        int32_t node_id;
        size_t txn_index_in_all;
    };
    std::vector<Locator> locators;

    for (int32_t nid : fast_node_ids) {
        auto it = fast_node_entries.find(nid);
        if (it == fast_node_entries.end()) {
            continue;
        }
        for (const auto &txn : it->second.txns) {
            all_candidates.push_back(txn);
            locators.push_back({nid, all_candidates.size() - 1});
        }
    }

    result.survival_mask = ResolveEpoch(epoch, all_candidates);

    std::unordered_map<int32_t, DnRaftEntry> filtered = fast_node_entries;
    for (auto &kv : filtered) {
        kv.second.txns.clear();
    }

    for (size_t i = 0; i < all_candidates.size(); ++i) {
        int32_t origin = locators[i].node_id;
        const DnTransaction &txn = all_candidates[i];
        if (result.survival_mask[i]) {
            result.merged_txns.push_back(txn);
            filtered[origin].txns.push_back(txn);
        } else {
            result.dropped_hashes_by_node[origin].push_back(DnHashTxn(txn));
        }
    }

    result.filtered_entries = std::move(filtered);
    return result;
}
