#include "dn_slow_node_apply.h"

#include <algorithm>

DnSlowNodeApplyResult DnSlowNodeApply::ApplySnapshot(const DnPartialSnapshotMessage &snap,
                                                     const std::vector<DnTransaction> &all_txns,
                                                     std::unordered_map<int32_t, DnPeerLogState> *logs)
{
    DnSlowNodeApplyResult result;
    result.epoch = snap.epoch;

    if (logs == nullptr) {
        return result;
    }

    for (const auto &contrib : snap.contributions) {
        int32_t origin = contrib.origin_node_id;
        DnPeerLogState &log = (*logs)[origin];

        DnRaftEntry entry;
        entry.epoch = snap.epoch;
        entry.type = contrib.committed_txns.empty() ? DnEntryType::NOOP : DnEntryType::WRITE;
        entry.txns = contrib.committed_txns;
        entry.index = static_cast<int32_t>(log.entries.size());

        log.entries.push_back(entry);
        int new_commit = static_cast<int32_t>(log.entries.size()) - 1;
        int old_commit = log.commit_index.load(std::memory_order_relaxed);
        if (new_commit > old_commit) {
            log.commit_index.store(new_commit, std::memory_order_release);
        }
        result.channels_updated++;
    }

    (void)all_txns;
    result.applied = result.channels_updated > 0;
    return result;
}
