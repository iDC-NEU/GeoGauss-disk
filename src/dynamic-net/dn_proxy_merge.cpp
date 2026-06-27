#include "dn_proxy_merge.h"
#include "dn_config.h"
#include "dn_hash.h"

DnPartialSnapshotMessage DnProxyMerge::BuildPartialSnapshot(int32_t proxy_node_id,
                                                            const DnPartialEpochState &pes)
{
    DnPartialSnapshotMessage snap;
    snap.epoch = pes.epoch;
    snap.proxy_node_id = proxy_node_id;
    snap.checksum = pes.merged_hash;

    for (const auto &kv : pes.fast_node_entries) {
        DnNodeContribution contrib;
        contrib.origin_node_id = kv.first;

        auto dropped_it = pes.dropped_hashes.find(kv.first);
        if (dropped_it != pes.dropped_hashes.end()) {
            contrib.dropped_hashes = dropped_it->second;
        }

        for (const auto &txn : kv.second.txns) {
            contrib.committed_txns.push_back(txn);
        }
        snap.contributions.push_back(std::move(contrib));
    }

    return snap;
}

bool DnProxyMerge::ShouldSendMetadataOnly(const DnNetworkConfig &cfg, int target_node_id)
{
    return cfg.enable_proxy && DnIsConfiguredSlow(cfg, target_node_id);
}

void DnProxyMerge::FillMetadataAppend(DnAppendEntriesRequest *req, const DnRaftEntry &entry)
{
    if (req == nullptr) {
        return;
    }
    req->entries.clear();
    req->payload_hash = DnHashRaftEntry(entry);
    req->payload_size = static_cast<int32_t>(entry.txns.size());
}
