#include "dn_snapshot_validator.h"
#include "dn_hash.h"

void DnSlowNodeMetadataStore::RecordMetadata(int32_t epoch, int32_t sender_channel, int32_t term,
                                             uint64_t payload_hash, int32_t payload_size)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    DnEpochMetadata &meta = m_table[epoch][sender_channel];
    meta.epoch = epoch;
    meta.received = true;
    meta.term = term;
    meta.payload_hash = payload_hash;
    meta.payload_size = payload_size;
}

bool DnSlowNodeMetadataStore::HasMetadata(int32_t epoch, int32_t sender_channel) const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto eit = m_table.find(epoch);
    if (eit == m_table.end()) {
        return false;
    }
    auto cit = eit->second.find(sender_channel);
    return cit != eit->second.end() && cit->second.received;
}

const DnEpochMetadata *DnSlowNodeMetadataStore::GetMetadata(int32_t epoch, int32_t sender_channel) const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto eit = m_table.find(epoch);
    if (eit == m_table.end()) {
        return nullptr;
    }
    auto cit = eit->second.find(sender_channel);
    if (cit == eit->second.end()) {
        return nullptr;
    }
    return &cit->second;
}

void DnSlowNodeMetadataStore::GcBeforeEpoch(int32_t epoch)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_table.begin();
    while (it != m_table.end()) {
        if (it->first < epoch) {
            it = m_table.erase(it);
        } else {
            ++it;
        }
    }
}

DnSnapshotValidationResult DnSnapshotValidator::Validate(const DnPartialSnapshotMessage &snap,
                                                         const DnSlowNodeMetadataStore &meta_store,
                                                         std::vector<DnTransaction> *out_all_txns)
{
    if (out_all_txns != nullptr) {
        out_all_txns->clear();
    }

    for (const auto &contrib : snap.contributions) {
        int32_t nid = contrib.origin_node_id;
        if (!meta_store.HasMetadata(snap.epoch, nid)) {
            return DnSnapshotValidationResult::MISSING_META;
        }

        uint64_t reconstructed = 0;
        for (const auto &txn : contrib.committed_txns) {
            reconstructed = DnCombineHashes(reconstructed, DnHashTxn(txn));
            if (out_all_txns != nullptr) {
                out_all_txns->push_back(txn);
            }
        }
        for (uint64_t dh : contrib.dropped_hashes) {
            reconstructed = DnCombineHashes(reconstructed, dh);
        }

        const DnEpochMetadata *meta = meta_store.GetMetadata(snap.epoch, nid);
        if (meta == nullptr || reconstructed != meta->payload_hash) {
            return DnSnapshotValidationResult::ORIGIN_HASH_MISMATCH;
        }
    }

    if (out_all_txns != nullptr) {
        uint64_t computed = DnHashMergedTxns(*out_all_txns);
        if (computed != snap.checksum) {
            return DnSnapshotValidationResult::GLOBAL_CHECKSUM_MISMATCH;
        }
    } else {
        std::vector<DnTransaction> tmp;
        for (const auto &contrib : snap.contributions) {
            for (const auto &txn : contrib.committed_txns) {
                tmp.push_back(txn);
            }
        }
        if (DnHashMergedTxns(tmp) != snap.checksum) {
            return DnSnapshotValidationResult::GLOBAL_CHECKSUM_MISMATCH;
        }
    }

    return DnSnapshotValidationResult::OK;
}
