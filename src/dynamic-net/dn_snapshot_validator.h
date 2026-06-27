//
// Slow-node metadata table + dual-layer PartialSnapshot validation.
//

#ifndef DYNAMIC_NET_DN_SNAPSHOT_VALIDATOR_H
#define DYNAMIC_NET_DN_SNAPSHOT_VALIDATOR_H

#include "dn_types.h"

#include <map>
#include <mutex>
#include <vector>

enum class DnSnapshotValidationResult {
    OK,
    MISSING_META,
    ORIGIN_HASH_MISMATCH,
    GLOBAL_CHECKSUM_MISMATCH
};

class DnSlowNodeMetadataStore {
public:
    void RecordMetadata(int32_t epoch, int32_t sender_channel, int32_t term, uint64_t payload_hash,
                        int32_t payload_size);

    bool HasMetadata(int32_t epoch, int32_t sender_channel) const;

    const DnEpochMetadata *GetMetadata(int32_t epoch, int32_t sender_channel) const;

    void GcBeforeEpoch(int32_t epoch);

private:
    mutable std::mutex m_mutex;
    /* epoch -> (channel -> metadata) */
    std::map<int32_t, std::map<int32_t, DnEpochMetadata>> m_table;
};

class DnSnapshotValidator {
public:
    /*
     * Layer-1: per-origin hash reconstruction vs previously received metadata.
     * Layer-2: global merged checksum vs snapshot.checksum.
     */
    static DnSnapshotValidationResult Validate(const DnPartialSnapshotMessage &snap,
                                               const DnSlowNodeMetadataStore &meta_store,
                                               std::vector<DnTransaction> *out_all_txns);
};

#endif /* DYNAMIC_NET_DN_SNAPSHOT_VALIDATOR_H */
