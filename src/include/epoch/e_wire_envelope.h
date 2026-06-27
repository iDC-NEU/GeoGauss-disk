#ifndef OPENGAUSS_E_WIRE_ENVELOPE_H
#define OPENGAUSS_E_WIRE_ENVELOPE_H

#include <cstdint>
#include <string>

#include "dn_types.h"
#include "epoch/e_txn_wire.h"

static constexpr uint64_t EPOCH_WIRE_ENVELOPE_MAGIC = UINT64_C(0xE0C0D00D);
static constexpr uint32_t EPOCH_WIRE_ENVELOPE_VERSION = 1;

enum EpochWireEnvelopeKind : uint32_t {
    EPOCH_WIRE_ENV_TXN_METADATA = 1,
    EPOCH_WIRE_ENV_PARTIAL_SNAPSHOT = 2
};

struct EpochWireMetadata {
    uint64_t csn{0};
    uint64_t start_epoch{0};
    uint64_t commit_epoch{0};
    uint32_t server_id{0};
    uint64_t payload_hash{0};
    int32_t payload_size{0};
};

bool EpochWireIsEnvelope(const char* data, size_t len);
bool EpochWireSerializeMetadata(const E_WireTxn& txn, uint64_t payload_hash, int32_t payload_size,
                                std::string* out);
bool EpochWireParseMetadata(const char* data, size_t len, EpochWireMetadata* out);

bool EpochWireSerializePartialSnapshot(const DnPartialSnapshotMessage& snap, std::string* out);
bool EpochWireParsePartialSnapshot(const char* data, size_t len, DnPartialSnapshotMessage* out);

#endif /* OPENGAUSS_E_WIRE_ENVELOPE_H */
