//
// Binary wire codec for dynamic-net packets (interoperable with epoch envelope magic).
//

#ifndef DYNAMIC_NET_DN_WIRE_CODEC_H
#define DYNAMIC_NET_DN_WIRE_CODEC_H

#include "dn_types.h"

#include <cstddef>
#include <string>

static constexpr uint64_t DN_WIRE_ENVELOPE_MAGIC = UINT64_C(0xE0C0D00D);
static constexpr uint32_t DN_WIRE_ENVELOPE_VERSION = 1;

enum DnWireEnvelopeKind : uint32_t {
    DN_WIRE_ENV_TXN_METADATA = 1,
    DN_WIRE_ENV_PARTIAL_SNAPSHOT = 2
};

/*
 * Decode an on-wire blob into a typed DnNetworkPacket.
 * Returns true when the blob is a recognized dynamic-net / epoch envelope.
 * Otherwise returns false and the caller should treat the blob as opaque WIRE_PAYLOAD.
 */
bool DnWireTryDecodePayload(const char *data, size_t len, int from_node_id, DnNetworkPacket *out);

bool DnWireSerializePartialSnapshot(const DnPartialSnapshotMessage &snap, std::string *out);
bool DnWireParsePartialSnapshot(const char *data, size_t len, DnPartialSnapshotMessage *out);

bool DnWireSerializeMetadataEnvelope(int32_t from_node, int32_t epoch, uint64_t payload_hash,
                                     int32_t payload_size, std::string *out);

/*
 * Serialize a network packet for ZMQ / libcomm transport.
 * WIRE_PAYLOAD returns wire_payload as-is; other kinds use native framing.
 */
bool DnWireSerializePacket(const DnNetworkPacket &pkt, std::string *out);

#endif /* DYNAMIC_NET_DN_WIRE_CODEC_H */
