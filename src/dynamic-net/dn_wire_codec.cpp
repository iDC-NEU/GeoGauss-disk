#include "dn_wire_codec.h"

#include <cstring>

namespace {

static void AppendU32(std::string *out, uint32_t v)
{
    out->append(reinterpret_cast<const char *>(&v), sizeof(v));
}

static void AppendU64(std::string *out, uint64_t v)
{
    out->append(reinterpret_cast<const char *>(&v), sizeof(v));
}

static void AppendI32(std::string *out, int32_t v)
{
    out->append(reinterpret_cast<const char *>(&v), sizeof(v));
}

static void AppendStr(std::string *out, const std::string &s)
{
    AppendU32(out, static_cast<uint32_t>(s.size()));
    if (!s.empty()) {
        out->append(s.data(), s.size());
    }
}

static bool ReadU32(const char *&p, const char *end, uint32_t *v)
{
    if (p + static_cast<ptrdiff_t>(sizeof(uint32_t)) > end) {
        return false;
    }
    memcpy(v, p, sizeof(uint32_t));
    p += sizeof(uint32_t);
    return true;
}

static bool ReadU64(const char *&p, const char *end, uint64_t *v)
{
    if (p + static_cast<ptrdiff_t>(sizeof(uint64_t)) > end) {
        return false;
    }
    memcpy(v, p, sizeof(uint64_t));
    p += sizeof(uint64_t);
    return true;
}

static bool ReadI32(const char *&p, const char *end, int32_t *v)
{
    if (p + static_cast<ptrdiff_t>(sizeof(int32_t)) > end) {
        return false;
    }
    memcpy(v, p, sizeof(int32_t));
    p += sizeof(int32_t);
    return true;
}

static bool ReadStr(const char *&p, const char *end, std::string *out)
{
    uint32_t len = 0;
    if (!ReadU32(p, end, &len)) {
        return false;
    }
    if (p + len > end) {
        return false;
    }
    out->assign(p, len);
    p += len;
    return true;
}

static bool ReadEnvelopeHeader(const char *data, size_t len, DnWireEnvelopeKind *kind)
{
    if (data == nullptr || len < sizeof(uint64_t) + sizeof(uint32_t) * 2 || kind == nullptr) {
        return false;
    }
    const char *p = data;
    const char *end = data + len;
    uint64_t magic = 0;
    uint32_t version = 0;
    uint32_t k = 0;
    if (!ReadU64(p, end, &magic) || magic != DN_WIRE_ENVELOPE_MAGIC) {
        return false;
    }
    if (!ReadU32(p, end, &version) ||
        (version != DN_WIRE_ENVELOPE_VERSION && version != 1)) {
        return false;
    }
    if (!ReadU32(p, end, &k)) {
        return false;
    }
    *kind = static_cast<DnWireEnvelopeKind>(k);
    return true;
}

static bool SerializeDnRow(const DnRow &row, std::string *out)
{
    AppendStr(out, row.key);
    AppendStr(out, row.value);
    uint8_t del = row.is_delete ? 1 : 0;
    out->append(reinterpret_cast<const char *>(&del), sizeof(del));
    return true;
}

static bool ParseDnRow(const char *&p, const char *end, DnRow *row)
{
    if (row == nullptr) {
        return false;
    }
    if (!ReadStr(p, end, &row->key) || !ReadStr(p, end, &row->value)) {
        return false;
    }
    if (p + static_cast<ptrdiff_t>(sizeof(uint8_t)) > end) {
        return false;
    }
    row->is_delete = (*p != 0);
    p += sizeof(uint8_t);
    return true;
}

static bool SerializeDnTxn(const DnTransaction &txn, std::string *out)
{
    AppendU64(out, txn.client_txn_id);
    AppendU32(out, txn.origin_node_id);
    AppendStr(out, txn.client_key);
    AppendU32(out, static_cast<uint32_t>(txn.rows.size()));
    for (const auto &row : txn.rows) {
        if (!SerializeDnRow(row, out)) {
            return false;
        }
    }
    return true;
}

static bool ParseDnTxn(const char *&p, const char *end, DnTransaction *txn)
{
    if (txn == nullptr) {
        return false;
    }
    if (!ReadU64(p, end, &txn->client_txn_id) || !ReadU32(p, end, &txn->origin_node_id) ||
        !ReadStr(p, end, &txn->client_key)) {
        return false;
    }
    uint32_t row_count = 0;
    if (!ReadU32(p, end, &row_count)) {
        return false;
    }
    txn->rows.clear();
    txn->rows.reserve(row_count);
    for (uint32_t i = 0; i < row_count; ++i) {
        DnRow row;
        if (!ParseDnRow(p, end, &row)) {
            return false;
        }
        txn->rows.push_back(std::move(row));
    }
    return true;
}

}  // namespace

bool DnWireSerializePartialSnapshot(const DnPartialSnapshotMessage &snap, std::string *out)
{
    if (out == nullptr) {
        return false;
    }
    out->clear();
    AppendU64(out, DN_WIRE_ENVELOPE_MAGIC);
    AppendU32(out, DN_WIRE_ENVELOPE_VERSION);
    AppendU32(out, static_cast<uint32_t>(DN_WIRE_ENV_PARTIAL_SNAPSHOT));
    AppendI32(out, snap.epoch);
    AppendI32(out, snap.proxy_node_id);
    AppendU64(out, snap.checksum);
    AppendU32(out, static_cast<uint32_t>(snap.contributions.size()));
    for (const auto &contrib : snap.contributions) {
        AppendI32(out, contrib.origin_node_id);
        AppendU32(out, static_cast<uint32_t>(contrib.committed_txns.size()));
        for (const auto &txn : contrib.committed_txns) {
            if (!SerializeDnTxn(txn, out)) {
                return false;
            }
        }
        AppendU32(out, static_cast<uint32_t>(contrib.dropped_hashes.size()));
        for (uint64_t h : contrib.dropped_hashes) {
            AppendU64(out, h);
        }
        AppendStr(out, contrib.reconstruction_bitmap);
    }
    return true;
}

bool DnWireParsePartialSnapshot(const char *data, size_t len, DnPartialSnapshotMessage *out)
{
    if (out == nullptr) {
        return false;
    }
    DnWireEnvelopeKind kind = DN_WIRE_ENV_PARTIAL_SNAPSHOT;
    if (!ReadEnvelopeHeader(data, len, &kind) || kind != DN_WIRE_ENV_PARTIAL_SNAPSHOT) {
        return false;
    }
    const char *p = data + sizeof(uint64_t) + sizeof(uint32_t) * 2;
    const char *end = data + len;
    if (!ReadI32(p, end, &out->epoch) || !ReadI32(p, end, &out->proxy_node_id) ||
        !ReadU64(p, end, &out->checksum)) {
        return false;
    }
    uint32_t contrib_count = 0;
    if (!ReadU32(p, end, &contrib_count)) {
        return false;
    }
    out->contributions.clear();
    out->contributions.reserve(contrib_count);
    for (uint32_t ci = 0; ci < contrib_count; ++ci) {
        DnNodeContribution contrib;
        if (!ReadI32(p, end, &contrib.origin_node_id)) {
            return false;
        }
        uint32_t txn_count = 0;
        if (!ReadU32(p, end, &txn_count)) {
            return false;
        }
        for (uint32_t ti = 0; ti < txn_count; ++ti) {
            DnTransaction txn;
            if (!ParseDnTxn(p, end, &txn)) {
                return false;
            }
            contrib.committed_txns.push_back(std::move(txn));
        }
        uint32_t drop_count = 0;
        if (!ReadU32(p, end, &drop_count)) {
            return false;
        }
        for (uint32_t di = 0; di < drop_count; ++di) {
            uint64_t h = 0;
            if (!ReadU64(p, end, &h)) {
                return false;
            }
            contrib.dropped_hashes.push_back(h);
        }
        if (!ReadStr(p, end, &contrib.reconstruction_bitmap)) {
            return false;
        }
        out->contributions.push_back(std::move(contrib));
    }
    return true;
}

bool DnWireSerializeMetadataEnvelope(int32_t from_node, int32_t epoch, uint64_t payload_hash,
                                     int32_t payload_size, std::string *out)
{
    if (out == nullptr) {
        return false;
    }
    out->clear();
    AppendU64(out, DN_WIRE_ENVELOPE_MAGIC);
    AppendU32(out, DN_WIRE_ENVELOPE_VERSION);
    AppendU32(out, static_cast<uint32_t>(DN_WIRE_ENV_TXN_METADATA));
    AppendU64(out, 0);
    AppendU64(out, static_cast<uint64_t>(epoch));
    AppendU32(out, static_cast<uint32_t>(from_node));
    AppendU64(out, payload_hash);
    AppendI32(out, payload_size);
    return true;
}

bool DnWireTryDecodePayload(const char *data, size_t len, int from_node_id, DnNetworkPacket *out)
{
    if (data == nullptr || out == nullptr || len < sizeof(uint64_t)) {
        return false;
    }

    DnWireEnvelopeKind kind = DN_WIRE_ENV_TXN_METADATA;
    if (!ReadEnvelopeHeader(data, len, &kind)) {
        return false;
    }

    out->from_node_id = from_node_id;

    if (kind == DN_WIRE_ENV_PARTIAL_SNAPSHOT) {
        out->kind = DnMsgKind::PARTIAL_SNAPSHOT;
        return DnWireParsePartialSnapshot(data, len, &out->partial_snapshot);
    }

    if (kind == DN_WIRE_ENV_TXN_METADATA) {
        const char *p = data + sizeof(uint64_t) + sizeof(uint32_t) * 2;
        const char *end = data + len;
        const char *ver_p = data + sizeof(uint64_t);
        uint32_t version = 0;
        if (!ReadU32(ver_p, end, &version) ||
            (version != DN_WIRE_ENVELOPE_VERSION && version != 1)) {
            return false;
        }
        uint64_t csn = 0;
        uint64_t epoch_val = 0;
        uint32_t server_id = 0;
        uint64_t payload_hash = 0;
        int32_t payload_size = 0;
        if (!ReadU64(p, end, &csn)) {
            return false;
        }
        if (version >= 2) {
            if (!ReadU64(p, end, &epoch_val)) {
                return false;
            }
        } else {
            uint64_t start_epoch = 0;
            uint64_t commit_epoch = 0;
            if (!ReadU64(p, end, &start_epoch) || !ReadU64(p, end, &commit_epoch)) {
                return false;
            }
            epoch_val = commit_epoch;
            (void)start_epoch;
        }
        if (!ReadU32(p, end, &server_id) || !ReadU64(p, end, &payload_hash) ||
            !ReadI32(p, end, &payload_size)) {
            return false;
        }
        (void)csn;
        out->kind = DnMsgKind::APPEND_ENTRIES;
        out->append_req.epoch = static_cast<int32_t>(epoch_val);
        out->append_req.channel_id = static_cast<int32_t>(server_id);
        out->append_req.payload_hash = payload_hash;
        out->append_req.payload_size = payload_size;
        out->append_req.entries.clear();
        return true;
    }

    return false;
}

bool DnWireSerializePacket(const DnNetworkPacket &pkt, std::string *out)
{
    if (out == nullptr) {
        return false;
    }
    if (pkt.kind == DnMsgKind::WIRE_PAYLOAD) {
        *out = pkt.wire_payload;
        return true;
    }
    if (pkt.kind == DnMsgKind::PARTIAL_SNAPSHOT) {
        return DnWireSerializePartialSnapshot(pkt.partial_snapshot, out);
    }
    if (pkt.kind == DnMsgKind::APPEND_ENTRIES && pkt.append_req.IsMetadataOnly()) {
        return DnWireSerializeMetadataEnvelope(pkt.from_node_id, pkt.append_req.epoch,
                                               pkt.append_req.payload_hash,
                                               pkt.append_req.payload_size, out);
    }
    return false;
}
