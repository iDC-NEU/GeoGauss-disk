#include "postgres.h"

#include "epoch/e_txn_wire.h"

#include "access/htup.h"

#include <cstring>

static void AppendU16(std::string* out, uint16_t v)
{
    out->append(reinterpret_cast<const char*>(&v), sizeof(v));
}

static void AppendU32(std::string* out, uint32_t v)
{
    out->append(reinterpret_cast<const char*>(&v), sizeof(v));
}

static void AppendU64(std::string* out, uint64_t v)
{
    out->append(reinterpret_cast<const char*>(&v), sizeof(v));
}

static bool ReadU32(const char*& p, const char* end, uint32_t* v)
{
    if (p + sizeof(uint32_t) > end) {
        return false;
    }
    memcpy(v, p, sizeof(uint32_t));
    p += sizeof(uint32_t);
    return true;
}

static bool ReadU64(const char*& p, const char* end, uint64_t* v)
{
    if (p + sizeof(uint64_t) > end) {
        return false;
    }
    memcpy(v, p, sizeof(uint64_t));
    p += sizeof(uint64_t);
    return true;
}

uint64_t E_WireRowExpectedCsn(const E_WireRow& row)
{
    if (row.expected_csn != 0) {
        return row.expected_csn;
    }
    if (row.tuple_data.size() >= offsetof(HeapTupleHeaderData, m_rowCSN) + sizeof(uint64_t)) {
        const auto* hdr = reinterpret_cast<const HeapTupleHeaderData*>(row.tuple_data.data());
        return hdr->m_rowCSN;
    }
    return 0;
}

bool E_WireSerializeTxn(const E_WireTxn& txn, std::string* out)
{
    if (out == nullptr) {
        return false;
    }
    out->clear();
    AppendU64(out, EPOCH_WIRE_MAGIC);
    AppendU32(out, EPOCH_WIRE_VERSION);
    AppendU64(out, txn.csn);
    AppendU64(out, txn.epoch);
    AppendU32(out, txn.server_id);
    AppendU32(out, static_cast<uint32_t>(txn.txn_state));
    if (EPOCH_WIRE_VERSION >= 6) {
        AppendU16(out, txn.route_wid);
    }
    AppendU32(out, static_cast<uint32_t>(txn.rows.size()));

    for (const auto& row : txn.rows) {
        AppendU32(out, static_cast<uint32_t>(row.op_type));
        AppendU32(out, row.relid);
        AppendU16(out, row.bi_hi);
        AppendU16(out, row.bi_lo);
        AppendU16(out, row.ip_posid);
        if (EPOCH_WIRE_VERSION >= 3) {
            AppendU64(out, row.expected_csn);
        }
        AppendU32(out, static_cast<uint32_t>(row.tuple_data.size()));
        if (!row.tuple_data.empty()) {
            out->append(row.tuple_data.data(), row.tuple_data.size());
        }
        AppendU32(out, static_cast<uint32_t>(row.relname.size()));
        if (!row.relname.empty()) {
            out->append(row.relname.data(), row.relname.size());
        }
        if (EPOCH_WIRE_VERSION >= 5) {
            AppendU16(out, row.pk_len);
            if (row.pk_len > 0) {
                out->append(row.pk.data(), row.pk_len);
            }
        }
    }
    return out->size() <= EPOCH_MAX_WIRE_TXN_BYTES;
}

static bool ReadU16(const char*& p, const char* end, uint16_t* v)
{
    if (p + sizeof(uint16_t) > end) {
        return false;
    }
    memcpy(v, p, sizeof(uint16_t));
    p += sizeof(uint16_t);
    return true;
}

bool E_WireDeserializeTxn(const char* data, size_t len, E_WireTxn* out)
{
    if (data == nullptr || out == nullptr || len < sizeof(uint64_t)) {
        return false;
    }
    const char* p = data;
    const char* end = data + len;
    uint64_t magic = 0;
    uint32_t version = 0;
    uint32_t row_count = 0;

    if (!ReadU64(p, end, &magic) || magic != EPOCH_WIRE_MAGIC) {
        return false;
    }
    if (!ReadU32(p, end, &version) ||
        (version != EPOCH_WIRE_VERSION && version != 5 && version != 4 && version != 3 && version != 2 &&
            version != 1)) {
        return false;
    }
    if (!ReadU64(p, end, &out->csn)) {
        return false;
    }
    if (version >= 4) {
        if (!ReadU64(p, end, &out->epoch)) {
            return false;
        }
    } else {
        uint64_t start_epoch = 0;
        uint64_t commit_epoch = 0;
        if (!ReadU64(p, end, &start_epoch) || !ReadU64(p, end, &commit_epoch)) {
            return false;
        }
        out->epoch = commit_epoch;
        (void)start_epoch;
    }
    if (!ReadU32(p, end, &out->server_id)) {
        return false;
    }
    uint32_t state = 0;
    if (!ReadU32(p, end, &state)) {
        return false;
    }
    out->txn_state = static_cast<EpochTxnState>(state);
    out->route_wid = 0;
    if (version >= 6) {
        if (!ReadU16(p, end, &out->route_wid)) {
            return false;
        }
    }
    if (!ReadU32(p, end, &row_count)) {
        return false;
    }
    out->rows.clear();
    out->rows.reserve(row_count);

    for (uint32_t i = 0; i < row_count; ++i) {
        E_WireRow row;
        uint32_t op = 0;
        uint32_t tuple_len = 0;
        if (!ReadU32(p, end, &op) || !ReadU32(p, end, &row.relid) || !ReadU16(p, end, &row.bi_hi) ||
            !ReadU16(p, end, &row.bi_lo) || !ReadU16(p, end, &row.ip_posid)) {
            return false;
        }
        row.op_type = static_cast<EpochOpType>(op);
        if (version >= 3) {
            if (!ReadU64(p, end, &row.expected_csn)) {
                return false;
            }
        }
        if (!ReadU32(p, end, &tuple_len)) {
            return false;
        }
        if (p + tuple_len > end) {
            return false;
        }
        if (tuple_len > 0) {
            row.tuple_data.assign(p, p + tuple_len);
            p += tuple_len;
        }
        if (version >= 2) {
            uint32_t name_len = 0;
            if (!ReadU32(p, end, &name_len) || p + name_len > end) {
                return false;
            }
            if (name_len > 0) {
                row.relname.assign(p, p + name_len);
                p += name_len;
            }
        }
        if (version >= 5) {
            if (!ReadU16(p, end, &row.pk_len) || row.pk_len > EPOCH_WIRE_MAX_PK) {
                return false;
            }
            if (row.pk_len > 0) {
                if (p + row.pk_len > end) {
                    return false;
                }
                row.pk.assign(p, p + row.pk_len);
                p += row.pk_len;
            }
        }
        out->rows.push_back(std::move(row));
    }
    return true;
}

bool E_WirePeekRouteWid(const char* data, size_t len, uint16_t* out)
{
    if (out == nullptr) {
        return false;
    }
    *out = 0;
    if (data == nullptr || len < sizeof(uint64_t) + sizeof(uint32_t)) {
        return false;
    }
    const char* p = data;
    const char* end = data + len;
    uint64_t magic = 0;
    uint32_t version = 0;
    if (!ReadU64(p, end, &magic) || magic != EPOCH_WIRE_MAGIC) {
        return false;
    }
    if (!ReadU32(p, end, &version)) {
        return false;
    }
    if (version < 6) {
        return true;
    }
    uint64_t skip64 = 0;
    uint32_t skip32 = 0;
    if (!ReadU64(p, end, &skip64) || !ReadU64(p, end, &skip64) || !ReadU32(p, end, &skip32) ||
        !ReadU32(p, end, &skip32)) {
        return false;
    }
    (void)skip64;
    (void)skip32;
    return ReadU16(p, end, out);
}

bool E_MergeTaskSerialize(const E_WireTxn& txn, std::string* out)
{
    if (out == nullptr) {
        return false;
    }
    out->clear();
    AppendU64(out, EPOCH_MERGE_TASK_MAGIC);
    AppendU32(out, EPOCH_MERGE_TASK_VERSION);
    AppendU64(out, txn.csn);
    AppendU64(out, txn.epoch);
    AppendU32(out, txn.server_id);
    AppendU32(out, static_cast<uint32_t>(txn.txn_state));
    if (EPOCH_MERGE_TASK_VERSION >= 2) {
        AppendU16(out, txn.route_wid);
    }
    AppendU32(out, static_cast<uint32_t>(txn.rows.size()));

    for (const auto& row : txn.rows) {
        AppendU32(out, static_cast<uint32_t>(row.op_type));
        AppendU32(out, row.relid);
        AppendU16(out, row.bi_hi);
        AppendU16(out, row.bi_lo);
        AppendU16(out, row.ip_posid);
        AppendU64(out, row.expected_csn);
        AppendU16(out, row.pk_len);
        if (row.pk_len > 0) {
            out->append(row.pk.data(), row.pk_len);
        }
        AppendU32(out, static_cast<uint32_t>(row.tuple_data.size()));
        if (!row.tuple_data.empty()) {
            out->append(row.tuple_data.data(), row.tuple_data.size());
        }
        AppendU32(out, static_cast<uint32_t>(row.relname.size()));
        if (!row.relname.empty()) {
            out->append(row.relname.data(), row.relname.size());
        }
    }
    return out->size() <= EPOCH_MAX_WIRE_TXN_BYTES;
}

bool E_MergeTaskDeserialize(const char* data, size_t len, E_WireTxn* out)
{
    if (data == nullptr || out == nullptr || len < sizeof(uint64_t)) {
        return false;
    }
    const char* p = data;
    const char* end = data + len;
    uint64_t magic = 0;
    uint32_t version = 0;
    uint32_t row_count = 0;

    if (!ReadU64(p, end, &magic) || magic != EPOCH_MERGE_TASK_MAGIC) {
        return false;
    }
    if (!ReadU32(p, end, &version) || (version != EPOCH_MERGE_TASK_VERSION && version != 1)) {
        return false;
    }
    if (!ReadU64(p, end, &out->csn) || !ReadU64(p, end, &out->epoch) || !ReadU32(p, end, &out->server_id)) {
        return false;
    }
    uint32_t state = 0;
    if (!ReadU32(p, end, &state)) {
        return false;
    }
    out->txn_state = static_cast<EpochTxnState>(state);
    out->route_wid = 0;
    if (version >= 2) {
        if (!ReadU16(p, end, &out->route_wid)) {
            return false;
        }
    }
    if (!ReadU32(p, end, &row_count)) {
        return false;
    }
    out->rows.clear();
    out->rows.reserve(row_count);

    for (uint32_t i = 0; i < row_count; ++i) {
        E_WireRow row;
        uint32_t op = 0;
        uint32_t tuple_len = 0;
        if (!ReadU32(p, end, &op) || !ReadU32(p, end, &row.relid) || !ReadU16(p, end, &row.bi_hi) ||
            !ReadU16(p, end, &row.bi_lo) || !ReadU16(p, end, &row.ip_posid) || !ReadU64(p, end, &row.expected_csn) ||
            !ReadU16(p, end, &row.pk_len) || row.pk_len > EPOCH_WIRE_MAX_PK) {
            return false;
        }
        row.op_type = static_cast<EpochOpType>(op);
        if (row.pk_len > 0) {
            if (p + row.pk_len > end) {
                return false;
            }
            row.pk.assign(p, p + row.pk_len);
            p += row.pk_len;
        }
        if (!ReadU32(p, end, &tuple_len) || p + tuple_len > end) {
            return false;
        }
        if (tuple_len > 0) {
            row.tuple_data.assign(p, p + tuple_len);
            p += tuple_len;
        }
        uint32_t name_len = 0;
        if (!ReadU32(p, end, &name_len) || p + name_len > end) {
            return false;
        }
        if (name_len > 0) {
            row.relname.assign(p, p + name_len);
            p += name_len;
        }
        out->rows.push_back(std::move(row));
    }
    return true;
}
