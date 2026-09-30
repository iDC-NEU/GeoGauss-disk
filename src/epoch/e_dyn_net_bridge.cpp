#include "postgres.h"

#include "epoch/e_dyn_net.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_types.h"

#include "dn_hash.h"
#include "dn_types.h"

#include <cstdio>
#include <cstring>

static DnRow EpochWireRowToDnRow(const E_WireRow& row)
{
    DnRow dn;
    char key_buf[512];
    int rc = snprintf_s(key_buf, sizeof(key_buf), sizeof(key_buf) - 1, "%s:%u:%u:%u:%u:%u",
                        row.relname.c_str(), row.relid, row.bi_hi, row.bi_lo, row.ip_posid,
                        static_cast<unsigned>(row.op_type));
    securec_check_ss(rc, "\0", "\0");
    dn.key = key_buf;
    dn.value.assign(row.tuple_data.begin(), row.tuple_data.end());
    dn.is_delete = (row.op_type == EPOCH_OP_DELETE);
    return dn;
}

static bool EpochParseDnRowKey(const std::string& key, E_WireRow* row)
{
    if (row == nullptr) {
        return false;
    }
    unsigned op = 0;
    char relname[256];
    relname[0] = '\0';
    unsigned relid = 0;
    unsigned bi_hi = 0;
    unsigned bi_lo = 0;
    unsigned ip_posid = 0;
    int matched = sscanf_s(key.c_str(), "%255[^:]:%u:%u:%u:%u:%u", relname, sizeof(relname), &relid,
                           &bi_hi, &bi_lo, &ip_posid, &op);
    if (matched < 6) {
        return false;
    }
    row->relname = relname;
    row->relid = relid;
    row->bi_hi = static_cast<uint16_t>(bi_hi);
    row->bi_lo = static_cast<uint16_t>(bi_lo);
    row->ip_posid = static_cast<uint16_t>(ip_posid);
    row->op_type = static_cast<EpochOpType>(op);
    return true;
}

DnRaftEntry EpochDynNetWireToRaftEntry(const E_WireTxn& txn)
{
    DnRaftEntry entry;
    entry.epoch = static_cast<int32_t>(txn.epoch);
    entry.type = txn.rows.empty() ? DnEntryType::NOOP : DnEntryType::WRITE;
    entry.index = 0;
    entry.term = 0;

    DnTransaction dn_txn;
    dn_txn.client_txn_id = txn.csn;
    dn_txn.origin_node_id = txn.server_id;
    char key_buf[64];
    int rc = snprintf_s(key_buf, sizeof(key_buf), sizeof(key_buf) - 1, "%llu:%u",
                        static_cast<unsigned long long>(txn.csn), txn.server_id);
    securec_check_ss(rc, "\0", "\0");
    dn_txn.client_key = key_buf;
    for (const auto& row : txn.rows) {
        dn_txn.rows.push_back(EpochWireRowToDnRow(row));
    }
    if (!txn.rows.empty() || txn.csn != 0) {
        entry.txns.push_back(std::move(dn_txn));
    }
    return entry;
}

E_WireTxn EpochDynNetDnTxnToWire(const DnTransaction& txn, int32_t epoch)
{
    E_WireTxn wire;
    wire.csn = txn.client_txn_id;
    wire.epoch = static_cast<uint64_t>(epoch);
    wire.server_id = txn.origin_node_id;
    wire.txn_state = EPOCH_TXN_COMMIT;

    for (const auto& dn_row : txn.rows) {
        E_WireRow row;
        if (!EpochParseDnRowKey(dn_row.key, &row)) {
            row.relname = dn_row.key;
            row.op_type = dn_row.is_delete ? EPOCH_OP_DELETE : EPOCH_OP_UPDATE;
        }
        row.tuple_data.assign(dn_row.value.begin(), dn_row.value.end());
        wire.rows.push_back(std::move(row));
    }
    return wire;
}

uint64_t EpochDynNetWireEntryHash(const E_WireTxn& txn)
{
    DnRaftEntry entry = EpochDynNetWireToRaftEntry(txn);
    return DnHashRaftEntry(entry);
}
