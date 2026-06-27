#include "dn_hash.h"

uint64_t DnHashTxn(const DnTransaction &txn)
{
    std::string data = txn.client_key + "_" + std::to_string(txn.client_txn_id);
    uint64_t h = std::hash<std::string>{}(data);
    for (const auto &row : txn.rows) {
        h = DnCombineHashes(h, std::hash<std::string>{}(row.key));
        h = DnCombineHashes(h, std::hash<std::string>{}(row.value));
    }
    return h;
}

uint64_t DnHashMergedTxns(const std::vector<DnTransaction> &txns)
{
    uint64_t h = 0;
    for (const auto &txn : txns) {
        h = DnCombineHashes(h, DnHashTxn(txn));
    }
    return h;
}

uint64_t DnHashRaftEntry(const DnRaftEntry &entry)
{
    uint64_t h = static_cast<uint64_t>(entry.epoch) ^ (static_cast<uint64_t>(entry.index) << 16);
    for (const auto &txn : entry.txns) {
        h = DnCombineHashes(h, DnHashTxn(txn));
    }
    return h;
}

uint64_t DnCombineHashes(uint64_t a, uint64_t b)
{
    return a ^ (b + 0x9e3779b97f4a7c15ULL + (a << 6) + (a >> 2));
}
