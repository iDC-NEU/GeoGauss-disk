#ifndef OPENGAUSS_E_CRDT_H
#define OPENGAUSS_E_CRDT_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "epoch/e_txn_wire.h"

struct E_CrdtMergeOutcome {
    bool txn_accepted;
    std::vector<std::pair<uint64_t, uint32_t>> aborted_txns;
};

/* 按 (relid, 主键) 做 LWW：csn 大者胜，csn 相同则 node_id 大者胜 */
class E_CrdtMerger {
public:
    bool MergeRow(const E_WireRow& row, uint64_t csn, uint32_t server_id);
    E_CrdtMergeOutcome MergeWireTxn(const E_WireTxn& txn);
    bool BuildMergedTxn(uint64_t epoch, uint64_t base_csn, uint32_t server_id, E_WireTxn* out) const;
    bool HasRemoteRows(uint32_t local_node) const;
    bool IsEmpty(void) const;
    void Clear();

private:
    struct Key {
        uint32_t relid;
        std::string pk_key;
        bool operator==(const Key& o) const;
    };
    struct KeyHash {
        size_t operator()(const Key& k) const;
    };
    struct Entry {
        E_WireRow row;
        uint64_t csn;
        uint32_t server_id;
    };
    struct InsertWinner {
        uint64_t csn;
        uint32_t server_id;
    };

    static bool RowWins(uint64_t csn_a, uint32_t sid_a, uint64_t csn_b, uint32_t sid_b);
    bool MergeRowInternal(const E_WireRow& row, uint64_t csn, uint32_t server_id, bool* accepted,
        std::vector<std::pair<uint64_t, uint32_t>>* aborted_txns);

    std::unordered_map<Key, Entry, KeyHash> map_;
    /* GeoGauss insertSetForCommit：同主键 INSERT 竞争 */
    std::unordered_map<std::string, InsertWinner> insert_winners_;
};

#endif /* OPENGAUSS_E_CRDT_H */
