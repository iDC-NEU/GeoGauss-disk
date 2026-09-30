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
    /* slot 满等不可恢复于 abort 路径，须 merge 重试 */
    bool resource_failure;
    std::vector<std::pair<uint64_t, uint32_t>> aborted_txns;
};

/* 按 (relid, 主键) CRDT merge：同 epoch 槽内 row.csn > T.csn 则 T 胜（first-write-wins） */
class E_CrdtMerger {
public:
    bool MergeRow(const E_WireRow& row, uint64_t csn, uint32_t server_id);
    E_CrdtMergeOutcome MergeWireTxn(const E_WireTxn& txn);
    bool BuildMergedTxn(uint64_t epoch, uint64_t base_csn, uint32_t server_id, E_WireTxn* out) const;
    /* worker 后台落盘：本节点 CRDT 获胜行（原由 backend O_RecordCommit 写入） */
    bool BuildLocalApplyTxn(uint64_t epoch, uint32_t local_node, E_WireTxn* out) const;
    /*
     * 按原始 wire txn 构造 apply 载荷：非 READ 行须在 map_ 中仍为 (csn, server_id) 赢家。
     * 调用方须已确认不在 abort_csn 中。
     */
    bool WireTxnWriteSetWonMerge(const E_WireTxn& source, E_WireTxn* out) const;
    bool HasRemoteRows(uint32_t local_node) const;
    bool HasLocalRows(uint32_t local_node) const;
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
    /* dry-run：只判定本行能否写入 map_，不修改 merger 状态 */
    bool PreviewMergeRowInternal(const E_WireRow& row, uint64_t csn, uint32_t server_id, bool* accepted) const;
    bool MergeRowInternal(const E_WireRow& row, uint64_t csn, uint32_t server_id, bool* accepted,
        std::vector<std::pair<uint64_t, uint32_t>>* aborted_txns);

    std::unordered_map<Key, Entry, KeyHash> map_;
    /* 同表同主键 INSERT 竞争；须含 relid（TPCC oorder/new_order PK 值相同） */
    std::unordered_map<Key, InsertWinner, KeyHash> insert_winners_;
};

#endif /* OPENGAUSS_E_CRDT_H */
