#include "postgres.h"

#include "epoch/e_crdt.h"
#include "epoch/e_crdt_merge_logic.h"
#include "epoch/e_config.h"
#include "epoch/e_row_key.h"
#include "epoch/e_statistician.h"

#include <unordered_map>

bool E_CrdtMerger::Key::operator==(const Key& o) const
{
    return relid == o.relid && pk_key == o.pk_key;
}

size_t E_CrdtMerger::KeyHash::operator()(const Key& k) const
{
    return std::hash<std::string>{}(k.pk_key) ^ (static_cast<size_t>(k.relid) << 1);
}

bool E_CrdtMerger::RowWins(uint64_t csn_a, uint32_t sid_a, uint64_t csn_b, uint32_t sid_b)
{
    return EpochCrdtTxnWinsOver(csn_a, sid_a, csn_b, sid_b);
}

void E_CrdtMerger::Clear()
{
    map_.clear();
    insert_winners_.clear();
}

bool E_CrdtMerger::PreviewMergeRowInternal(const E_WireRow& row, uint64_t csn, uint32_t server_id,
    bool* accepted) const
{
    if (accepted != nullptr) {
        *accepted = true;
    }

    Key key;
    Oid localRelid = E_ResolveLocalRelid(row);
    key.relid = static_cast<uint32_t>(localRelid);
    if (!E_BuildPkKeyFromWireRow(row.relid, row, &key.pk_key)) {
        if (accepted != nullptr) {
            *accepted = false;
        }
        return false;
    }

    if (row.op_type == EPOCH_OP_INSERT) {
        auto ins_it = insert_winners_.find(key);
        if (ins_it == insert_winners_.end()) {
            /* 新 INSERT 键：本 txn 可写入 */
        } else {
            const InsertWinner& winner = ins_it->second;
            if (winner.csn == csn && winner.server_id == server_id) {
                /* 同 txn 重放 */
            } else if (!RowWins(csn, server_id, winner.csn, winner.server_id)) {
                if (accepted != nullptr) {
                    *accepted = false;
                }
                return false;
            }
        }
    }

    auto it = map_.find(key);
    if (it == map_.end()) {
        return true;
    }

    const Entry& old = it->second;
    if (old.csn == csn && old.server_id == server_id) {
        return true;
    }
    if (RowWins(csn, server_id, old.csn, old.server_id)) {
        return true;
    }

    if (accepted != nullptr) {
        *accepted = false;
    }
    return false;
}

bool E_CrdtMerger::MergeRowInternal(const E_WireRow& row, uint64_t csn, uint32_t server_id, bool* accepted,
    std::vector<std::pair<uint64_t, uint32_t>>* aborted_txns)
{
    if (accepted != nullptr) {
        *accepted = true;
    }

    Key key;
    Oid localRelid = E_ResolveLocalRelid(row);
    key.relid = static_cast<uint32_t>(localRelid);
    if (!E_BuildPkKeyFromWireRow(row.relid, row, &key.pk_key)) {
        static std::unordered_map<uint32_t, int> g_pk_fail_tick;
        int& tick = g_pk_fail_tick[row.relid];
        if (++tick % 20 == 1) {
            Oid localRelid = E_ResolveLocalRelid(row);
        }
        if (accepted != nullptr) {
            *accepted = false;
        }
        return false;
    }

    if (row.op_type == EPOCH_OP_INSERT) {
        auto ins_it = insert_winners_.find(key);
        if (ins_it == insert_winners_.end()) {
            insert_winners_[key] = InsertWinner{csn, server_id};
        } else {
            InsertWinner& winner = ins_it->second;
            if (winner.csn == csn && winner.server_id == server_id) {
                /* 同 txn 重放同一行：继续写入 map_，不可 return（否则丢行） */
            } else if (RowWins(csn, server_id, winner.csn, winner.server_id)) {
                if (aborted_txns != nullptr) {
                    aborted_txns->emplace_back(winner.csn, winner.server_id);
                }
                winner = InsertWinner{csn, server_id};
            } else {
                if (accepted != nullptr) {
                    *accepted = false;
                }
                return false;
            }
        }
    }

    auto it = map_.find(key);
    if (it == map_.end()) {
        map_[key] = Entry{row, csn, server_id};
        return true;
    }

    const Entry& old = it->second;
    if (old.csn == csn && old.server_id == server_id) {
        return true;
    }
    if (RowWins(csn, server_id, old.csn, old.server_id)) {
        if (aborted_txns != nullptr) {
            aborted_txns->emplace_back(old.csn, old.server_id);
        }
        it->second = Entry{row, csn, server_id};
        return true;
    }

    if (accepted != nullptr) {
        *accepted = false;
    }
    return false;
}

bool E_CrdtMerger::MergeRow(const E_WireRow& row, uint64_t csn, uint32_t server_id)
{
    return MergeRowInternal(row, csn, server_id, nullptr, nullptr);
}

E_CrdtMergeOutcome E_CrdtMerger::MergeWireTxn(const E_WireTxn& txn)
{
    E_CrdtMergeOutcome outcome;
    outcome.txn_accepted = true;
    outcome.resource_failure = false;
    if (txn.rows.empty()) {
        return outcome;
    }

    /*
     * 与 backend wire txn 对齐：整笔 txn 要么全部写入 map_，要么全部拒绝。
     * 逐行 merge 会在部分行失败后仍留下脏行，导致 apply BuildApplyTxn 触发 invariant。
     */
    std::vector<const E_WireRow*> writable_rows;
    writable_rows.reserve(txn.rows.size());
    for (const auto& row : txn.rows) {
        if (row.op_type != EPOCH_OP_READ) {
            writable_rows.push_back(&row);
        }
    }
    if (writable_rows.empty()) {
        return outcome;
    }

    for (const E_WireRow* row : writable_rows) {
        bool accepted = true;
        (void)PreviewMergeRowInternal(*row, txn.csn, txn.server_id, &accepted);
        if (!accepted) {
            outcome.txn_accepted = false;
            return outcome;
        }
    }

    for (const E_WireRow* row : writable_rows) {
        (void)MergeRowInternal(*row, txn.csn, txn.server_id, nullptr, &outcome.aborted_txns);
    }

    return outcome;
}

bool E_CrdtMerger::BuildMergedTxn(uint64_t epoch, uint64_t base_csn, uint32_t server_id, E_WireTxn* out) const
{
    if (out == nullptr) {
        return false;
    }
    out->csn = base_csn;
    out->epoch = epoch;
    out->server_id = server_id;
    out->txn_state = EPOCH_TXN_COMMIT;
    out->rows.clear();
    uint32_t local_node = EpochGetNodeId();
    uint64_t max_csn = base_csn;
    for (const auto& kv : map_) {
        /* 本节点行由 worker BuildLocalApplyTxn 落盘；多主 backend 不在 RECORD 写 heap */
        if (kv.second.server_id == local_node) {
            continue;
        }
        out->rows.push_back(kv.second.row);
        if (kv.second.csn > max_csn) {
            max_csn = kv.second.csn;
        }
    }
    out->csn = max_csn;
    return !out->rows.empty();
}

bool E_CrdtMerger::WireTxnWriteSetWonMerge(const E_WireTxn& source, E_WireTxn* out) const
{
    if (out == nullptr) {
        return false;
    }
    out->csn = source.csn;
    out->epoch = source.epoch;
    out->server_id = source.server_id;
    out->txn_state = EPOCH_TXN_COMMIT;
    out->rows.clear();

    for (const auto& row : source.rows) {
        if (row.op_type == EPOCH_OP_READ) {
            continue;
        }
        Key key;
        Oid localRelid = E_ResolveLocalRelid(row);
        key.relid = static_cast<uint32_t>(localRelid);
        if (!E_BuildPkKeyFromWireRow(row.relid, row, &key.pk_key)) {
            return false;
        }
        auto it = map_.find(key);
        if (it == map_.end()) {
            return false;
        }
        if (it->second.csn != source.csn || it->second.server_id != source.server_id) {
            return false;
        }
        out->rows.push_back(it->second.row);
    }
    return !out->rows.empty();
}

bool E_CrdtMerger::BuildLocalApplyTxn(uint64_t epoch, uint32_t local_node, E_WireTxn* out) const
{
    if (out == nullptr) {
        return false;
    }
    out->csn = 0;
    out->epoch = epoch;
    out->server_id = local_node;
    out->txn_state = EPOCH_TXN_COMMIT;
    out->rows.clear();
    uint64_t max_csn = 0;
    for (const auto& kv : map_) {
        if (kv.second.server_id != local_node) {
            continue;
        }
        out->rows.push_back(kv.second.row);
        if (kv.second.csn > max_csn) {
            max_csn = kv.second.csn;
        }
    }
    out->csn = max_csn;
    return !out->rows.empty();
}

bool E_CrdtMerger::HasRemoteRows(uint32_t local_node) const
{
    for (const auto& kv : map_) {
        if (kv.second.server_id != local_node) {
            return true;
        }
    }
    return false;
}

bool E_CrdtMerger::HasLocalRows(uint32_t local_node) const
{
    for (const auto& kv : map_) {
        if (kv.second.server_id == local_node) {
            return true;
        }
    }
    return false;
}

bool E_CrdtMerger::IsEmpty(void) const
{
    return map_.empty();
}
