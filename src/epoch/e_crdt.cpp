#include "postgres.h"

#include "epoch/e_crdt.h"
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
    /*
     * 同一 txn 重放（TXN_REQ、乱序重传）时 csn/sid 相同，须视为幂等胜出，
     * 否则 RowWins 在相等时返回 false → txn_accepted=false → 误记 abort_csn。
     */
    if (csn_a == csn_b && sid_a == sid_b) {
        return true;
    }
    if (csn_a > csn_b) {
        return true;
    }
    if (csn_a < csn_b) {
        return false;
    }
    return sid_a > sid_b;
}

void E_CrdtMerger::Clear()
{
    map_.clear();
    insert_winners_.clear();
}

bool E_CrdtMerger::MergeRowInternal(const E_WireRow& row, uint64_t csn, uint32_t server_id, bool* accepted,
    std::vector<std::pair<uint64_t, uint32_t>>* aborted_txns)
{
    if (accepted != nullptr) {
        *accepted = true;
    }

    Key key;
    key.relid = row.relid;
    if (!E_BuildPkKeyFromWireRow(row.relid, row, &key.pk_key)) {
        static std::unordered_map<uint32_t, int> g_pk_fail_tick;
        int& tick = g_pk_fail_tick[row.relid];
        if (++tick % 20 == 1) {
            Oid localRelid = E_ResolveLocalRelid(row);
            EpochRecvDiagLogMergePkFail(row.relid,
                static_cast<uint32_t>(localRelid),
                row.relname.empty() ? nullptr : row.relname.c_str(),
                row.op_type);
        }
        if (accepted != nullptr) {
            *accepted = false;
        }
        return false;
    }

    if (row.op_type == EPOCH_OP_INSERT) {
        auto ins_it = insert_winners_.find(key.pk_key);
        if (ins_it == insert_winners_.end()) {
            insert_winners_[key.pk_key] = InsertWinner{csn, server_id};
        } else {
            InsertWinner& winner = ins_it->second;
            if (winner.csn == csn && winner.server_id == server_id) {
                return true;
            }
            if (RowWins(csn, server_id, winner.csn, winner.server_id)) {
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
    if (txn.rows.empty()) {
        return outcome;
    }

    for (const auto& row : txn.rows) {
        if (row.op_type == EPOCH_OP_READ) {
            continue;
        }
        bool accepted = true;
        (void)MergeRowInternal(row, txn.csn, txn.server_id, &accepted, &outcome.aborted_txns);
        if (!accepted) {
            outcome.txn_accepted = false;
        }
    }

    return outcome;
}

bool E_CrdtMerger::BuildMergedTxn(uint64_t epoch, uint64_t base_csn, uint32_t server_id, E_WireTxn* out) const
{
    if (out == nullptr) {
        return false;
    }
    out->csn = base_csn;
    out->start_epoch = epoch;
    out->commit_epoch = epoch;
    out->server_id = server_id;
    out->txn_state = EPOCH_TXN_COMMIT;
    out->rows.clear();
    uint32_t local_node = EpochGetNodeId();
    uint64_t max_csn = base_csn;
    for (const auto& kv : map_) {
        /* 本节点已在 O_RecordCommit 落盘，仅 apply 对端获胜的行 */
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

bool E_CrdtMerger::HasRemoteRows(uint32_t local_node) const
{
    for (const auto& kv : map_) {
        if (kv.second.server_id != local_node) {
            return true;
        }
    }
    return false;
}

bool E_CrdtMerger::IsEmpty(void) const
{
    return map_.empty();
}
