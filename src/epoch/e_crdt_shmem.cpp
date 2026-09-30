#include "postgres.h"

#include "epoch/e_crdt_shmem.h"
#include "epoch/e_config.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_crdt_merge_logic.h"
#include "epoch/e_row_key.h"
#include "epoch/e_merge_profile.h"

#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/spin.h"

#include "portability/instr_time.h"

#include <algorithm>
#include <cstring>
#include <functional>
#include <vector>

struct EpochCrdtRowEntry {
    uint8_t used;
    uint8_t op_type;
    uint16_t pk_len;
    uint16_t relname_len;
    uint16_t tuple_len;
    uint32_t relid;
    uint16_t bi_hi;
    uint16_t bi_lo;
    uint16_t ip_posid;
    char pk[EPOCH_CRDT_MAX_PK];
    char relname[EPOCH_CRDT_MAX_RELNAME];
    char tuple_data[EPOCH_CRDT_MAX_TUPLE];
    uint64_t cen;
    uint64_t csn;
    uint32_t server_id;
};

struct EpochCrdtInsertEntry {
    uint8_t used;
    uint16_t pk_len;
    uint32_t relid;
    char pk[EPOCH_CRDT_MAX_PK];
    uint64_t cen;
    uint64_t csn;
    uint32_t server_id;
};

struct EpochCrdtShardHeader {
    slock_t lock;
    uint32_t row_count;
    uint32_t insert_count;
    uint64_t bound_epoch;
};

struct EpochCrdtSlotHeader {
    slock_t meta_lock;
    uint64_t bound_epoch;
};

struct EpochCrdtShmemHeader {
    uint32_t slot_count;
    uint32_t rows_per_slot;
    uint32_t inserts_per_slot;
    uint32_t num_shards;
    uint32_t rows_per_shard;
    uint32_t inserts_per_shard;
};

struct EpochCrdtShmem {
    EpochCrdtShmemHeader header;
    char blob[1];
};

static constexpr const char* EPOCH_CRDT_SHMEM_NAME = "Epoch CRDT Store";

EpochCrdtShmem* g_epochCrdtShmem = nullptr;

static uint32_t EpochCrdtComputeRowsPerShard(uint32_t rows_per_slot)
{
    return (rows_per_slot + EPOCH_CRDT_NUM_SHARDS - 1U) / EPOCH_CRDT_NUM_SHARDS + EPOCH_CRDT_ROW_SKEW_SLACK;
}

static uint32_t EpochCrdtComputeInsertsPerShard(uint32_t inserts_per_slot)
{
    return (inserts_per_slot + EPOCH_CRDT_NUM_SHARDS - 1U) / EPOCH_CRDT_NUM_SHARDS + EPOCH_CRDT_INSERT_SKEW_SLACK;
}

static uint32_t EpochCrdtShardRowsCap(void)
{
    if (g_epochCrdtShmem == nullptr) {
        return EpochCrdtComputeRowsPerShard(EpochGetCrdtRowsPerSlot());
    }
    return g_epochCrdtShmem->header.rows_per_shard;
}

static uint32_t EpochCrdtShardInsertsCap(void)
{
    if (g_epochCrdtShmem == nullptr) {
        return EpochCrdtComputeInsertsPerShard(EpochGetCrdtInsertsPerSlot());
    }
    return g_epochCrdtShmem->header.inserts_per_shard;
}

static uint32_t EpochCrdtSlotCount(void)
{
    if (g_epochCrdtShmem == nullptr) {
        return EpochGetCrdtSlotCount();
    }
    return g_epochCrdtShmem->header.slot_count;
}

static uint32_t EpochCrdtNumShardsRuntime(void)
{
    if (g_epochCrdtShmem == nullptr) {
        return EPOCH_CRDT_NUM_SHARDS;
    }
    return g_epochCrdtShmem->header.num_shards;
}

static size_t EpochCrdtShardStride(uint32_t rows_per_shard, uint32_t inserts_per_shard)
{
    return sizeof(EpochCrdtShardHeader) + static_cast<size_t>(rows_per_shard) * sizeof(EpochCrdtRowEntry) +
           static_cast<size_t>(inserts_per_shard) * sizeof(EpochCrdtInsertEntry);
}

static size_t EpochCrdtSlotStride(uint32_t num_shards, uint32_t rows_per_shard, uint32_t inserts_per_shard)
{
    return sizeof(EpochCrdtSlotHeader) + static_cast<size_t>(num_shards) *
                                              EpochCrdtShardStride(rows_per_shard, inserts_per_shard);
}

static EpochCrdtSlotHeader* EpochCrdtSlotHeaderAt(uint32_t slot_idx)
{
    if (g_epochCrdtShmem == nullptr) {
        return nullptr;
    }
    uint32_t num_shards = EpochCrdtNumShardsRuntime();
    uint32_t rows_per_shard = EpochCrdtShardRowsCap();
    uint32_t inserts_per_shard = EpochCrdtShardInsertsCap();
    char* base = g_epochCrdtShmem->blob;
    return reinterpret_cast<EpochCrdtSlotHeader*>(base + static_cast<size_t>(slot_idx) *
                                                               EpochCrdtSlotStride(num_shards, rows_per_shard,
                                                                   inserts_per_shard));
}

static EpochCrdtShardHeader* EpochCrdtShardAt(EpochCrdtSlotHeader* slot, uint32_t shard_idx)
{
    uint32_t rows_per_shard = EpochCrdtShardRowsCap();
    uint32_t inserts_per_shard = EpochCrdtShardInsertsCap();
    char* base = reinterpret_cast<char*>(slot + 1);
    return reinterpret_cast<EpochCrdtShardHeader*>(base + static_cast<size_t>(shard_idx) *
                                                                EpochCrdtShardStride(rows_per_shard, inserts_per_shard));
}

static EpochCrdtRowEntry* EpochCrdtShardRows(EpochCrdtShardHeader* shard_hdr)
{
    return reinterpret_cast<EpochCrdtRowEntry*>(shard_hdr + 1);
}

static EpochCrdtInsertEntry* EpochCrdtShardInserts(EpochCrdtShardHeader* shard_hdr)
{
    return reinterpret_cast<EpochCrdtInsertEntry*>(EpochCrdtShardRows(shard_hdr) + EpochCrdtShardRowsCap());
}

static EpochCrdtSlotHeader* EpochCrdtGetSlot(uint64_t epoch)
{
    if (g_epochCrdtShmem == nullptr || epoch == 0) {
        return nullptr;
    }
    uint32_t slot_count = EpochCrdtSlotCount();
    if (slot_count == 0) {
        return nullptr;
    }
    return EpochCrdtSlotHeaderAt(static_cast<uint32_t>(epoch % slot_count));
}

struct EpochCrdtPreparedRow {
    const E_WireRow* row;
    uint32_t local_relid;
    char pk[EPOCH_CRDT_MAX_PK];
    uint16_t pk_len;
};

static uint32_t EpochCrdtShardIndex(uint32_t relid, const char* pk, uint16_t pk_len)
{
    size_t hash = std::hash<uint32_t>{}(relid);
    for (uint16_t i = 0; i < pk_len; ++i) {
        hash = hash * 31U + static_cast<unsigned char>(pk[i]);
    }
    return static_cast<uint32_t>(hash % EpochCrdtNumShardsRuntime());
}

static EpochCrdtShardHeader* EpochCrdtShardForPk(EpochCrdtSlotHeader* slot, uint32_t relid, const char* pk,
    uint16_t pk_len, uint32_t* shard_idx_out)
{
    uint32_t shard_idx = EpochCrdtShardIndex(relid, pk, pk_len);
    if (shard_idx_out != nullptr) {
        *shard_idx_out = shard_idx;
    }
    return EpochCrdtShardAt(slot, shard_idx);
}

static void EpochCrdtResetShardLocked(EpochCrdtShardHeader* shard);

static void EpochCrdtEnsureShardEpochLocked(EpochCrdtShardHeader* shard, uint64_t slot_epoch)
{
    if (shard->bound_epoch == slot_epoch) {
        return;
    }
    EpochCrdtResetShardLocked(shard);
    shard->bound_epoch = slot_epoch;
}

static void EpochCrdtAcquireShards(EpochCrdtSlotHeader* slot, uint64_t slot_epoch,
    const std::vector<uint32_t>& shard_indices)
{
    for (uint32_t shard_idx : shard_indices) {
        EpochCrdtShardHeader* shard = EpochCrdtShardAt(slot, shard_idx);
        SpinLockAcquire(&shard->lock);
        EpochCrdtEnsureShardEpochLocked(shard, slot_epoch);
    }
}

static void EpochCrdtReleaseShards(EpochCrdtSlotHeader* slot, const std::vector<uint32_t>& shard_indices)
{
    for (auto it = shard_indices.rbegin(); it != shard_indices.rend(); ++it) {
        EpochCrdtShardHeader* shard = EpochCrdtShardAt(slot, *it);
        SpinLockRelease(&shard->lock);
    }
}

static void EpochCrdtAcquireAllShards(EpochCrdtSlotHeader* slot)
{
    uint32_t num_shards = EpochCrdtNumShardsRuntime();
    for (uint32_t i = 0; i < num_shards; ++i) {
        SpinLockAcquire(&EpochCrdtShardAt(slot, i)->lock);
    }
}

static void EpochCrdtReleaseAllShards(EpochCrdtSlotHeader* slot)
{
    uint32_t num_shards = EpochCrdtNumShardsRuntime();
    for (uint32_t i = num_shards; i > 0; --i) {
        SpinLockRelease(&EpochCrdtShardAt(slot, i - 1)->lock);
    }
}

static void EpochCrdtCollectShardIndices(const std::vector<EpochCrdtPreparedRow>& prepared_rows,
    std::vector<uint32_t>* shard_indices)
{
    if (shard_indices == nullptr) {
        return;
    }
    shard_indices->clear();
    shard_indices->reserve(prepared_rows.size());
    for (const EpochCrdtPreparedRow& prep : prepared_rows) {
        shard_indices->push_back(EpochCrdtShardIndex(prep.local_relid, prep.pk, prep.pk_len));
    }
    std::sort(shard_indices->begin(), shard_indices->end());
    shard_indices->erase(std::unique(shard_indices->begin(), shard_indices->end()), shard_indices->end());
}

static void EpochCrdtResetShardLocked(EpochCrdtShardHeader* shard)
{
    /*
     * O(1) 惰性 reset：只重置计数与 bound_epoch，不清槽。
     * 查找/写入均受 row_count/insert_count 上界约束，新写入从 slot 0 覆写旧数据。
     */
    shard->row_count = 0;
    shard->insert_count = 0;
    shard->bound_epoch = 0;
}

static void EpochCrdtResetSlotLocked(EpochCrdtSlotHeader* slot)
{
    slot->bound_epoch = 0;
    uint32_t num_shards = EpochCrdtNumShardsRuntime();
    for (uint32_t i = 0; i < num_shards; ++i) {
        EpochCrdtResetShardLocked(EpochCrdtShardAt(slot, i));
    }
}

static void EpochCrdtEnsureSlotEpoch(EpochCrdtSlotHeader* slot, uint64_t epoch)
{
    SpinLockAcquire(&slot->meta_lock);
    if (slot->bound_epoch != epoch) {
        slot->bound_epoch = epoch;
    }
    SpinLockRelease(&slot->meta_lock);
}

static bool EpochCrdtBuildPk(const E_WireRow& row, char* pk, uint16_t* pk_len)
{
    if (pk == nullptr || pk_len == nullptr) {
        return false;
    }
    return E_BuildPkKeyFromWireRow(row.relid, row, pk, EPOCH_CRDT_MAX_PK, pk_len);
}

static bool EpochCrdtPrepareRow(const E_WireRow& row, EpochCrdtPreparedRow* prep)
{
    if (prep == nullptr) {
        return false;
    }
    prep->row = &row;
    prep->local_relid = 0;
    prep->pk_len = 0;

    instr_time prep_start;
    INSTR_TIME_SET_CURRENT(prep_start);
    Oid localRelid = E_ResolveLocalRelid(row);
    if (!OidIsValid(localRelid)) {
        return false;
    }
    if (row.relname.size() >= EPOCH_CRDT_MAX_RELNAME || row.tuple_data.size() > EPOCH_CRDT_MAX_TUPLE) {
        return false;
    }
    prep->local_relid = static_cast<uint32_t>(localRelid);
    instr_time prep_end;
    INSTR_TIME_SET_CURRENT(prep_end);
    INSTR_TIME_SUBTRACT(prep_end, prep_start);
    EpochMergeProfileAddUs(EMP_PREPARE_ROWS, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(prep_end)));

    if (E_WireRowHasPackedPk(row)) {
        prep->pk_len = row.pk_len;
        errno_t rc = memcpy_s(prep->pk, sizeof(prep->pk), row.pk.data(), row.pk_len);
        securec_check(rc, "\0", "\0");
        return true;
    }
    if (!EpochCrdtBuildPk(row, prep->pk, &prep->pk_len)) {
        return false;
    }
    return true;
}

static bool EpochCrdtPkEqual(uint32_t relid, const char* pk_a, uint16_t pk_len_a, const EpochCrdtInsertEntry& b)
{
    return b.used != 0 && b.relid == relid && b.pk_len == pk_len_a &&
           memcmp(b.pk, pk_a, pk_len_a) == 0;
}

static bool EpochCrdtPkEqualRow(uint32_t relid, const char* pk, uint16_t pk_len, const EpochCrdtRowEntry& row)
{
    return row.used != 0 && row.relid == relid && row.pk_len == pk_len && memcmp(row.pk, pk, pk_len) == 0;
}

static EpochCrdtInsertEntry* EpochCrdtFindInsert(EpochCrdtShardHeader* shard, uint32_t relid, const char* pk,
    uint16_t pk_len)
{
    EpochCrdtInsertEntry* inserts = EpochCrdtShardInserts(shard);
    for (uint32_t i = 0; i < shard->insert_count; ++i) {
        if (EpochCrdtPkEqual(relid, pk, pk_len, inserts[i])) {
            return &inserts[i];
        }
    }
    return nullptr;
}

static EpochCrdtRowEntry* EpochCrdtFindRow(EpochCrdtShardHeader* shard, uint32_t relid, const char* pk,
    uint16_t pk_len)
{
    EpochCrdtRowEntry* rows = EpochCrdtShardRows(shard);
    for (uint32_t i = 0; i < shard->row_count; ++i) {
        if (EpochCrdtPkEqualRow(relid, pk, pk_len, rows[i])) {
            return &rows[i];
        }
    }
    return nullptr;
}

static bool EpochCrdtStoreRowLocked(EpochCrdtShardHeader* shard, const EpochCrdtPreparedRow& prep, uint64_t cen,
    uint64_t csn, uint32_t server_id, EpochCrdtRowEntry* existing_row, E_CrdtMergeOutcome* outcome)
{
    const E_WireRow& row = *prep.row;
    errno_t rc = EOK;
    EpochCrdtRowEntry* entry = existing_row;
    if (entry == nullptr) {
        entry = EpochCrdtFindRow(shard, prep.local_relid, prep.pk, prep.pk_len);
    }
    if (entry == nullptr) {
        if (shard->row_count >= EpochCrdtShardRowsCap()) {
            ereport(WARNING,
                (errmsg("epoch crdt: shard row slot full epoch=%lu relid=%u", cen, prep.local_relid)));
            if (outcome != nullptr) {
                outcome->resource_failure = true;
            }
            return false;
        }
        EpochCrdtRowEntry* rows = EpochCrdtShardRows(shard);
        entry = &rows[shard->row_count++];
        entry->used = 1;
        entry->pk_len = prep.pk_len;
        rc = memcpy_s(entry->pk, sizeof(entry->pk), prep.pk, prep.pk_len);
        securec_check(rc, "\0", "\0");
    }

    entry->op_type = static_cast<uint8_t>(row.op_type);
    entry->relid = prep.local_relid;
    entry->bi_hi = row.bi_hi;
    entry->bi_lo = row.bi_lo;
    entry->ip_posid = row.ip_posid;
    entry->relname_len = static_cast<uint16_t>(row.relname.size());
    entry->tuple_len = static_cast<uint16_t>(row.tuple_data.size());
    entry->cen = cen;
    entry->csn = csn;
    entry->server_id = server_id;
    if (entry->relname_len > 0) {
        rc = memcpy_s(entry->relname, sizeof(entry->relname), row.relname.data(), entry->relname_len);
        securec_check(rc, "\0", "\0");
    }
    if (entry->tuple_len > 0) {
        rc = memcpy_s(entry->tuple_data, sizeof(entry->tuple_data), row.tuple_data.data(), entry->tuple_len);
        securec_check(rc, "\0", "\0");
    }
    return true;
}

static void EpochCrdtUnpackRow(const EpochCrdtRowEntry& entry, E_WireRow* row)
{
    row->op_type = static_cast<EpochOpType>(entry.op_type);
    row->relid = entry.relid;
    row->bi_hi = entry.bi_hi;
    row->bi_lo = entry.bi_lo;
    row->ip_posid = entry.ip_posid;
    row->relname.assign(entry.relname, entry.relname_len);
    row->tuple_data.assign(entry.tuple_data, entry.tuple_data + entry.tuple_len);
    row->expected_csn = entry.csn;
    /* 保留 packed PK，供 apply 亲和按 w_id 分片（勿丢成仅 tuple） */
    row->pk_len = entry.pk_len;
    if (entry.pk_len > 0) {
        row->pk.assign(entry.pk, entry.pk_len);
    } else {
        row->pk.clear();
    }
}

static bool EpochCrdtSameTxn(uint64_t a_csn, uint32_t a_sid, uint64_t b_csn, uint32_t b_sid)
{
    return a_csn == b_csn && a_sid == b_sid;
}

static bool EpochCrdtPreviewPreWrittenConflict(const EpochCrdtRowEntry* existing, uint64_t t_csn,
    uint32_t server_id, bool* accepted)
{
    if (EpochCrdtSameTxn(t_csn, server_id, existing->csn, existing->server_id)) {
        return true;
    }
    if (EpochCrdtIncomingTxnWins(existing->csn, existing->server_id, t_csn, server_id)) {
        return true;
    }
    if (accepted != nullptr) {
        *accepted = false;
    }
    return false;
}

static bool EpochCrdtPreviewRowLocked(EpochCrdtShardHeader* shard, const EpochCrdtPreparedRow& prep,
    uint64_t txn_epoch, uint64_t t_csn, uint32_t server_id, bool* accepted)
{
    if (accepted != nullptr) {
        *accepted = true;
    }

    const E_WireRow& row = *prep.row;
    if (row.op_type == EPOCH_OP_INSERT) {
        EpochCrdtInsertEntry* ins = EpochCrdtFindInsert(shard, prep.local_relid, prep.pk, prep.pk_len);
        if (ins != nullptr) {
            if (EpochCrdtSameTxn(t_csn, server_id, ins->csn, ins->server_id)) {
                return true;
            }
            if (!EpochCrdtIncomingTxnWins(ins->csn, ins->server_id, t_csn, server_id)) {
                if (accepted != nullptr) {
                    *accepted = false;
                }
                return false;
            }
        }
    }

    EpochCrdtRowEntry* existing = EpochCrdtFindRow(shard, prep.local_relid, prep.pk, prep.pk_len);
    if (existing == nullptr || existing->cen != txn_epoch) {
        return true;
    }

    return EpochCrdtPreviewPreWrittenConflict(existing, t_csn, server_id, accepted);
}

static bool EpochCrdtMergeRowLocked(EpochCrdtShardHeader* shard, const EpochCrdtPreparedRow& prep, uint64_t txn_epoch,
    uint64_t t_csn, uint32_t server_id, bool* accepted, E_CrdtMergeOutcome* outcome)
{
    if (accepted != nullptr) {
        *accepted = true;
    }

    const E_WireRow& row = *prep.row;
    if (row.op_type == EPOCH_OP_INSERT) {
        EpochCrdtInsertEntry* ins = EpochCrdtFindInsert(shard, prep.local_relid, prep.pk, prep.pk_len);
        if (ins == nullptr) {
            if (shard->insert_count >= EpochCrdtShardInsertsCap()) {
                ereport(WARNING,
                    (errmsg("epoch crdt: shard insert slot full epoch=%lu relid=%u", txn_epoch, prep.local_relid)));
                if (outcome != nullptr) {
                    outcome->resource_failure = true;
                }
                if (accepted != nullptr) {
                    *accepted = false;
                }
                return false;
            }
            EpochCrdtInsertEntry* inserts = EpochCrdtShardInserts(shard);
            ins = &inserts[shard->insert_count++];
            ins->used = 1;
            ins->relid = prep.local_relid;
            ins->pk_len = prep.pk_len;
            errno_t rc = memcpy_s(ins->pk, sizeof(ins->pk), prep.pk, prep.pk_len);
            securec_check(rc, "\0", "\0");
            ins->cen = txn_epoch;
            ins->csn = t_csn;
            ins->server_id = server_id;
        } else if (EpochCrdtSameTxn(t_csn, server_id, ins->csn, ins->server_id)) {
            /* replay */
        } else if (EpochCrdtIncomingTxnWins(ins->csn, ins->server_id, t_csn, server_id)) {
            if (outcome != nullptr) {
                outcome->aborted_txns.emplace_back(ins->csn, ins->server_id);
            }
            ins->cen = txn_epoch;
            ins->csn = t_csn;
            ins->server_id = server_id;
        } else {
            if (accepted != nullptr) {
                *accepted = false;
            }
            return false;
        }
    }

    EpochCrdtRowEntry* existing = EpochCrdtFindRow(shard, prep.local_relid, prep.pk, prep.pk_len);
    if (existing == nullptr || existing->cen != txn_epoch) {
        return EpochCrdtStoreRowLocked(shard, prep, txn_epoch, t_csn, server_id, existing, outcome);
    }

    if (EpochCrdtSameTxn(t_csn, server_id, existing->csn, existing->server_id)) {
        return true;
    }
    if (EpochCrdtIncomingTxnWins(existing->csn, existing->server_id, t_csn, server_id)) {
        if (outcome != nullptr) {
            outcome->aborted_txns.emplace_back(existing->csn, existing->server_id);
        }
        return EpochCrdtStoreRowLocked(shard, prep, txn_epoch, t_csn, server_id, existing, outcome);
    }
    if (accepted != nullptr) {
        *accepted = false;
    }
    return false;
}

Size EpochCrdtShmemSize(void)
{
    uint32_t slot_count = EpochGetCrdtSlotCount();
    uint32_t rows_per_slot = EpochGetCrdtRowsPerSlot();
    uint32_t inserts_per_slot = EpochGetCrdtInsertsPerSlot();
    uint32_t num_shards = EPOCH_CRDT_NUM_SHARDS;
    uint32_t rows_per_shard = EpochCrdtComputeRowsPerShard(rows_per_slot);
    uint32_t inserts_per_shard = EpochCrdtComputeInsertsPerShard(inserts_per_slot);
    return offsetof(EpochCrdtShmem, blob) +
           static_cast<size_t>(slot_count) * EpochCrdtSlotStride(num_shards, rows_per_shard, inserts_per_shard);
}

static void EpochCrdtShmemInitAllSlots(bool reset_data)
{
    uint32_t slot_count = EpochCrdtSlotCount();
    for (uint32_t i = 0; i < slot_count; ++i) {
        EpochCrdtSlotHeader* slot = EpochCrdtSlotHeaderAt(i);
        SpinLockInit(&slot->meta_lock);
        slot->bound_epoch = 0;
        uint32_t num_shards = EpochCrdtNumShardsRuntime();
        for (uint32_t s = 0; s < num_shards; ++s) {
            EpochCrdtShardHeader* shard = EpochCrdtShardAt(slot, s);
            SpinLockInit(&shard->lock);
            if (reset_data) {
                EpochCrdtResetShardLocked(shard);
            }
        }
    }
}

void EpochCrdtShmemInit(void)
{
    bool found = false;
    g_epochCrdtShmem = static_cast<EpochCrdtShmem*>(
        ShmemInitStruct(EPOCH_CRDT_SHMEM_NAME, EpochCrdtShmemSize(), &found));
    if (g_epochCrdtShmem == nullptr) {
        return;
    }
    if (!found) {
        uint32_t rows_per_slot = EpochGetCrdtRowsPerSlot();
        uint32_t inserts_per_slot = EpochGetCrdtInsertsPerSlot();
        g_epochCrdtShmem->header.slot_count = EpochGetCrdtSlotCount();
        g_epochCrdtShmem->header.rows_per_slot = rows_per_slot;
        g_epochCrdtShmem->header.inserts_per_slot = inserts_per_slot;
        g_epochCrdtShmem->header.num_shards = EPOCH_CRDT_NUM_SHARDS;
        g_epochCrdtShmem->header.rows_per_shard = EpochCrdtComputeRowsPerShard(rows_per_slot);
        g_epochCrdtShmem->header.inserts_per_shard = EpochCrdtComputeInsertsPerShard(inserts_per_slot);
        EpochCrdtShmemInitAllSlots(true);
    }
}

void EpochCrdtShmemClearEpoch(uint64_t epoch)
{
    if (g_epochCrdtShmem == nullptr) {
        return;
    }
    EpochCrdtSlotHeader* slot = EpochCrdtGetSlot(epoch);
    if (slot == nullptr) {
        return;
    }
    EpochCrdtAcquireAllShards(slot);
    SpinLockAcquire(&slot->meta_lock);
    if (slot->bound_epoch == epoch) {
        EpochCrdtResetSlotLocked(slot);
    }
    SpinLockRelease(&slot->meta_lock);
    EpochCrdtReleaseAllShards(slot);
}

E_CrdtMergeOutcome EpochCrdtShmemMergeWireTxn(const E_WireTxn& txn)
{
    E_CrdtMergeOutcome outcome;
    outcome.txn_accepted = true;
    outcome.resource_failure = false;
    if (txn.rows.empty() || g_epochCrdtShmem == nullptr) {
        if (txn.rows.empty()) {
            return outcome;
        }
        outcome.txn_accepted = false;
        return outcome;
    }

    EpochCrdtSlotHeader* slot = EpochCrdtGetSlot(txn.epoch);
    if (slot == nullptr) {
        outcome.txn_accepted = false;
        outcome.resource_failure = true;
        return outcome;
    }

    std::vector<EpochCrdtPreparedRow> prepared_rows;
    prepared_rows.reserve(txn.rows.size());
    for (const auto& row : txn.rows) {
        if (row.op_type == EPOCH_OP_READ) {
            continue;
        }
        EpochCrdtPreparedRow prep;
        if (!EpochCrdtPrepareRow(row, &prep)) {
            outcome.txn_accepted = false;
            outcome.resource_failure = true;
            return outcome;
        }
        prepared_rows.push_back(prep);
    }
    EpochMergeProfileSetRowCount(prepared_rows.size());

    if (prepared_rows.empty()) {
        return outcome;
    }

    instr_time ensure_start;
    INSTR_TIME_SET_CURRENT(ensure_start);
    EpochCrdtEnsureSlotEpoch(slot, txn.epoch);
    instr_time ensure_end;
    INSTR_TIME_SET_CURRENT(ensure_end);
    INSTR_TIME_SUBTRACT(ensure_end, ensure_start);
    EpochMergeProfileAddUs(EMP_ENSURE_SLOT, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(ensure_end)));

    std::vector<uint32_t> shard_indices;
    instr_time collect_start;
    INSTR_TIME_SET_CURRENT(collect_start);
    EpochCrdtCollectShardIndices(prepared_rows, &shard_indices);
    instr_time collect_end;
    INSTR_TIME_SET_CURRENT(collect_end);
    INSTR_TIME_SUBTRACT(collect_end, collect_start);
    EpochMergeProfileAddUs(EMP_COLLECT_SHARDS, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(collect_end)));
    EpochMergeProfileSetShardCount(shard_indices.size());

    instr_time acquire_start;
    INSTR_TIME_SET_CURRENT(acquire_start);
    EpochCrdtAcquireShards(slot, txn.epoch, shard_indices);
    instr_time acquire_end;
    INSTR_TIME_SET_CURRENT(acquire_end);
    INSTR_TIME_SUBTRACT(acquire_end, acquire_start);
    EpochMergeProfileAddUs(EMP_SHARD_ACQUIRE, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(acquire_end)));

    instr_time preview_start;
    INSTR_TIME_SET_CURRENT(preview_start);
    for (const EpochCrdtPreparedRow& prep : prepared_rows) {
        EpochCrdtShardHeader* shard = EpochCrdtShardForPk(slot, prep.local_relid, prep.pk, prep.pk_len, nullptr);
        bool accepted = true;
        if (!EpochCrdtPreviewRowLocked(shard, prep, txn.epoch, txn.csn, txn.server_id, &accepted) || !accepted) {
            outcome.txn_accepted = false;
            instr_time preview_end;
            INSTR_TIME_SET_CURRENT(preview_end);
            INSTR_TIME_SUBTRACT(preview_end, preview_start);
            EpochMergeProfileAddUs(EMP_PREVIEW, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(preview_end)));
            instr_time release_start;
            INSTR_TIME_SET_CURRENT(release_start);
            EpochCrdtReleaseShards(slot, shard_indices);
            instr_time release_end;
            INSTR_TIME_SET_CURRENT(release_end);
            INSTR_TIME_SUBTRACT(release_end, release_start);
            EpochMergeProfileAddUs(EMP_SHARD_RELEASE, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(release_end)));
            return outcome;
        }
    }
    instr_time preview_end;
    INSTR_TIME_SET_CURRENT(preview_end);
    INSTR_TIME_SUBTRACT(preview_end, preview_start);
    EpochMergeProfileAddUs(EMP_PREVIEW, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(preview_end)));

    instr_time merge_start;
    INSTR_TIME_SET_CURRENT(merge_start);
    for (const EpochCrdtPreparedRow& prep : prepared_rows) {
        EpochCrdtShardHeader* shard = EpochCrdtShardForPk(slot, prep.local_relid, prep.pk, prep.pk_len, nullptr);
        bool accepted = true;
        if (!EpochCrdtMergeRowLocked(shard, prep, txn.epoch, txn.csn, txn.server_id, &accepted, &outcome) ||
            !accepted) {
            outcome.txn_accepted = false;
            instr_time merge_end;
            INSTR_TIME_SET_CURRENT(merge_end);
            INSTR_TIME_SUBTRACT(merge_end, merge_start);
            EpochMergeProfileAddUs(EMP_MERGE_ROWS, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(merge_end)));
            instr_time release_start;
            INSTR_TIME_SET_CURRENT(release_start);
            EpochCrdtReleaseShards(slot, shard_indices);
            instr_time release_end;
            INSTR_TIME_SET_CURRENT(release_end);
            INSTR_TIME_SUBTRACT(release_end, release_start);
            EpochMergeProfileAddUs(EMP_SHARD_RELEASE, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(release_end)));
            return outcome;
        }
    }
    instr_time merge_end;
    INSTR_TIME_SET_CURRENT(merge_end);
    INSTR_TIME_SUBTRACT(merge_end, merge_start);
    EpochMergeProfileAddUs(EMP_MERGE_ROWS, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(merge_end)));

    instr_time release_start;
    INSTR_TIME_SET_CURRENT(release_start);
    EpochCrdtReleaseShards(slot, shard_indices);
    instr_time release_end;
    INSTR_TIME_SET_CURRENT(release_end);
    INSTR_TIME_SUBTRACT(release_end, release_start);
    EpochMergeProfileAddUs(EMP_SHARD_RELEASE, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(release_end)));
    return outcome;
}

static uint32_t EpochCrdtCountWritableRows(const E_WireTxn& txn)
{
    uint32_t count = 0;
    for (const auto& row : txn.rows) {
        if (row.op_type != EPOCH_OP_READ) {
            count++;
        }
    }
    return count;
}

static void EpochCrdtCollectTxnMergedRows(EpochCrdtSlotHeader* slot, uint64_t epoch, uint64_t csn, uint32_t server_id,
    std::vector<const EpochCrdtRowEntry*>* entries)
{
    if (slot == nullptr || entries == nullptr) {
        return;
    }
    entries->clear();
    uint32_t num_shards = EpochCrdtNumShardsRuntime();
    for (uint32_t i = 0; i < num_shards; ++i) {
        EpochCrdtShardHeader* shard = EpochCrdtShardAt(slot, i);
        SpinLockAcquire(&shard->lock);
        EpochCrdtEnsureShardEpochLocked(shard, epoch);
        EpochCrdtRowEntry* rows = EpochCrdtShardRows(shard);
        for (uint32_t j = 0; j < shard->row_count; ++j) {
            const EpochCrdtRowEntry& row = rows[j];
            if (row.used != 0 && row.cen == epoch && row.csn == csn && row.server_id == server_id) {
                entries->push_back(&row);
            }
        }
        SpinLockRelease(&shard->lock);
    }
}

void EpochCrdtShmemBuildApplyTxn(const E_WireTxn& source, E_WireTxn* out)
{
    Assert(out != nullptr);
    if (out == nullptr) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("epoch crdt apply invariant: out is NULL")));
    }
    Assert(g_epochCrdtShmem != nullptr);
    if (g_epochCrdtShmem == nullptr) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("epoch crdt apply invariant: CRDT shmem not initialized")));
    }

    out->csn = source.csn;
    out->epoch = source.epoch;
    out->server_id = source.server_id;
    out->txn_state = EPOCH_TXN_COMMIT;
    out->route_wid = source.route_wid;
    out->rows.clear();

    uint32_t expected_rows = EpochCrdtCountWritableRows(source);
    if (expected_rows == 0) {
        Assert(false);
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("epoch crdt apply invariant: BuildApplyTxn called for txn with no writable rows "
                    "epoch=%lu csn=%lu server_id=%u",
                 source.epoch,
                 source.csn,
                 source.server_id)));
    }

    EpochCrdtSlotHeader* slot = EpochCrdtGetSlot(source.epoch);
    if (slot == nullptr) {
        Assert(false);
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("epoch crdt apply invariant: CRDT slot missing epoch=%lu csn=%lu server_id=%u",
                 source.epoch,
                 source.csn,
                 source.server_id)));
    }

    SpinLockAcquire(&slot->meta_lock);
    if (slot->bound_epoch != source.epoch) {
        uint64_t bound = slot->bound_epoch;
        SpinLockRelease(&slot->meta_lock);
        Assert(false);
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("epoch crdt apply invariant: slot bound_epoch mismatch epoch=%lu bound=%lu csn=%lu server_id=%u",
                 source.epoch,
                 bound,
                 source.csn,
                 source.server_id)));
    }
    SpinLockRelease(&slot->meta_lock);

    std::vector<const EpochCrdtRowEntry*> merged_rows;
    merged_rows.reserve(expected_rows);
    EpochCrdtCollectTxnMergedRows(slot, source.epoch, source.csn, source.server_id, &merged_rows);
    if (merged_rows.size() != expected_rows) {
        Assert(false);
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("epoch crdt apply invariant: CRDT row count mismatch epoch=%lu csn=%lu server_id=%u "
                    "expected=%u found=%zu",
                 source.epoch,
                 source.csn,
                 source.server_id,
                 expected_rows,
                 merged_rows.size())));
    }

    out->rows.reserve(merged_rows.size());
    for (const EpochCrdtRowEntry* entry : merged_rows) {
        E_WireRow out_row;
        EpochCrdtUnpackRow(*entry, &out_row);
        out->rows.push_back(std::move(out_row));
    }
    if (out->rows.empty()) {
        Assert(false);
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("epoch crdt apply invariant: empty apply payload epoch=%lu csn=%lu server_id=%u",
                 source.epoch,
                 source.csn,
                 source.server_id)));
    }
}

static bool EpochCrdtShmemHasRowsForNode(uint64_t epoch, uint32_t local_node, bool remote_only)
{
    if (g_epochCrdtShmem == nullptr) {
        return false;
    }
    EpochCrdtSlotHeader* slot = EpochCrdtGetSlot(epoch);
    if (slot == nullptr) {
        return false;
    }

    EpochCrdtAcquireAllShards(slot);
    SpinLockAcquire(&slot->meta_lock);
    if (slot->bound_epoch != epoch) {
        SpinLockRelease(&slot->meta_lock);
        EpochCrdtReleaseAllShards(slot);
        return false;
    }
    SpinLockRelease(&slot->meta_lock);

    uint32_t num_shards = EpochCrdtNumShardsRuntime();
    for (uint32_t i = 0; i < num_shards; ++i) {
        EpochCrdtShardHeader* shard = EpochCrdtShardAt(slot, i);
        EpochCrdtRowEntry* rows = EpochCrdtShardRows(shard);
        for (uint32_t j = 0; j < shard->row_count; ++j) {
            const EpochCrdtRowEntry& row = rows[j];
            if (row.used == 0) {
                continue;
            }
            bool match = remote_only ? (row.server_id != local_node) : (row.server_id == local_node);
            if (match) {
                EpochCrdtReleaseAllShards(slot);
                return true;
            }
        }
    }
    EpochCrdtReleaseAllShards(slot);
    return false;
}

bool EpochCrdtShmemHasRemoteRows(uint64_t epoch, uint32_t local_node)
{
    return EpochCrdtShmemHasRowsForNode(epoch, local_node, true);
}

bool EpochCrdtShmemHasLocalRows(uint64_t epoch, uint32_t local_node)
{
    return EpochCrdtShmemHasRowsForNode(epoch, local_node, false);
}

bool EpochCrdtShmemIsEmpty(uint64_t epoch)
{
    if (g_epochCrdtShmem == nullptr) {
        return true;
    }
    EpochCrdtSlotHeader* slot = EpochCrdtGetSlot(epoch);
    if (slot == nullptr) {
        return true;
    }

    EpochCrdtAcquireAllShards(slot);
    SpinLockAcquire(&slot->meta_lock);
    if (slot->bound_epoch != epoch) {
        SpinLockRelease(&slot->meta_lock);
        EpochCrdtReleaseAllShards(slot);
        return true;
    }
    SpinLockRelease(&slot->meta_lock);

    uint32_t num_shards = EpochCrdtNumShardsRuntime();
    for (uint32_t i = 0; i < num_shards; ++i) {
        EpochCrdtShardHeader* shard = EpochCrdtShardAt(slot, i);
        if (shard->row_count > 0) {
            EpochCrdtReleaseAllShards(slot);
            return false;
        }
    }
    EpochCrdtReleaseAllShards(slot);
    return true;
}
