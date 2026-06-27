//
// Legacy O_DeltaTable API — thin wrapper over O_StagingTable.
//
// Existing occ/ integration (g_enable_occ_delta) continues to compile against
// include/occ/o_delta_table.h. When staging mode is fully wired, callers should
// migrate to o_staging_api.h.
//

#include "occ/o_delta_table.h"
#include "o_staging_table.h"
#include "o_staging_memory.h"

#include "utils/rel.h"

O_DeltaTable &O_DeltaTable::Instance()
{
    static O_DeltaTable instance;
    return instance;
}

O_DeltaKey O_DeltaTable::MakeKey(Relation relation, ItemPointer tid) const
{
    O_DeltaKey key;
    key.relid = RelationGetRelid(relation);
    key.block = ItemPointerGetBlockNumber(tid);
    key.offset = ItemPointerGetOffsetNumber(tid);
    return key;
}

size_t O_DeltaTable::ShardIndex(const O_DeltaKey &key) const
{
    return O_DeltaKeyHash{}(key) % SHARD_COUNT;
}

void O_DeltaTable::Upsert(Relation relation, ItemPointer tid, HeapTuple tuple, uint64_t csn)
{
    O_StagingTable::Instance().StageUpsert(relation, tid, tuple, csn);
}

void O_DeltaTable::MarkDeleted(Relation relation, ItemPointer tid, uint64_t csn)
{
    O_StagingTable::Instance().StageDelete(relation, tid, csn);
}

bool O_DeltaTable::Lookup(Relation relation, ItemPointer tid, O_DeltaEntry &outEntry)
{
    O_StagingEntry staged;
    if (!O_StagingTable::Instance().Lookup(relation, tid, staged)) {
        return false;
    }

    outEntry.deleted = staged.deleted;
    outEntry.is_latest = staged.is_latest;
    outEntry.csn = staged.csn;
    outEntry.tuple = nullptr;
    return true;
}

HeapTuple O_DeltaTable::LookupCopyTuple(Relation relation, ItemPointer tid)
{
    return O_StagingTable::Instance().LookupCopyTuple(relation, tid);
}
