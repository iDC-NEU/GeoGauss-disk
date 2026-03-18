//
// Delta table for OCC optimized path (occ-opt).
// Stores committed versions (and deletions) keyed by relation + TID,
// so readers can find latest tuples without touching heap buffers.
//

#ifndef OPENGAUSS_O_DELTA_TABLE_H
#define OPENGAUSS_O_DELTA_TABLE_H

#include "postgres.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "storage/block.h"
#include "storage/itemptr.h"
#include "utils/relcache.h"

#include <unordered_map>
#include <mutex>

struct O_DeltaKey
{
    Oid relid;
    BlockNumber block;
    OffsetNumber offset;

    bool operator==(const O_DeltaKey &other) const
    {
        return relid == other.relid && block == other.block && offset == other.offset;
    }
};

struct O_DeltaKeyHash
{
    std::size_t operator()(const O_DeltaKey &key) const noexcept
    {
        std::size_t h1 = std::hash<Oid>{}(key.relid);
        std::size_t h2 = std::hash<BlockNumber>{}(key.block);
        std::size_t h3 = std::hash<OffsetNumber>{}(key.offset);
        return ((h1 ^ (h2 << 1)) >> 1) ^ (h3 << 1);
    }
};

struct O_DeltaEntry
{
    HeapTuple tuple; /* nullptr when deleted */
    bool deleted;
    uint64 csn;

    O_DeltaEntry() : tuple(nullptr), deleted(false), csn(0) {}
};

class O_DeltaTable
{
public:
    static O_DeltaTable &Instance();

    /* Upsert committed version into delta table. */
    void Upsert(Relation relation, ItemPointer tid, HeapTuple tuple, uint64 csn);

    /* Mark tuple as deleted in delta table. */
    void MarkDeleted(Relation relation, ItemPointer tid, uint64 csn);

    /*
     * Lookup latest committed entry in delta table.
     * Returns true when an entry exists (including deleted entries).
     */
    bool Lookup(Relation relation, ItemPointer tid, O_DeltaEntry &outEntry);

private:
    O_DeltaTable() = default;
    O_DeltaTable(const O_DeltaTable &) = delete;
    O_DeltaTable &operator=(const O_DeltaTable &) = delete;

    O_DeltaKey MakeKey(Relation relation, ItemPointer tid) const;

    std::unordered_map<O_DeltaKey, O_DeltaEntry, O_DeltaKeyHash> m_table;
    std::mutex m_mutex;
};

#endif // OPENGAUSS_O_DELTA_TABLE_H

