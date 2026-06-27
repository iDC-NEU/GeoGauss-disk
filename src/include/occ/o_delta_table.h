//
// Delta table for OCC optimized path (occ-opt).
// Stores committed versions (and deletions) keyed by relation + TID,
// so readers can find latest tuples without touching heap buffers.
//

#ifndef OPENGAUSS_O_DELTA_TABLE_H
#define OPENGAUSS_O_DELTA_TABLE_H

#include "postgres.h"
#include "access/htup.h"
//#include "access/htup_details.h"
//#include "storage/block.h"
//#include "storage/itemptr.h"
#include "utils/relcache.h"

#include <array>
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
    bool is_latest;
    uint64 csn;

    O_DeltaEntry() : tuple(nullptr), deleted(false), is_latest(false), csn(0) {}
};

class O_DeltaTable
{
public:
    static constexpr size_t SHARD_COUNT = 64;

    static O_DeltaTable &Instance();

    /* Upsert committed version into delta table. */
    void Upsert(Relation relation, ItemPointer tid, HeapTuple tuple, uint64_t csn);

    /* Mark tuple as deleted in delta table. */
    void MarkDeleted(Relation relation, ItemPointer tid, uint64_t csn);

    /*
     * Lookup latest committed entry in delta table.
     * Returns true when an entry exists (including deleted entries).
     * 元数据（csn/deleted/is_latest）在锁内读取；勿在返回后使用 outEntry.tuple 指针。
     */
    bool Lookup(Relation relation, ItemPointer tid, O_DeltaEntry &outEntry);

    /*
     * 在持锁期间 heap_copytuple，返回独立副本；调用方负责 heap_freetuple（若需释放）。
     * 未命中或已删除时返回 nullptr。
     */
    HeapTuple LookupCopyTuple(Relation relation, ItemPointer tid);

private:
    O_DeltaTable() = default;
    O_DeltaTable(const O_DeltaTable &) = delete;
    O_DeltaTable &operator=(const O_DeltaTable &) = delete;

    O_DeltaKey MakeKey(Relation relation, ItemPointer tid) const;
    size_t ShardIndex(const O_DeltaKey &key) const;

    using DeltaMap = std::unordered_map<O_DeltaKey, O_DeltaEntry, O_DeltaKeyHash>;

    std::array<DeltaMap, SHARD_COUNT> m_shards;
    std::array<std::mutex, SHARD_COUNT> m_mutexes;
};

#endif // OPENGAUSS_O_DELTA_TABLE_H
