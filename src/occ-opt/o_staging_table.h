//
// Version Staging Hash Table — core in-memory store for deferred write-back.
//

#ifndef OCC_OPT_O_STAGING_TABLE_H
#define OCC_OPT_O_STAGING_TABLE_H

#include "o_staging_types.h"

#include <array>
#include <mutex>
#include <unordered_map>
#include <vector>

class O_StagingTable
{
public:
    static constexpr size_t DEFAULT_SHARD_COUNT = 64;
    static constexpr size_t MAX_SHARD_COUNT = 64;

    static O_StagingTable &Instance();

    void Configure(size_t shard_count);

    /*
     * Write path: stage a committed tuple version without touching heap page.
     * Overwrites any previous staged version for the same (relid, block, offset).
     * Returns true when this is a newly staged slot (not overwriting an existing entry).
     */
    bool StageUpsert(Relation relation, ItemPointer tid, HeapTuple tuple, uint64_t csn);

    /* Returns true when this is a newly staged slot. */
    bool StageDelete(Relation relation, ItemPointer tid, uint64_t csn);

    /*
     * Read path: metadata lookup (tuple pointer is NOT returned — use LookupCopyTuple).
     */
    bool Lookup(Relation relation, ItemPointer tid, O_StagingEntry &outEntry) const;

    /* Caller-owned HeapTuple in CurrentMemoryContext (heap_freetuple). */
    HeapTuple LookupCopyTuple(Relation relation, ItemPointer tid) const;

    bool LookupForRead(Relation relation, ItemPointer tid, O_StagingLookupResult &result) const;

    /*
     * Remove a staged entry after successful flush to heap.
     */
    bool Remove(Relation relation, ItemPointer tid);

    /*
     * Collect all staged entries belonging to one heap page.
     * Used by deferred write-back to batch-merge modifications.
     */
    void CollectPageEntries(Oid relid, BlockNumber block,
                            std::vector<std::pair<O_StagingKey, O_StagingEntry>> &outEntries) const;

    bool PageHasEntries(Oid relid, BlockNumber block) const;

    size_t CountPageEntries(Oid relid, BlockNumber block) const;

    O_StagingStats GetStatsSnapshot() const;

    void ResetStats();

private:
    O_StagingTable();
    O_StagingTable(const O_StagingTable &) = delete;
    O_StagingTable &operator=(const O_StagingTable &) = delete;

    O_StagingKey MakeKey(Relation relation, ItemPointer tid) const;
    size_t ShardIndex(const O_StagingKey &key) const;

    using StagingMap = std::unordered_map<O_StagingKey, O_StagingEntry, O_StagingKeyHash>;

    size_t m_shardCount;
    std::array<StagingMap, MAX_SHARD_COUNT> m_shards;
    mutable std::array<std::mutex, MAX_SHARD_COUNT> m_mutexes;

    mutable O_StagingStats m_stats;
    mutable std::mutex m_statsMutex;
};

#endif /* OCC_OPT_O_STAGING_TABLE_H */
