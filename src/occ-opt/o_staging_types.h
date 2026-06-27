//
// Shared types for OCC version staging hash table (occ-opt).
// Implements the "Version Staging Hash Table + Deferred Write-back" design.
//

#ifndef OCC_OPT_O_STAGING_TYPES_H
#define OCC_OPT_O_STAGING_TYPES_H

#include "postgres.h"
#include "access/htup.h"
#include "utils/relcache.h"

#include <cstddef>
#include <cstdint>
#include <functional>

/* (Relation OID, PageID, TupleID) — staging table primary key */
struct O_StagingKey
{
    Oid relid;
    BlockNumber block;
    OffsetNumber offset;

    bool operator==(const O_StagingKey &other) const
    {
        return relid == other.relid && block == other.block && offset == other.offset;
    }
};

struct O_StagingKeyHash
{
    std::size_t operator()(const O_StagingKey &key) const noexcept
    {
        std::size_t h1 = std::hash<Oid>{}(key.relid);
        std::size_t h2 = std::hash<BlockNumber>{}(key.block);
        std::size_t h3 = std::hash<OffsetNumber>{}(key.offset);
        return ((h1 ^ (h2 << 1)) >> 1) ^ (h3 << 1);
    }
};

/* Page-level aggregation key for dirty tracking and batch flush */
struct O_StagingPageKey
{
    Oid relid;
    BlockNumber block;

    bool operator==(const O_StagingPageKey &other) const
    {
        return relid == other.relid && block == other.block;
    }
};

struct O_StagingPageKeyHash
{
    std::size_t operator()(const O_StagingPageKey &key) const noexcept
    {
        std::size_t h1 = std::hash<Oid>{}(key.relid);
        std::size_t h2 = std::hash<BlockNumber>{}(key.block);
        return h1 ^ (h2 << 1);
    }
};

/*
 * One staged version for a tuple slot.
 * While not flushed, this is the authoritative visible version for readers.
 */
struct O_StagingEntry
{
    HeapTuple tuple; /* nullptr when deleted tombstone */
    bool deleted;
    bool is_latest;
    uint64_t csn;
    uint64_t staged_csn; /* CSN assigned at stage time (same as csn for now) */

    O_StagingEntry() : tuple(nullptr), deleted(false), is_latest(false), csn(0), staged_csn(0) {}
};

/* Unified read-path result */
struct O_StagingLookupResult
{
    bool found;
    bool deleted;
    bool is_latest;
    uint64_t csn;
    HeapTuple tuple_copy; /* caller owns; heap_freetuple when done */

    O_StagingLookupResult()
        : found(false), deleted(false), is_latest(false), csn(0), tuple_copy(nullptr)
    {
    }
};

struct O_StagingConfig
{
    size_t shard_count;
    uint32_t dirty_threshold_per_page;
    uint64_t flush_interval_ms;
    bool enable_background_flusher;

    O_StagingConfig()
        : shard_count(64),
          dirty_threshold_per_page(32),
          flush_interval_ms(100),
          enable_background_flusher(false)
    {
    }
};

struct O_StagingStats
{
    uint64_t staged_entries;
    uint64_t staged_pages;
    uint64_t stage_upserts;
    uint64_t stage_deletes;
    uint64_t read_hits;
    uint64_t read_misses;
    uint64_t flush_pages;
    uint64_t flush_tuples;
    uint64_t flush_failures;
};

/*
 * Callback invoked for each staged tuple during page flush.
 * Return true on success; false aborts the page flush and leaves entries staged.
 */
using O_StagingTupleWriteFn = std::function<bool(
    Relation relation, ItemPointer tid, HeapTuple tuple, uint64_t csn, bool deleted)>;

struct O_StagingFlushCallbacks
{
    O_StagingTupleWriteFn write_tuple;

    O_StagingFlushCallbacks() : write_tuple(nullptr) {}
};

inline O_StagingPageKey O_StagingPageKeyFromTid(Oid relid, ItemPointer tid)
{
    O_StagingPageKey key;
    key.relid = relid;
    key.block = ItemPointerGetBlockNumber(tid);
    return key;
}

inline O_StagingKey O_StagingKeyFromTid(Oid relid, ItemPointer tid)
{
    O_StagingKey key;
    key.relid = relid;
    key.block = ItemPointerGetBlockNumber(tid);
    key.offset = ItemPointerGetOffsetNumber(tid);
    return key;
}

#endif /* OCC_OPT_O_STAGING_TYPES_H */
