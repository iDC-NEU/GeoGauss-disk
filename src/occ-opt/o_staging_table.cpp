//
// Version Staging Hash Table implementation.
//

#include "o_staging_table.h"
#include "o_staging_memory.h"

#include "utils/rel.h"

O_StagingTable &O_StagingTable::Instance()
{
    static O_StagingTable instance;
    return instance;
}

O_StagingTable::O_StagingTable() : m_shardCount(DEFAULT_SHARD_COUNT)
{
    ResetStats();
}

void O_StagingTable::Configure(size_t shard_count)
{
    if (shard_count == 0) {
        shard_count = DEFAULT_SHARD_COUNT;
    }
    if (shard_count > MAX_SHARD_COUNT) {
        ereport(WARNING,
                (errmsg("O_StagingTable::Configure: shard_count %zu capped to %zu",
                        shard_count,
                        MAX_SHARD_COUNT)));
        shard_count = MAX_SHARD_COUNT;
    }
    if (shard_count == m_shardCount) {
        return;
    }

    for (size_t i = 0; i < m_shardCount; ++i) {
        std::lock_guard<std::mutex> lock(m_mutexes[i]);
        if (!m_shards[i].empty()) {
            ereport(WARNING,
                    (errmsg("O_StagingTable::Configure ignored: table is not empty (%zu shards)",
                            m_shardCount)));
            return;
        }
    }

    m_shardCount = shard_count;
}

O_StagingKey O_StagingTable::MakeKey(Relation relation, ItemPointer tid) const
{
    return O_StagingKeyFromTid(RelationGetRelid(relation), tid);
}

size_t O_StagingTable::ShardIndex(const O_StagingKey &key) const
{
    return O_StagingKeyHash{}(key) % m_shardCount;
}

bool O_StagingTable::StageUpsert(Relation relation, ItemPointer tid, HeapTuple tuple, uint64_t csn)
{
    if (relation == nullptr || tid == nullptr || tuple == nullptr || tuple->t_data == nullptr) {
        return false;
    }

    O_StagingKey key = MakeKey(relation, tid);
    size_t shard = ShardIndex(key);

    std::lock_guard<std::mutex> lock(m_mutexes[shard]);
    StagingMap &table = m_shards[shard];

    HeapTuple copy = O_StagingCopyToPersistent(tuple);
    if (copy == nullptr) {
        return false;
    }

    copy->t_self = *tid;
    copy->t_tableOid = RelationGetRelid(relation);
    copy->t_bucketId = RelationGetBktid(relation);
    if (copy->t_data != nullptr) {
        copy->t_data->m_rowCSN = csn;
        copy->t_data->is_latest = true;
    }

    bool is_new = false;
    auto it = table.find(key);
    if (it == table.end()) {
        is_new = true;
        O_StagingEntry entry;
        entry.tuple = copy;
        entry.deleted = false;
        entry.is_latest = true;
        entry.csn = csn;
        entry.staged_csn = csn;
        table.emplace(key, entry);
    } else {
        if (it->second.tuple != nullptr) {
            O_StagingFreePersistent(it->second.tuple);
        }
        it->second.tuple = copy;
        it->second.deleted = false;
        it->second.is_latest = true;
        it->second.csn = csn;
        it->second.staged_csn = csn;
    }

    {
        std::lock_guard<std::mutex> statsLock(m_statsMutex);
        m_stats.stage_upserts++;
    }
    return is_new;
}

bool O_StagingTable::StageDelete(Relation relation, ItemPointer tid, uint64_t csn)
{
    if (relation == nullptr || tid == nullptr) {
        return false;
    }

    O_StagingKey key = MakeKey(relation, tid);
    size_t shard = ShardIndex(key);

    std::lock_guard<std::mutex> lock(m_mutexes[shard]);
    StagingMap &table = m_shards[shard];
    auto it = table.find(key);
    bool is_new = false;
    if (it == table.end()) {
        is_new = true;
        O_StagingEntry entry;
        entry.tuple = nullptr;
        entry.deleted = true;
        entry.is_latest = false;
        entry.csn = csn;
        entry.staged_csn = csn;
        table.emplace(key, entry);
    } else {
        if (it->second.tuple != nullptr) {
            O_StagingFreePersistent(it->second.tuple);
            it->second.tuple = nullptr;
        }
        it->second.deleted = true;
        it->second.is_latest = false;
        it->second.csn = csn;
        it->second.staged_csn = csn;
    }

    {
        std::lock_guard<std::mutex> statsLock(m_statsMutex);
        m_stats.stage_deletes++;
    }
    return is_new;
}

bool O_StagingTable::Lookup(Relation relation, ItemPointer tid, O_StagingEntry &outEntry) const
{
    if (relation == nullptr || tid == nullptr) {
        return false;
    }

    O_StagingKey key = MakeKey(relation, tid);
    size_t shard = ShardIndex(key);

    std::lock_guard<std::mutex> lock(m_mutexes[shard]);
    auto it = m_shards[shard].find(key);
    if (it == m_shards[shard].end()) {
        std::lock_guard<std::mutex> statsLock(m_statsMutex);
        m_stats.read_misses++;
        return false;
    }

    outEntry.deleted = it->second.deleted;
    outEntry.is_latest = it->second.is_latest;
    outEntry.csn = it->second.csn;
    outEntry.staged_csn = it->second.staged_csn;
    outEntry.tuple = nullptr;

    {
        std::lock_guard<std::mutex> statsLock(m_statsMutex);
        m_stats.read_hits++;
    }
    return true;
}

HeapTuple O_StagingTable::LookupCopyTuple(Relation relation, ItemPointer tid) const
{
    if (relation == nullptr || tid == nullptr) {
        return nullptr;
    }

    O_StagingKey key = MakeKey(relation, tid);
    size_t shard = ShardIndex(key);

    std::lock_guard<std::mutex> lock(m_mutexes[shard]);
    auto it = m_shards[shard].find(key);
    if (it == m_shards[shard].end()) {
        return nullptr;
    }
    if (it->second.deleted || !O_StagingTupleLengthValid(it->second.tuple)) {
        return nullptr;
    }

    /* Caller-owned copy (CurrentMemoryContext); never share staging-mcxt tuples. */
    return O_StagingCopyToCaller(it->second.tuple);
}

bool O_StagingTable::LookupForRead(Relation relation, ItemPointer tid,
                                   O_StagingLookupResult &result) const
{
    result = O_StagingLookupResult();
    if (relation == nullptr || tid == nullptr) {
        return false;
    }

    O_StagingKey key = MakeKey(relation, tid);
    size_t shard = ShardIndex(key);

    std::lock_guard<std::mutex> lock(m_mutexes[shard]);
    auto it = m_shards[shard].find(key);
    if (it == m_shards[shard].end()) {
        std::lock_guard<std::mutex> statsLock(m_statsMutex);
        m_stats.read_misses++;
        return false;
    }

    result.found = true;
    result.deleted = it->second.deleted;
    result.is_latest = it->second.is_latest;
    result.csn = it->second.csn;

    if (!it->second.deleted) {
        if (!O_StagingTupleLengthValid(it->second.tuple)) {
            result.found = false;
            return false;
        }
        /* Caller-owned: heap_freetuple OK; avoids concurrent free on staging mcxt. */
        result.tuple_copy = O_StagingCopyToCaller(it->second.tuple);
        if (result.tuple_copy == nullptr) {
            result.found = false;
            return false;
        }
    }

    {
        std::lock_guard<std::mutex> statsLock(m_statsMutex);
        m_stats.read_hits++;
    }
    return true;
}

bool O_StagingTable::Remove(Relation relation, ItemPointer tid)
{
    if (relation == nullptr || tid == nullptr) {
        return false;
    }

    O_StagingKey key = MakeKey(relation, tid);
    size_t shard = ShardIndex(key);

    std::lock_guard<std::mutex> lock(m_mutexes[shard]);
    auto it = m_shards[shard].find(key);
    if (it == m_shards[shard].end()) {
        return false;
    }

    if (it->second.tuple != nullptr) {
        O_StagingFreePersistent(it->second.tuple);
    }
    m_shards[shard].erase(it);
    return true;
}

void O_StagingTable::CollectPageEntries(Oid relid, BlockNumber block,
                                        std::vector<std::pair<O_StagingKey, O_StagingEntry>> &outEntries) const
{
    outEntries.clear();
    for (size_t shard = 0; shard < m_shardCount; ++shard) {
        std::lock_guard<std::mutex> lock(m_mutexes[shard]);
        for (const auto &kv : m_shards[shard]) {
            if (kv.first.relid == relid && kv.first.block == block) {
                outEntries.push_back(kv);
            }
        }
    }
}

bool O_StagingTable::PageHasEntries(Oid relid, BlockNumber block) const
{
    return CountPageEntries(relid, block) > 0;
}

size_t O_StagingTable::CountPageEntries(Oid relid, BlockNumber block) const
{
    size_t count = 0;
    for (size_t shard = 0; shard < m_shardCount; ++shard) {
        std::lock_guard<std::mutex> lock(m_mutexes[shard]);
        for (const auto &kv : m_shards[shard]) {
            if (kv.first.relid == relid && kv.first.block == block) {
                count++;
            }
        }
    }
    return count;
}

O_StagingStats O_StagingTable::GetStatsSnapshot() const
{
    std::lock_guard<std::mutex> statsLock(m_statsMutex);
    O_StagingStats stats = m_stats;
    stats.staged_entries = 0;
    for (size_t i = 0; i < m_shardCount; ++i) {
        std::lock_guard<std::mutex> lock(m_mutexes[i]);
        stats.staged_entries += m_shards[i].size();
    }
    return stats;
}

void O_StagingTable::ResetStats()
{
    std::lock_guard<std::mutex> statsLock(m_statsMutex);
    m_stats = O_StagingStats{};
}
