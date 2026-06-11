//
// Implementation of global delta table for OCC optimized path.
//

#include "occ/o_delta_table.h"
#include "utils/rel.h"
#include "utils/memutils.h"

namespace {

MemoryContext g_occ_delta_mcxt = nullptr;
/* AllocSet 非线程安全：所有 delta 上下文 alloc/free 须串行化 */
std::mutex g_delta_mcxt_mutex;

MemoryContext O_GetDeltaMemoryContext()
{
    if (g_occ_delta_mcxt == nullptr) {
        g_occ_delta_mcxt = AllocSetContextCreate(
            TopMemoryContext,
            "OCC Delta Table",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    }
    return g_occ_delta_mcxt;
}

bool O_DeltaTupleLengthValid(HeapTuple tuple)
{
    if (!HeapTupleIsValid(tuple) || tuple->t_data == nullptr) {
        return false;
    }
    Size len = tuple->t_len;
    return len > 0 && len <= MaxHeapTupleSize;
}

HeapTuple O_DeltaCopyToPersistentLocked(HeapTuple tuple)
{
    if (!O_DeltaTupleLengthValid(tuple)) {
        return nullptr;
    }

    MemoryContext old = MemoryContextSwitchTo(O_GetDeltaMemoryContext());
    HeapTuple copy = heap_copytuple(tuple);
    MemoryContextSwitchTo(old);
    return copy;
}

void O_DeltaFreePersistentLocked(HeapTuple tuple)
{
    if (tuple == nullptr) {
        return;
    }
    MemoryContext old = MemoryContextSwitchTo(O_GetDeltaMemoryContext());
    heap_freetuple(tuple);
    MemoryContextSwitchTo(old);
}

}  // namespace

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

void O_DeltaTable::Upsert(Relation relation, ItemPointer tid, HeapTuple tuple, uint64 csn)
{
    if (relation == nullptr || tid == nullptr || tuple == nullptr || tuple->t_data == nullptr)
    {
        return;
    }

    O_DeltaKey key = MakeKey(relation, tid);
    size_t shard = ShardIndex(key);

    std::lock_guard<std::mutex> lock(m_mutexes[shard]);
    DeltaMap &table = m_shards[shard];

    std::lock_guard<std::mutex> mcxtLock(g_delta_mcxt_mutex);
    HeapTuple copy = O_DeltaCopyToPersistentLocked(tuple);
    if (copy == nullptr) {
        return;
    }

    /* 索引 TID 为 delta 键，必须与 tuple->t_self 一致，否则 ctid 投影失败 */
    copy->t_self = *tid;
    copy->t_tableOid = RelationGetRelid(relation);
    copy->t_bucketId = RelationGetBktid(relation);

    auto it = table.find(key);
    if (it == table.end())
    {
        O_DeltaEntry entry;
        entry.tuple = copy;
        entry.deleted = false;
        entry.is_latest = true;
        entry.csn = csn;
        table.emplace(key, entry);
    }
    else
    {
        if (it->second.tuple != nullptr)
        {
            O_DeltaFreePersistentLocked(it->second.tuple);
        }
        it->second.tuple = copy;
        it->second.deleted = false;
        it->second.is_latest = true;
        it->second.csn = csn;
    }
}

void O_DeltaTable::MarkDeleted(Relation relation, ItemPointer tid, uint64 csn)
{
    if (relation == nullptr || tid == nullptr)
    {
        return;
    }

    O_DeltaKey key = MakeKey(relation, tid);
    size_t shard = ShardIndex(key);

    std::lock_guard<std::mutex> lock(m_mutexes[shard]);
    DeltaMap &table = m_shards[shard];
    auto it = table.find(key);
    if (it == table.end())
    {
        O_DeltaEntry entry;
        entry.tuple = nullptr;
        entry.deleted = true;
        entry.is_latest = false;
        entry.csn = csn;
        table.emplace(key, entry);
    }
    else
    {
        std::lock_guard<std::mutex> mcxtLock(g_delta_mcxt_mutex);
        if (it->second.tuple != nullptr)
        {
            O_DeltaFreePersistentLocked(it->second.tuple);
            it->second.tuple = nullptr;
        }
        it->second.deleted = true;
        it->second.is_latest = false;
        it->second.csn = csn;
    }
}

bool O_DeltaTable::Lookup(Relation relation, ItemPointer tid, O_DeltaEntry &outEntry)
{
    if (relation == nullptr || tid == nullptr)
    {
        return false;
    }

    O_DeltaKey key = MakeKey(relation, tid);
    size_t shard = ShardIndex(key);

    std::lock_guard<std::mutex> lock(m_mutexes[shard]);
    auto it = m_shards[shard].find(key);
    if (it == m_shards[shard].end())
    {
        return false;
    }

    outEntry.deleted = it->second.deleted;
    outEntry.is_latest = it->second.is_latest;
    outEntry.csn = it->second.csn;
    outEntry.tuple = nullptr;
    return true;
}

HeapTuple O_DeltaTable::LookupCopyTuple(Relation relation, ItemPointer tid)
{
    if (relation == nullptr || tid == nullptr)
    {
        return nullptr;
    }

    O_DeltaKey key = MakeKey(relation, tid);
    size_t shard = ShardIndex(key);

    std::lock_guard<std::mutex> lock(m_mutexes[shard]);
    auto it = m_shards[shard].find(key);
    if (it == m_shards[shard].end())
    {
        return nullptr;
    }
    if (it->second.deleted || !O_DeltaTupleLengthValid(it->second.tuple))
    {
        return nullptr;
    }

    /* 读期间持有两把锁，避免并发 free 导致 UAF */
    std::lock_guard<std::mutex> mcxtLock(g_delta_mcxt_mutex);
    return heap_copytuple(it->second.tuple);
}
