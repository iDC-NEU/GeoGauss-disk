//
// Implementation of global delta table for OCC optimized path.
//

#include "occ/o_delta_table.h"

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

void O_DeltaTable::Upsert(Relation relation, ItemPointer tid, HeapTuple tuple, uint64 csn)
{
    if (relation == nullptr || tid == nullptr || tuple == nullptr || tuple->t_data == nullptr)
    {
        return;
    }

    O_DeltaKey key = MakeKey(relation, tid);

    /* Make an independent copy of tuple in current memory context. */
    HeapTuple copy = heap_copytuple(tuple);

    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_table.find(key);
    if (it == m_table.end())
    {
        O_DeltaEntry entry;
        entry.tuple = copy;
        entry.deleted = false;
        entry.csn = csn;
        m_table.emplace(key, entry);
    }
    else
    {
        /* Replace existing entry. Free previous tuple if any. */
        if (it->second.tuple != nullptr)
        {
            heap_freetuple(it->second.tuple);
        }
        it->second.tuple = copy;
        it->second.deleted = false;
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

    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_table.find(key);
    if (it == m_table.end())
    {
        O_DeltaEntry entry;
        entry.tuple = nullptr;
        entry.deleted = true;
        entry.csn = csn;
        m_table.emplace(key, entry);
    }
    else
    {
        if (it->second.tuple != nullptr)
        {
            heap_freetuple(it->second.tuple);
            it->second.tuple = nullptr;
        }
        it->second.deleted = true;
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

    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_table.find(key);
    if (it == m_table.end())
    {
        return false;
    }

    outEntry = it->second;
    return true;
}

