//
// Deferred write-back implementation.
//

#include "o_staging_flush.h"
#include "o_staging_table.h"
#include "o_dirty_page_tracker.h"

#include "access/heapam.h"
#include "utils/rel.h"

O_StagingFlush &O_StagingFlush::Instance()
{
    static O_StagingFlush instance;
    return instance;
}

O_StagingFlush::O_StagingFlush()
{
    m_callbacks.write_tuple = DefaultNoOpWrite;
}

void O_StagingFlush::SetCallbacks(const O_StagingFlushCallbacks &callbacks)
{
    std::lock_guard<std::mutex> lock(m_callbackMutex);
    m_callbacks = callbacks;
    if (!m_callbacks.write_tuple) {
        m_callbacks.write_tuple = DefaultNoOpWrite;
    }
}

bool O_StagingFlush::DefaultNoOpWrite(Relation relation, ItemPointer tid, HeapTuple tuple,
                                      uint64_t csn, bool deleted)
{
    (void)relation;
    (void)tid;
    (void)tuple;
    (void)csn;
    (void)deleted;
    return true;
}

size_t O_StagingFlush::FlushPage(Oid relid, BlockNumber block)
{
    O_StagingTable &staging = O_StagingTable::Instance();
    O_DirtyPageTracker &tracker = O_DirtyPageTracker::Instance();

    std::vector<std::pair<O_StagingKey, O_StagingEntry>> entries;
    staging.CollectPageEntries(relid, block, entries);
    if (entries.empty()) {
        O_StagingPageKey pageKey;
        pageKey.relid = relid;
        pageKey.block = block;
        tracker.ClearPage(pageKey);
        return 0;
    }

    Relation rel = heap_open(relid, RowExclusiveLock);
    if (!RelationIsValid(rel)) {
        return 0;
    }

    O_StagingFlushCallbacks callbacks;
    {
        std::lock_guard<std::mutex> lock(m_callbackMutex);
        callbacks = m_callbacks;
    }

    size_t flushed = 0;
    size_t failures = 0;

    for (const auto &kv : entries) {
        ItemPointerData tid;
        ItemPointerSet(&tid, kv.first.block, kv.first.offset);

        bool ok = false;
        if (kv.second.deleted) {
            ok = callbacks.write_tuple(rel, &tid, nullptr, kv.second.csn, true);
        } else if (kv.second.tuple != nullptr) {
            ok = callbacks.write_tuple(rel, &tid, kv.second.tuple, kv.second.csn, false);
        } else {
            ok = true;
        }

        if (!ok) {
            failures++;
            continue;
        }

        if (staging.Remove(rel, &tid)) {
            flushed++;
        }
    }

    heap_close(rel, RowExclusiveLock);
    tracker.RecordFlushed(relid, block, static_cast<uint32_t>(flushed));

    O_StagingStats stats = staging.GetStatsSnapshot();
    (void)stats;
    if (failures > 0) {
        ereport(DEBUG1,
                (errmsg("O_StagingFlush::FlushPage rel=%u block=%u flushed=%zu failures=%zu",
                        relid, block, flushed, failures)));
    }

    return flushed;
}

size_t O_StagingFlush::FlushAll()
{
    O_StagingTable &staging = O_StagingTable::Instance();
    O_DirtyPageTracker &tracker = O_DirtyPageTracker::Instance();

    std::vector<O_StagingPageKey> pages;
    tracker.GetAllDirtyPages(pages);

    if (pages.empty()) {
        return 0;
    }

    size_t totalFlushed = 0;
    for (const O_StagingPageKey &pageKey : pages) {
        totalFlushed += FlushPage(pageKey.relid, pageKey.block);
    }
    return totalFlushed;
}
