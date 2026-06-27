//
// Unified facade implementation for version staging hash table.
//

#include "o_staging_api.h"
#include "o_staging_table.h"
#include "o_dirty_page_tracker.h"
#include "o_staging_flush.h"
#include "o_background_flusher.h"

#include "utils/rel.h"

namespace {

bool g_staging_enabled = false;
O_StagingConfig g_staging_config;

bool O_StagingAccessTypeNeedsStage(AccessType type)
{
    switch (type) {
        case WR:
        case DEL:
        case INS:
        case INV:
            return true;
        default:
            return false;
    }
}

void O_StagingMaybeRequestFlush(Oid relid, ItemPointer tid, uint64_t csn, bool is_new_slot)
{
    O_DirtyPageTracker &tracker = O_DirtyPageTracker::Instance();
    if (tracker.RecordStaged(relid, tid, csn, is_new_slot)) {
        BlockNumber block = ItemPointerGetBlockNumber(tid);
        O_BackgroundFlusher::Instance().RequestImmediateFlush(relid, block);
    }
}

}  // namespace

void O_StagingInit(const O_StagingConfig *config)
{
    if (config != nullptr) {
        g_staging_config = *config;
    } else {
        g_staging_config = O_StagingConfig{};
    }

    O_StagingTable::Instance().Configure(g_staging_config.shard_count);
    O_DirtyPageTracker::Instance().Configure(g_staging_config.dirty_threshold_per_page);
    O_BackgroundFlusher::Instance().Configure(g_staging_config);

    O_StagingFlushCallbacks callbacks;
    callbacks.write_tuple = O_StagingFlush::DefaultNoOpWrite;
    O_StagingFlush::Instance().SetCallbacks(callbacks);

    if (g_staging_config.enable_background_flusher) {
        O_StagingStartBackgroundFlusher();
    }
}

void O_StagingShutdown()
{
    O_StagingStopBackgroundFlusher();
    O_StagingFlushAll();
    O_DirtyPageTracker::Instance().ClearAll();
    g_staging_enabled = false;
}

bool O_StagingEnabled()
{
    return g_staging_enabled;
}

void O_StagingSetEnabled(bool enabled)
{
    g_staging_enabled = enabled;
    if (enabled && g_staging_config.enable_background_flusher) {
        O_StagingStartBackgroundFlusher();
    } else if (!enabled) {
        O_StagingStopBackgroundFlusher();
    }
}

const O_StagingConfig &O_StagingGetConfig()
{
    return g_staging_config;
}

void O_StagingSetConfig(const O_StagingConfig &config)
{
    g_staging_config = config;
    O_StagingTable::Instance().Configure(config.shard_count);
    O_DirtyPageTracker::Instance().Configure(config.dirty_threshold_per_page);
    O_BackgroundFlusher::Instance().Configure(config);
}

void O_StagingSetFlushCallbacks(const O_StagingFlushCallbacks &callbacks)
{
    O_StagingFlush::Instance().SetCallbacks(callbacks);
}

bool O_StagingStageCommit(Relation relation, ItemPointer tid, HeapTuple tuple, uint64_t csn,
                          AccessType access_type)
{
    if (!g_staging_enabled || relation == nullptr || tid == nullptr) {
        return false;
    }
    if (!O_StagingAccessTypeNeedsStage(access_type)) {
        return false;
    }

    O_StagingTable &staging = O_StagingTable::Instance();
    Oid relid = RelationGetRelid(relation);
    bool is_new_slot = false;

    switch (access_type) {
        case DEL:
            is_new_slot = staging.StageDelete(relation, tid, csn);
            break;
        case INS:
        case WR:
        case INV:
            if (tuple == nullptr || tuple->t_data == nullptr) {
                return false;
            }
            is_new_slot = staging.StageUpsert(relation, tid, tuple, csn);
            break;
        default:
            return false;
    }

    O_StagingMaybeRequestFlush(relid, tid, csn, is_new_slot);
    return true;
}

bool O_StagingTryRead(Relation relation, ItemPointer tid, O_StagingLookupResult &result)
{
    if (!g_staging_enabled || relation == nullptr || tid == nullptr) {
        return false;
    }
    return O_StagingTable::Instance().LookupForRead(relation, tid, result);
}

HeapTuple O_StagingReadCopyTuple(Relation relation, ItemPointer tid)
{
    if (!g_staging_enabled || relation == nullptr || tid == nullptr) {
        return nullptr;
    }
    return O_StagingTable::Instance().LookupCopyTuple(relation, tid);
}

bool O_StagingHasEntry(Relation relation, ItemPointer tid)
{
    if (!g_staging_enabled || relation == nullptr || tid == nullptr) {
        return false;
    }
    O_StagingEntry entry;
    return O_StagingTable::Instance().Lookup(relation, tid, entry);
}

bool O_StagingCheckVersion(Relation relation, ItemPointer tid, uint64_t expected_csn,
                           bool check_latest)
{
    if (!g_staging_enabled || relation == nullptr || tid == nullptr) {
        return true;
    }

    O_StagingEntry entry;
    if (!O_StagingTable::Instance().Lookup(relation, tid, entry)) {
        return true;
    }

    if (entry.csn != expected_csn) {
        return false;
    }
    if (check_latest && !entry.is_latest && !entry.deleted) {
        return false;
    }
    return true;
}

size_t O_StagingFlushPage(Oid relid, BlockNumber block)
{
    return O_StagingFlush::Instance().FlushPage(relid, block);
}

size_t O_StagingFlushAll()
{
    return O_StagingFlush::Instance().FlushAll();
}

void O_StagingFlusherTick()
{
    if (!g_staging_enabled) {
        return;
    }
    O_BackgroundFlusher::Instance().Tick();
}

void O_StagingStartBackgroundFlusher()
{
    O_BackgroundFlusher::Instance().Start();
}

void O_StagingStopBackgroundFlusher()
{
    O_BackgroundFlusher::Instance().Stop();
}

O_StagingStats O_StagingGetStats()
{
    O_StagingStats stats = O_StagingTable::Instance().GetStatsSnapshot();
    stats.staged_pages = O_DirtyPageTracker::Instance().GetTrackedPageCount();
    return stats;
}
