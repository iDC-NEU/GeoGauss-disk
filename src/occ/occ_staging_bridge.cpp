#include "occ/o_staging.h"
#include "occ/occ_internal.h"

#include "o_staging_api.h"
#include "o_staging_heap_write.h"

#include "utils/guc.h"

#include <mutex>

static std::once_flag g_staging_once;

bool g_enable_occ_staging = false;

bool OccStagingOverlayEnabled(void)
{
    return g_enable_occ_staging || g_enable_occ_delta;
}

void OccStagingModuleInit(void)
{
    if (!g_enable_occ_staging) {
        return;
    }

    O_StagingConfig cfg;
    cfg.shard_count = 64;
    cfg.dirty_threshold_per_page = 32;
    cfg.flush_interval_ms = 100;
    cfg.enable_background_flusher = true;

    O_StagingInit(&cfg);
    O_StagingSetEnabled(true);

    O_StagingFlushCallbacks callbacks;
    callbacks.write_tuple = O_StagingHeapWriteTuple;
    O_StagingSetFlushCallbacks(callbacks);
    O_StagingStartBackgroundFlusher();
}

void OccStagingModuleShutdown(void)
{
    if (!O_StagingEnabled()) {
        return;
    }
    O_StagingShutdown();
}

void OccStagingEnsureInit(void)
{
    std::call_once(g_staging_once, OccStagingModuleInit);
}

void OccStagingFlusherTick(void)
{
    if (!O_StagingEnabled()) {
        return;
    }
    O_StagingFlusherTick();
}

bool OccStagingTryReadTuple(Relation rel, ItemPointer tid, HeapTuple *out_copy, bool *is_deleted)
{
    if (!OccStagingOverlayEnabled() || rel == nullptr || tid == nullptr) {
        return false;
    }
    OccStagingEnsureInit();

    O_StagingLookupResult result;
    if (!O_StagingTryRead(rel, tid, result)) {
        return false;
    }
    if (is_deleted != nullptr) {
        *is_deleted = result.deleted;
    }
    if (result.deleted) {
        if (out_copy != nullptr) {
            *out_copy = nullptr;
        }
        return true;
    }
    if (out_copy != nullptr) {
        *out_copy = result.tuple_copy;
    } else if (result.tuple_copy != nullptr) {
        heap_freetuple(result.tuple_copy);
    }
    return true;
}

bool OccStagingCheckTupleVersion(Relation rel, ItemPointer tid, uint64_t expected_csn, bool check_latest)
{
    if (!OccStagingOverlayEnabled() || rel == nullptr || tid == nullptr) {
        return true;
    }
    OccStagingEnsureInit();

    if (!O_StagingHasEntry(rel, tid)) {
        return true;
    }
    return O_StagingCheckVersion(rel, tid, expected_csn, check_latest);
}

bool OccStageAccessCommit(Relation rel, ItemPointer tid, HeapTuple tuple, uint64_t csn, AccessType access_type)
{
    OccStagingEnsureInit();
    return O_StagingStageCommit(rel, tid, tuple, csn, access_type);
}

void OccStagingRegisterGucs(void)
{
    DefineCustomBoolVariable("enable_occ_staging",
                             "Enable version staging hash table (deferred write-back) for OCC disk tables.",
                             NULL,
                             &g_enable_occ_staging,
                             false,
                             PGC_POSTMASTER,
                             GUC_NO_RESET_ALL,
                             NULL,
                             NULL,
                             NULL);
}
