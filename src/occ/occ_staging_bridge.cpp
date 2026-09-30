#include "occ/o_staging.h"
#include "occ/occ_internal.h"

#include "epoch/e_config.h"
#include "epoch/e_tip_staging_shmem.h"

#include "o_staging_api.h"
#include "o_staging_heap_write.h"

#include "utils/guc.h"

#include <climits>
#include <mutex>

static std::once_flag g_staging_once;
static std::once_flag g_epoch_local_tip_once;

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

void OccStagingInitEpochLocalTip(void)
{
    if (!g_epoch_local_tip_staging) {
        return;
    }
    std::call_once(g_epoch_local_tip_once, []() {
        /* Cross-process tip: attach shmem; flag O_StagingEnabled for Materialize checks. */
        EpochTipStagingShmemInit();
        O_StagingSetEnabled(true);
    });
}

void OccStagingFlusherTick(void)
{
    /*
     * tip flush runs on epoch worker main (PollSendSeal) where Relation open is safe.
     * Physical std::thread must not heap_open / SysCache.
     */
    if (g_epoch_local_tip_staging) {
        return;
    }
    if (!O_StagingEnabled()) {
        return;
    }
    O_StagingFlusherTick();
}

bool OccStagingTryReadTuple(Relation rel, ItemPointer tid, HeapTuple *out_copy, bool *is_deleted)
{
    if (rel == nullptr || tid == nullptr) {
        return false;
    }

    if (g_epoch_local_tip_staging) {
        OccStagingInitEpochLocalTip();
        return EpochTipStagingLookupCopy(rel, tid, out_copy, is_deleted);
    }

    if (!O_StagingEnabled()) {
        if (!OccStagingOverlayEnabled()) {
            return false;
        }
        OccStagingEnsureInit();
        if (!O_StagingEnabled()) {
            return false;
        }
    }

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

bool OccStagingHasEntry(Relation rel, ItemPointer tid)
{
    if (rel == nullptr || tid == nullptr) {
        return false;
    }
    if (g_epoch_local_tip_staging) {
        OccStagingInitEpochLocalTip();
        return EpochTipStagingHasEntry(rel, tid);
    }
    if (!O_StagingEnabled()) {
        if (!OccStagingOverlayEnabled()) {
            return false;
        }
        OccStagingEnsureInit();
        if (!O_StagingEnabled()) {
            return false;
        }
    }
    return O_StagingHasEntry(rel, tid);
}

bool OccStagingCheckTupleVersion(Relation rel, ItemPointer tid, uint64_t expected_csn, bool check_latest)
{
    if (rel == nullptr || tid == nullptr) {
        return true;
    }
    if (g_epoch_local_tip_staging) {
        OccStagingInitEpochLocalTip();
        HeapTuple copy = nullptr;
        bool deleted = false;
        if (!EpochTipStagingLookupCopy(rel, tid, &copy, &deleted)) {
            return true;
        }
        if (deleted || copy == nullptr) {
            if (copy != nullptr) {
                heap_freetuple(copy);
            }
            return false;
        }
        bool ok = true;
        if (check_latest && copy->t_data != nullptr && !copy->t_data->is_latest) {
            ok = false;
        }
        if (ok && copy->t_data != nullptr && copy->t_data->m_rowCSN != expected_csn) {
            ok = false;
        }
        heap_freetuple(copy);
        return ok;
    }
    if (!O_StagingEnabled()) {
        if (!OccStagingOverlayEnabled()) {
            return true;
        }
        OccStagingEnsureInit();
        if (!O_StagingEnabled()) {
            return true;
        }
    }

    if (!O_StagingHasEntry(rel, tid)) {
        return true;
    }
    return O_StagingCheckVersion(rel, tid, expected_csn, check_latest);
}

bool OccStageAccessCommit(Relation rel, ItemPointer tid, HeapTuple tuple, uint64_t csn, AccessType access_type)
{
    if (g_epoch_local_tip_staging) {
        OccStagingInitEpochLocalTip();
        switch (access_type) {
            case DEL:
                return EpochTipStagingDelete(rel, tid, csn);
            case WR:
            case INV:
            case INS:
                return EpochTipStagingUpsert(rel, tid, tuple, csn);
            default:
                return false;
        }
    }
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
