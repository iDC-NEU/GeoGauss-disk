//
// Unified facade for version staging hash table (occ-opt).
//
// Integration points (not wired yet — call from occ/ when ready):
//   - O_StagingStageCommit()  replaces immediate heap write in O_Access::WriteGlobalChanges
//   - O_StagingTryRead()      called from occ_sentinel / O_TxnAccess read path
//   - O_StagingCheckVersion() called from OccTransactionManager validation
//   - O_StagingFlusherTick()  driven by postmaster or bgworker
//

#ifndef OCC_OPT_O_STAGING_API_H
#define OCC_OPT_O_STAGING_API_H

#include "o_staging_types.h"
#include "occ/o_global.h"

#ifdef __cplusplus
extern "C" {
#endif

void O_StagingInit(const O_StagingConfig *config);
void O_StagingShutdown();

bool O_StagingEnabled();
void O_StagingSetEnabled(bool enabled);

const O_StagingConfig &O_StagingGetConfig();
void O_StagingSetConfig(const O_StagingConfig &config);

void O_StagingSetFlushCallbacks(const O_StagingFlushCallbacks &callbacks);

/*
 * Write path: stage committed version after OCC validation.
 * Does NOT write heap page — deferred write-back handles persistence.
 */
bool O_StagingStageCommit(Relation relation, ItemPointer tid, HeapTuple tuple, uint64_t csn,
                          AccessType access_type);

/*
 * Read path: consult staging table before heap.
 */
bool O_StagingTryRead(Relation relation, ItemPointer tid, O_StagingLookupResult &result);

HeapTuple O_StagingReadCopyTuple(Relation relation, ItemPointer tid);

/*
 * Validation: compare expected CSN against staged version (if present).
 * Returns true when no staged entry exists (caller should fall back to heap check).
 */
bool O_StagingHasEntry(Relation relation, ItemPointer tid);
bool O_StagingCheckVersion(Relation relation, ItemPointer tid, uint64_t expected_csn,
                           bool check_latest);

/*
 * Flush / maintenance
 */
size_t O_StagingFlushPage(Oid relid, BlockNumber block);
size_t O_StagingFlushAll();
void O_StagingFlusherTick();
void O_StagingStartBackgroundFlusher();
void O_StagingStopBackgroundFlusher();

O_StagingStats O_StagingGetStats();

#ifdef __cplusplus
}
#endif

#endif /* OCC_OPT_O_STAGING_API_H */
