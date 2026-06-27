#ifndef OPENGAUSS_O_STAGING_H
#define OPENGAUSS_O_STAGING_H

#include "postgres.h"
#include "access/htup.h"
#include "storage/item/itemptr.h"
#include "utils/relcache.h"
#include "occ/o_global.h"

extern bool g_enable_occ_staging;

void OccStagingRegisterGucs(void);
void OccStagingModuleInit(void);
void OccStagingModuleShutdown(void);
void OccStagingEnsureInit(void);
void OccStagingFlusherTick(void);

bool OccStagingOverlayEnabled(void);

/*
 * Read path: returns true when staging has an entry.
 * out_copy is heap_copytuple result — caller must heap_freetuple when done.
 */
bool OccStagingTryReadTuple(Relation rel, ItemPointer tid, HeapTuple *out_copy, bool *is_deleted);

bool OccStagingCheckTupleVersion(Relation rel, ItemPointer tid, uint64_t expected_csn, bool check_latest);

bool OccStageAccessCommit(Relation rel, ItemPointer tid, HeapTuple tuple, uint64_t csn, AccessType access_type);

#endif /* OPENGAUSS_O_STAGING_H */
