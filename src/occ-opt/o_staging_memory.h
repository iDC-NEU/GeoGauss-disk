//
// Dedicated memory context for staged tuple copies (occ-opt).
// Table-owned entries live in the staging context; copies returned to callers
// are allocated in CurrentMemoryContext so heap_freetuple is safe without the
// staging MemoryContext lock.
//

#ifndef OCC_OPT_O_STAGING_MEMORY_H
#define OCC_OPT_O_STAGING_MEMORY_H

#include "postgres.h"
#include "access/htup.h"

MemoryContext O_StagingGetMemoryContext();

bool O_StagingTupleLengthValid(HeapTuple tuple);

/* Table-owned copy: allocated in staging MemoryContext (must O_StagingFreePersistent). */
HeapTuple O_StagingCopyToPersistent(HeapTuple tuple);

void O_StagingFreePersistent(HeapTuple tuple);

/* Caller-owned copy: allocated in CurrentMemoryContext (heap_freetuple). */
HeapTuple O_StagingCopyToCaller(HeapTuple tuple);

#endif /* OCC_OPT_O_STAGING_MEMORY_H */
