//
// Dedicated memory context for staged tuple copies (occ-opt).
//

#ifndef OCC_OPT_O_STAGING_MEMORY_H
#define OCC_OPT_O_STAGING_MEMORY_H

#include "postgres.h"
#include "access/htup.h"

MemoryContext O_StagingGetMemoryContext();

bool O_StagingTupleLengthValid(HeapTuple tuple);

HeapTuple O_StagingCopyToPersistent(HeapTuple tuple);

void O_StagingFreePersistent(HeapTuple tuple);

#endif /* OCC_OPT_O_STAGING_MEMORY_H */
