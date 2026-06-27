//
// Heap write-back callbacks for staging flush (integration-ready, not wired yet).
//
// Register via:
//   O_StagingFlushCallbacks cb;
//   cb.write_tuple = O_StagingHeapWriteTuple;
//   O_StagingSetFlushCallbacks(cb);
//

#ifndef OCC_OPT_O_STAGING_HEAP_WRITE_H
#define OCC_OPT_O_STAGING_HEAP_WRITE_H

#include "o_staging_types.h"

bool O_StagingHeapWriteTuple(Relation relation, ItemPointer tid, HeapTuple tuple, uint64_t csn,
                             bool deleted);

#endif /* OCC_OPT_O_STAGING_HEAP_WRITE_H */
