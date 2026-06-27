//
// OCC optimization path — version staging hash table module (occ-opt).
//
// Components:
//   o_staging_table       Version Staging Hash Table (sharded)
//   o_dirty_page_tracker  Page-level dirty tuple counting
//   o_staging_flush       Deferred write-back with pluggable callbacks
//   o_background_flusher  Periodic / threshold-triggered flush scheduler
//   o_staging_api         Unified facade for future occ/ integration
//   o_delta_table         Legacy wrapper (include/occ/o_delta_table.h)
//

#ifndef OCC_OPT_OCC_OPT_H
#define OCC_OPT_OCC_OPT_H

#include "o_staging_types.h"
#include "o_staging_api.h"
#include "o_staging_table.h"
#include "o_dirty_page_tracker.h"
#include "o_staging_flush.h"
#include "o_background_flusher.h"
#include "o_staging_heap_write.h"

#endif /* OCC_OPT_OCC_OPT_H */
