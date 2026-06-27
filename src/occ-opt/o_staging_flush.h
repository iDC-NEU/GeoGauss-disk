//
// Deferred write-back: flush staged versions to heap via pluggable callbacks.
//

#ifndef OCC_OPT_O_STAGING_FLUSH_H
#define OCC_OPT_O_STAGING_FLUSH_H

#include "o_staging_types.h"

#include <mutex>

class O_StagingFlush
{
public:
    static O_StagingFlush &Instance();

    void SetCallbacks(const O_StagingFlushCallbacks &callbacks);

    /*
     * Flush all staged tuples on one page.
     * Returns number of tuples successfully flushed.
     * On partial failure, successfully flushed entries are removed; failed ones remain staged.
     */
    size_t FlushPage(Oid relid, BlockNumber block);

    /*
     * Flush every page that currently has staged entries.
     */
    size_t FlushAll();

    /*
     * Default no-op write callback for framework bring-up (always succeeds).
     */
    static bool DefaultNoOpWrite(Relation relation, ItemPointer tid, HeapTuple tuple,
                                 uint64_t csn, bool deleted);

private:
    O_StagingFlush();

    O_StagingFlushCallbacks m_callbacks;
    std::mutex m_callbackMutex;
};

#endif /* OCC_OPT_O_STAGING_FLUSH_H */
