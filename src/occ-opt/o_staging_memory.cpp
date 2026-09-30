//
// Memory helpers for version staging hash table.
//

#include "o_staging_memory.h"
#include "utils/memutils.h"

#include <mutex>

namespace {

MemoryContext g_occ_staging_mcxt = nullptr;
std::mutex g_staging_mcxt_mutex;

}  // namespace

MemoryContext O_StagingGetMemoryContext()
{
    if (g_occ_staging_mcxt == nullptr) {
        g_occ_staging_mcxt = AllocSetContextCreate(
            TopMemoryContext,
            "OCC Version Staging Table",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    }
    return g_occ_staging_mcxt;
}

bool O_StagingTupleLengthValid(HeapTuple tuple)
{
    if (!HeapTupleIsValid(tuple) || tuple->t_data == nullptr) {
        return false;
    }
    Size len = tuple->t_len;
    return len > 0 && len <= MaxHeapTupleSize;
}

HeapTuple O_StagingCopyToPersistent(HeapTuple tuple)
{
    if (!O_StagingTupleLengthValid(tuple)) {
        return nullptr;
    }

    /*
     * Staging MemoryContext is shared across apply/backend threads in-process.
     * All AllocSet ops on it must hold g_staging_mcxt_mutex.
     */
    std::lock_guard<std::mutex> lock(g_staging_mcxt_mutex);
    MemoryContext old = MemoryContextSwitchTo(O_StagingGetMemoryContext());
    HeapTuple copy = heap_copytuple(tuple);
    MemoryContextSwitchTo(old);
    return copy;
}

void O_StagingFreePersistent(HeapTuple tuple)
{
    if (tuple == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(g_staging_mcxt_mutex);
    MemoryContext old = MemoryContextSwitchTo(O_StagingGetMemoryContext());
    heap_freetuple(tuple);
    MemoryContextSwitchTo(old);
}

HeapTuple O_StagingCopyToCaller(HeapTuple tuple)
{
    if (!O_StagingTupleLengthValid(tuple)) {
        return nullptr;
    }
    /* CurrentMemoryContext: callers may heap_freetuple without staging mcxt lock. */
    return heap_copytuple(tuple);
}
