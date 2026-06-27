//
// Real heap write-back callbacks — call O_heap_* from occ/ module.
// Not registered by default; O_StagingInit uses no-op callbacks until integration.
//

#include "o_staging_heap_write.h"
#include "occ/o_heapam.h"

#include "access/xact.h"
#include "utils/rel.h"

bool O_StagingHeapWriteTuple(Relation relation, ItemPointer tid, HeapTuple tuple, uint64_t csn,
                             bool deleted)
{
    if (!RelationIsValid(relation) || tid == nullptr) {
        return false;
    }

    CommandId cid = GetCurrentCommandId(false);

    if (deleted) {
        TM_Result rc = O_heap_delete(relation, tid, cid, csn);
        return rc == TM_Ok;
    }

    if (tuple == nullptr || tuple->t_data == nullptr) {
        return false;
    }

    tuple->t_data->m_rowCSN = csn;
    tuple->t_data->is_latest = true;

    TM_Result rc = O_heap_update(relation, tid, tuple, cid, csn, nullptr, nullptr);
    return rc == TM_Ok;
}
