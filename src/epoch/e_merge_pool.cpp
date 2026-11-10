#include "postgres.h"

#include "epoch/e_merge_pool.h"
#include "epoch/e_merge_shmem.h"
#include "epoch/e_abort_csn.h"
#include "epoch/e_crdt_shmem.h"

#include "utils/elog.h"

void EpochMergePoolInit(void)
{
    /* CRDT / abort-csn 的共享内存是 merge 的前置条件 */
    EpochAbortCsnShmemInit();
    EpochCrdtShmemInit();
    EpochMergeQueueInit();
}

void EpochMergePoolShutdown(void)
{
}

bool EpochMergePoolSubmit(const E_WireTxn& txn)
{
    return EpochMergeQueueEnqueueTxn(txn);
}
