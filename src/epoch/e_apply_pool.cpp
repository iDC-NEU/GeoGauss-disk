#include "postgres.h"

#include "epoch/e_apply_pool.h"
#include "epoch/e_apply_shmem.h"

void EpochApplyPoolInit(void)
{
    EpochApplyQueueInit();
}

void EpochApplyPoolShutdown(void)
{
    EpochApplyQueueRequestShutdown();
}
