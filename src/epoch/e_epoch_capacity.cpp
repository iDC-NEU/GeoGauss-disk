#include "postgres.h"

#include "epoch/e_epoch_capacity.h"
#include "epoch/e_config.h"

static uint32_t EpochClampPositiveInt(int value, uint32_t fallback)
{
    if (value <= 0) {
        return fallback;
    }
    return static_cast<uint32_t>(value);
}

uint32_t EpochGetTxnQueueCapacity(void)
{
    return EpochClampPositiveInt(g_epoch_txn_queue_capacity, EPOCH_DEFAULT_TXN_QUEUE_CAPACITY);
}

uint32_t EpochGetAbortCsnPerEpoch(void)
{
    return EpochClampPositiveInt(g_epoch_abort_csn_per_epoch, EPOCH_DEFAULT_ABORT_CSN_PER_EPOCH);
}

uint32_t EpochGetCrdtSlotCount(void)
{
    return EpochClampPositiveInt(g_epoch_crdt_slot_count, EPOCH_DEFAULT_CRDT_SLOT_COUNT);
}

uint32_t EpochGetCrdtRowsPerSlot(void)
{
    return EpochClampPositiveInt(g_epoch_crdt_rows_per_slot, EPOCH_DEFAULT_CRDT_ROWS_PER_SLOT);
}

uint32_t EpochGetCrdtInsertsPerSlot(void)
{
    return EpochClampPositiveInt(g_epoch_crdt_inserts_per_slot, EPOCH_DEFAULT_CRDT_INSERTS_PER_SLOT);
}

uint32_t EpochGetMergeQueueCapacity(void)
{
    return EpochClampPositiveInt(g_epoch_merge_queue_capacity, EPOCH_DEFAULT_MERGE_QUEUE_CAPACITY);
}

uint32_t EpochGetMergeDoneCapacity(void)
{
    return EpochClampPositiveInt(g_epoch_merge_done_capacity, EPOCH_DEFAULT_MERGE_DONE_CAPACITY);
}

uint32_t EpochGetApplyQueueCapacity(void)
{
    return EpochClampPositiveInt(g_epoch_apply_queue_capacity, EPOCH_DEFAULT_APPLY_QUEUE_CAPACITY);
}

uint32_t EpochGetApplyDoneCapacity(void)
{
    return EpochClampPositiveInt(g_epoch_apply_done_capacity, EPOCH_DEFAULT_APPLY_DONE_CAPACITY);
}

uint32_t EpochGetWorkQueueSlotCount(void)
{
    return EpochGetCrdtSlotCount();
}
