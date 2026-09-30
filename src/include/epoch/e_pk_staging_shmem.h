#ifndef OPENGAUSS_E_PK_STAGING_SHMEM_H
#define OPENGAUSS_E_PK_STAGING_SHMEM_H

#include "postgres.h"

#include "storage/item/itemptr.h"

#include <cstdint>

/*
 * Cross-process PK → root TID directory for epoch apply.
 * Speeds INSERT existence checks and remote locate; tip content stays in tip staging.
 * Key = (relid, pk bytes); same encoding as E_BuildPkKey / CRDT (max 128).
 */
static constexpr uint32_t EPOCH_PK_STAGING_MAX_PK = 128;
/*
 * 50 warehouses × 100k items 的 stock 热行远大于 262144。表满后 upsert 会覆盖
 * 探测起点，下一次远端 locate 未命中就退回 bmsql_stock 索引扫描（单行可到上百毫秒）。
 * 2M 条约 ~320MB，让一轮 TPCC 的 PK→TID 留在哈希里。
 */
static constexpr uint32_t EPOCH_PK_STAGING_NUM_ENTRIES = 2097152;
static constexpr uint32_t EPOCH_PK_STAGING_NUM_SHARDS = 997;
static constexpr uint32_t EPOCH_PK_STAGING_SKEW_SLACK = 64;

extern Size EpochPkStagingShmemSize(void);
extern void EpochPkStagingShmemInit(void);

/* Register or refresh PK → TID. overwritten on upsert. */
extern bool EpochPkStagingUpsert(Oid relid, const char* pk, uint16_t pk_len, ItemPointer tid);

/* Tombstone PK (row deleted from tip); Lookup reports found+deleted. */
extern bool EpochPkStagingMarkDeleted(Oid relid, const char* pk, uint16_t pk_len);

/*
 * true if entry exists.
 * deleted=true → PK known gone; out_tid may still hold last tid.
 */
extern bool EpochPkStagingLookup(Oid relid, const char* pk, uint16_t pk_len, ItemPointerData* out_tid,
    bool* deleted);

#endif /* OPENGAUSS_E_PK_STAGING_SHMEM_H */
