#ifndef OPENGAUSS_E_CRDT_SHMEM_H
#define OPENGAUSS_E_CRDT_SHMEM_H

#include <cstdint>

#include "epoch/e_concurrent_map.h"
#include "epoch/e_crdt.h"
#include "epoch/e_txn_wire.h"

/* 单行 wire tuple 字段上限（CRDT shmem 行槽位；须覆盖 TPCC bmsql_customer UPDATE，实测 max≈740B） */
static constexpr uint32_t EPOCH_CRDT_MAX_PK = 128;
static constexpr uint32_t EPOCH_CRDT_MAX_RELNAME = 64;
static constexpr uint32_t EPOCH_CRDT_MAX_TUPLE = 1024;

/*
 * 997 分片（对齐 EpochConcurrent::UnorderedMap）：每分片独立锁 + 行/insert 槽位。
 * 容量 = ceil(slot总量/997) + skew 余量，吸收 hash 偏斜（内存换查找时间）。
 */
static constexpr uint32_t EPOCH_CRDT_NUM_SHARDS =
    static_cast<uint32_t>(EpochConcurrent::UnorderedMap<uint64_t, uint64_t>::NUM_SHARDS);
static constexpr uint32_t EPOCH_CRDT_ROW_SKEW_SLACK = 256;
static constexpr uint32_t EPOCH_CRDT_INSERT_SKEW_SLACK = 64;

struct EpochCrdtShmem;

extern EpochCrdtShmem* g_epochCrdtShmem;

extern Size EpochCrdtShmemSize(void);
extern void EpochCrdtShmemInit(void);
extern void EpochCrdtShmemClearEpoch(uint64_t epoch);
extern E_CrdtMergeOutcome EpochCrdtShmemMergeWireTxn(const E_WireTxn& txn);
/* merge 关窗后从 CRDT shmem 组装 apply 载荷；前置条件：txn 不在 abort_csn 且有写行 */
extern void EpochCrdtShmemBuildApplyTxn(const E_WireTxn& source, E_WireTxn* out);
extern bool EpochCrdtShmemHasRemoteRows(uint64_t epoch, uint32_t local_node);
extern bool EpochCrdtShmemHasLocalRows(uint64_t epoch, uint32_t local_node);
extern bool EpochCrdtShmemIsEmpty(uint64_t epoch);

#endif /* OPENGAUSS_E_CRDT_SHMEM_H */
