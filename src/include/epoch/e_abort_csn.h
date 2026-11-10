#ifndef OPENGAUSS_E_ABORT_CSN_H
#define OPENGAUSS_E_ABORT_CSN_H

#include <cstdint>

#include "epoch/e_crdt.h"
#include "epoch/e_txn_wire.h"

struct EpochAbortCsnEntry {
    uint64_t csn;
    uint32_t server_id;
};

extern Size EpochAbortCsnShmemSize(void);
extern void EpochAbortCsnShmemInit(void);
extern void EpochAbortCsnClear(uint64_t epochId);
extern bool EpochAbortCsnInsert(uint64_t epochId, uint64_t csn, uint32_t serverId);
extern bool EpochAbortCsnContains(uint64_t epochId, uint64_t csn, uint32_t serverId);
/* 该时间片被判输的事务数（一个 csn = 一个事务）；须在 EpochAbortCsnClear 之前取 */
extern uint64_t EpochAbortCsnCount(uint64_t epochId);

#endif /* OPENGAUSS_E_ABORT_CSN_H */
