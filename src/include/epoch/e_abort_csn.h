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
extern void EpochAbortCsnInsert(uint64_t epochId, uint64_t csn, uint32_t serverId);
extern bool EpochAbortCsnContains(uint64_t epochId, uint64_t csn, uint32_t serverId);
extern void EpochAbortCsnRecordMergeOutcome(uint64_t epochId, const E_WireTxn& txn, const E_CrdtMergeOutcome& outcome);

#endif /* OPENGAUSS_E_ABORT_CSN_H */
