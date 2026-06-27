#ifndef OPENGAUSS_E_ABORT_CSN_H
#define OPENGAUSS_E_ABORT_CSN_H

#include <atomic>
#include <cstdint>

#include "epoch/e_types.h"

/* GeoGauss MOTAdaptor::abort_transcation_csn_set，按 epoch 槽位存放 merge 输家 CSN */
static constexpr uint32_t EPOCH_ABORT_CSNS_PER_SLOT = 1024;

struct EpochAbortCsnEntry {
    std::atomic<uint64_t> csn;
    std::atomic<uint32_t> server_id;
};

struct EpochAbortCsnSlot {
    std::atomic<uint32_t> count;
    EpochAbortCsnEntry entries[EPOCH_ABORT_CSNS_PER_SLOT];
};

struct EpochAbortCsnBank {
    EpochAbortCsnSlot slots[EPOCH_DEFAULT_CACHE_SLOTS];
};

void EpochAbortCsnBankInit(EpochAbortCsnBank* bank);
void EpochAbortCsnClear(uint64_t epochId);
void EpochAbortCsnInsert(uint64_t epochId, uint64_t csn, uint32_t serverId);
bool EpochAbortCsnContains(uint64_t epochId, uint64_t csn, uint32_t serverId);

#endif /* OPENGAUSS_E_ABORT_CSN_H */
