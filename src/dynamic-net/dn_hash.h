//
// Deterministic hashing helpers for metadata / snapshot validation.
//

#ifndef DYNAMIC_NET_DN_HASH_H
#define DYNAMIC_NET_DN_HASH_H

#include "dn_types.h"

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

uint64_t DnHashTxn(const DnTransaction &txn);

uint64_t DnHashMergedTxns(const std::vector<DnTransaction> &txns);

uint64_t DnHashRaftEntry(const DnRaftEntry &entry);

uint64_t DnCombineHashes(uint64_t a, uint64_t b);

#endif /* DYNAMIC_NET_DN_HASH_H */
