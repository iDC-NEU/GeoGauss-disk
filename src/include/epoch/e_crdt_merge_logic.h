#ifndef OPENGAUSS_E_CRDT_MERGE_LOGIC_H
#define OPENGAUSS_E_CRDT_MERGE_LOGIC_H

#include <cstdint>

/*
 * 同 epoch CRDT 槽内 FFW：existing.csn > incoming.csn → incoming 胜；
 * CSN 相等时 server_id（node id）较小者胜。
 * cen 区分 bootstrap（槽无行 / cen != txn.epoch）与槽内竞争；bootstrap 只要求 heap 上行存在。
 */

/* a 相对 b 胜出（幂等：同一 (csn,sid) 视为 a 胜） */
static inline bool EpochCrdtTxnWinsOver(uint64_t csn_a, uint32_t sid_a, uint64_t csn_b, uint32_t sid_b)
{
    if (csn_a == csn_b && sid_a == sid_b) {
        return true;
    }
    if (csn_a < csn_b) {
        return true;
    }
    if (csn_a > csn_b) {
        return false;
    }
    return sid_a < sid_b;
}

/* incoming T 相对槽内 existing 行胜出 */
static inline bool EpochCrdtIncomingTxnWins(uint64_t row_csn, uint32_t row_sid, uint64_t t_csn, uint32_t t_sid)
{
    return EpochCrdtTxnWinsOver(t_csn, t_sid, row_csn, row_sid);
}

#endif /* OPENGAUSS_E_CRDT_MERGE_LOGIC_H */
