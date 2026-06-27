//
// Epoch Timer orchestration: FIXED vs CATCHUP proposal + Jump rewrite.
//

#ifndef DYNAMIC_NET_DN_TIMER_HANDLER_H
#define DYNAMIC_NET_DN_TIMER_HANDLER_H

#include "dn_types.h"

#include <map>
#include <vector>

struct DnTimerTickInput {
    int32_t local_proposal_epoch;
    int32_t current_term;
    int32_t next_log_index;
    std::vector<DnTransaction> pending_txns;

    DnTimerTickInput()
        : local_proposal_epoch(0), current_term(0), next_log_index(0)
    {
    }
};

struct DnTimerTickResult {
    bool should_propose;
    DnProposalStrategy strategy;
    DnRaftEntry proposed_entry;
    int32_t next_local_epoch;

    /*
     * When CATCHUP Jump fires: log_index -> rewritten epoch for local uncommitted entries.
     */
    std::map<int32_t, int32_t> log_epoch_updates;

    DnTimerTickResult() : should_propose(false), next_local_epoch(0) {}
};

class DnTimerHandler {
public:
    /*
     * One Timer period: consult strategy, optionally rewrite send queue, build RaftEntry.
     * Does NOT send on wire — caller uses result with DnPrepareAppendForTarget().
     */
    static DnTimerTickResult OnTimerTick(const DnTimerTickInput &input);
};

#endif /* DYNAMIC_NET_DN_TIMER_HANDLER_H */
