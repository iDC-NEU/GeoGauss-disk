//
// Slow-node log reconstruction after PartialSnapshot validation.
//

#ifndef DYNAMIC_NET_DN_SLOW_NODE_APPLY_H
#define DYNAMIC_NET_DN_SLOW_NODE_APPLY_H

#include "dn_peer_log.h"
#include "dn_types.h"

#include <unordered_map>
#include <vector>

struct DnSlowNodeApplyResult {
    bool applied;
    int32_t epoch;
    int32_t channels_updated;

    DnSlowNodeApplyResult() : applied(false), epoch(0), channels_updated(0) {}
};

class DnSlowNodeApply {
public:
    /*
     * Reconstruct per-channel log entries from validated snapshot contributions.
     * Framework: writes merged txns into logs[origin].entries at matching epoch slot.
     */
    static DnSlowNodeApplyResult ApplySnapshot(const DnPartialSnapshotMessage &snap,
                                               const std::vector<DnTransaction> &all_txns,
                                               std::unordered_map<int32_t, DnPeerLogState> *logs);
};

#endif /* DYNAMIC_NET_DN_SLOW_NODE_APPLY_H */
