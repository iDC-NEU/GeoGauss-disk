//
// Prepare outgoing AppendEntries for fast vs slow targets (proxy path).
//

#ifndef DYNAMIC_NET_DN_APPEND_BUILDER_H
#define DYNAMIC_NET_DN_APPEND_BUILDER_H

#include "dn_config.h"
#include "dn_proxy_merge.h"
#include "dn_types.h"

#include <vector>

struct DnAppendPlan {
    int32_t target_node_id;
    DnAppendEntriesRequest request;
    bool is_metadata_only;

    DnAppendPlan() : target_node_id(-1), is_metadata_only(false) {}
};

class DnAppendBuilder {
public:
    /*
     * Given a local RaftEntry and channel context, build per-target AppendEntries.
     * Slow nodes receive metadata-only when proxy is enabled; fast nodes receive full payload.
     */
    static std::vector<DnAppendPlan> BuildBroadcastPlans(const DnNetworkConfig &cfg, int32_t channel_id,
                                                         int32_t channel_leader_id, int32_t term,
                                                         int32_t leader_commit, const DnRaftEntry &entry);

    static DnAppendPlan BuildSinglePlan(const DnNetworkConfig &cfg, int32_t target_node_id, int32_t channel_id,
                                        int32_t channel_leader_id, int32_t term, int32_t leader_commit,
                                        const DnRaftEntry &entry);
};

#endif /* DYNAMIC_NET_DN_APPEND_BUILDER_H */
