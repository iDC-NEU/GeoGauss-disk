#include "dn_append_builder.h"

std::vector<DnAppendPlan> DnAppendBuilder::BuildBroadcastPlans(const DnNetworkConfig &cfg, int32_t channel_id,
                                                               int32_t channel_leader_id, int32_t term,
                                                               int32_t leader_commit, const DnRaftEntry &entry)
{
    std::vector<DnAppendPlan> plans;
    for (int i = 0; i < cfg.node_count; ++i) {
        if (i == cfg.local_node_id) {
            continue;
        }
        plans.push_back(
            BuildSinglePlan(cfg, i, channel_id, channel_leader_id, term, leader_commit, entry));
    }
    return plans;
}

DnAppendPlan DnAppendBuilder::BuildSinglePlan(const DnNetworkConfig &cfg, int32_t target_node_id,
                                              int32_t channel_id, int32_t channel_leader_id, int32_t term,
                                              int32_t leader_commit, const DnRaftEntry &entry)
{
    DnAppendPlan plan;
    plan.target_node_id = target_node_id;
    plan.request.term = term;
    plan.request.channel_id = channel_id;
    plan.request.channel_leader_id = channel_leader_id;
    plan.request.leader_commit = leader_commit;
    plan.request.epoch = entry.epoch;
    plan.is_metadata_only = DnProxyMerge::ShouldSendMetadataOnly(cfg, target_node_id);

    if (plan.is_metadata_only) {
        DnProxyMerge::FillMetadataAppend(&plan.request, entry);
    } else {
        plan.request.entries.push_back(entry);
    }
    return plan;
}
