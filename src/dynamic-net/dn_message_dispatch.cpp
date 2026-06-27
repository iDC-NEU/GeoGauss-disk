#include "dn_message_dispatch.h"
#include "dn_api.h"
#include "dn_config.h"
#include "dn_slow_node_apply.h"

namespace {

int DnMajority(int node_count)
{
    return node_count / 2 + 1;
}

}  // namespace

DnDispatchResult DnMessageDispatch::Dispatch(const DnIncomingPacket &pkt)
{
    switch (pkt.packet.kind) {
        case DnMsgKind::APPEND_ENTRIES:
            return HandleAppend(pkt.from_node_id, pkt.packet.append_req);
        case DnMsgKind::COMMIT_NOTIFY:
            return HandleCommitNotify(pkt.from_node_id, pkt.packet.commit_notify, nullptr);
        case DnMsgKind::FAST_HASH:
            return HandleFastHash(pkt.from_node_id, pkt.packet.fast_hash);
        case DnMsgKind::PARTIAL_SNAPSHOT:
            return HandlePartialSnapshot(pkt.from_node_id, pkt.packet.partial_snapshot);
        case DnMsgKind::SNAPSHOT_ACK:
            return HandleSnapshotAck(pkt.from_node_id, pkt.packet.snapshot_ack);
        default:
            return DnDispatchResult{};
    }
}

DnDispatchResult DnMessageDispatch::HandleAppend(int from_node, const DnAppendEntriesRequest &req)
{
    DnDispatchResult result;
    DnRuntime &rt = DnRuntime::Instance();
    rt.OnAppendReceived(from_node, req);
    result.handled = true;

    if (!req.entries.empty() && DnIsFastNode(rt.GetConfig(), rt.GetConfig().local_node_id)) {
        const DnRaftEntry &entry = req.entries.front();
        if (rt.GetConfig().enable_proxy && rt.PartialRegistry() != nullptr) {
            rt.PartialRegistry()->RecordFastNodeEntry(entry.epoch, from_node, entry);
            result.need_try_fast_merge = true;
            result.fast_merge_epoch = entry.epoch;
        }
    }
    return result;
}

DnDispatchResult DnMessageDispatch::HandleCommitNotify(int from_node, const DnCommitNotify &notify,
                                                         const DnRaftEntry *committed_entry)
{
    (void)from_node;
    DnDispatchResult result;
    result.handled = true;
    result.need_advance_global_epoch = true;

    DnRuntime &rt = DnRuntime::Instance();
    if (committed_entry != nullptr && rt.GetConfig().enable_proxy &&
        DnIsFastNode(rt.GetConfig(), rt.GetConfig().local_node_id) && rt.PartialRegistry() != nullptr) {
        rt.PartialRegistry()->RecordFastNodeEntry(committed_entry->epoch, notify.channel_id, *committed_entry);
        result.need_try_fast_merge = true;
        result.fast_merge_epoch = committed_entry->epoch;
    }
    return result;
}

DnDispatchResult DnMessageDispatch::HandleFastHash(int from_node, const DnFastHashMessage &msg)
{
    DnDispatchResult result;
    DnRuntime &rt = DnRuntime::Instance();
    if (rt.PartialRegistry() == nullptr) {
        return result;
    }

    rt.PartialRegistry()->RecordFastHash(msg.epoch, from_node, msg.hash,
                                         DnMajority(rt.GetConfig().node_count));
    result.handled = true;

    if (rt.PartialRegistry()->IsHashQuorumReached(msg.epoch)) {
        result.need_try_fast_merge = true;
        result.fast_merge_epoch = msg.epoch;
    }
    return result;
}

DnDispatchResult DnMessageDispatch::HandlePartialSnapshot(int from_node, const DnPartialSnapshotMessage &msg)
{
    (void)from_node;
    DnDispatchResult result;
    DnRuntime &rt = DnRuntime::Instance();

    if (!DnIsConfiguredSlow(rt.GetConfig(), rt.GetConfig().local_node_id)) {
        return result;
    }

    std::vector<DnTransaction> all_txns;
    result.snapshot_validation = rt.ValidateSnapshot(msg, &all_txns);
    result.handled = true;
    result.snapshot_epoch = msg.epoch;

    if (result.snapshot_validation == DnSnapshotValidationResult::OK) {
        DnSlowNodeApplyResult apply_result =
            DnSlowNodeApply::ApplySnapshot(msg, all_txns, rt.PeerLogsMutable());
        if (apply_result.applied) {
            result.snapshot_applied = true;
            result.applied_txns = all_txns;
            rt.NotifyPeerLogApplied(msg, apply_result);
        }
    }
    return result;
}

DnDispatchResult DnMessageDispatch::HandleSnapshotAck(int from_node, const DnSnapshotAckMessage &msg)
{
    (void)from_node;
    (void)msg;
    DnDispatchResult result;
    result.handled = true;
    result.need_advance_global_epoch = true;
    return result;
}
