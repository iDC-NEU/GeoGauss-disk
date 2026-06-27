//
// Incoming message dispatch (framework — no ZMQ dependency).
//

#ifndef DYNAMIC_NET_DN_MESSAGE_DISPATCH_H
#define DYNAMIC_NET_DN_MESSAGE_DISPATCH_H

#include "dn_network_module.h"
#include "dn_snapshot_validator.h"
#include "dn_types.h"

#include <vector>

struct DnDispatchResult {
    bool handled;
    bool need_advance_global_epoch;
    bool need_try_fast_merge;
    int32_t fast_merge_epoch;
    bool need_build_proxy;
    int32_t proxy_epoch;
    DnSnapshotValidationResult snapshot_validation;
    bool snapshot_applied;
    int32_t snapshot_epoch;
    std::vector<DnTransaction> applied_txns;

    DnDispatchResult()
        : handled(false),
          need_advance_global_epoch(false),
          need_try_fast_merge(false),
          fast_merge_epoch(0),
          need_build_proxy(false),
          proxy_epoch(0),
          snapshot_validation(DnSnapshotValidationResult::OK),
          snapshot_applied(false),
          snapshot_epoch(0)
    {
    }
};

class DnMessageDispatch {
public:
    static DnDispatchResult Dispatch(const DnIncomingPacket &pkt);

    static DnDispatchResult HandleAppend(int from_node, const DnAppendEntriesRequest &req);
    static DnDispatchResult HandleCommitNotify(int from_node, const DnCommitNotify &notify,
                                               const DnRaftEntry *committed_entry);
    static DnDispatchResult HandleFastHash(int from_node, const DnFastHashMessage &msg);
    static DnDispatchResult HandlePartialSnapshot(int from_node, const DnPartialSnapshotMessage &msg);
    static DnDispatchResult HandleSnapshotAck(int from_node, const DnSnapshotAckMessage &msg);
};

#endif /* DYNAMIC_NET_DN_MESSAGE_DISPATCH_H */
