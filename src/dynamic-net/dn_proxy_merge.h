//
// Proxy merge: build PartialSnapshot from fast-node merged state (dynamic-net).
//

#ifndef DYNAMIC_NET_DN_PROXY_MERGE_H
#define DYNAMIC_NET_DN_PROXY_MERGE_H

#include "dn_config.h"
#include "dn_partial_epoch_state.h"
#include "dn_types.h"

#include <vector>

class DnProxyMerge {
public:
    /*
     * Build PartialSnapshotMessage from merged PartialEpochState.
     */
    static DnPartialSnapshotMessage BuildPartialSnapshot(int32_t proxy_node_id,
                                                         const DnPartialEpochState &pes);

    /*
     * Decide AppendEntries mode for a target node.
     * Fast nodes receive full payload; slow nodes receive metadata-only when proxy enabled.
     */
    static bool ShouldSendMetadataOnly(const DnNetworkConfig &cfg, int target_node_id);

    /*
     * Populate metadata fields on AppendEntries for slow-node path.
     */
    static void FillMetadataAppend(DnAppendEntriesRequest *req, const DnRaftEntry &entry);
};

#endif /* DYNAMIC_NET_DN_PROXY_MERGE_H */
