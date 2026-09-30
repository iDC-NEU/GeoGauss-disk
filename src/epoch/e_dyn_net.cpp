#include "postgres.h"

#include "epoch/e_dyn_net.h"
#include "epoch/e_config.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_statistician.h"
#include "epoch/e_types.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_wire_envelope.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_pipeline.h"

#include "dn_api.h"
#include "dn_hash.h"
#include "dn_proxy_merge.h"
#include "dn_snapshot_validator.h"
#include "dn_transport.h"
#include "dn_wire_codec.h"

#include <deque>
#include <mutex>
#include <string>
#include <vector>

#include "miscadmin.h"

extern bool EpochNetPublishDirect(const std::string& payload);
extern bool EpochNetPublishTo(uint32_t target_node, const std::string& payload);

static bool g_dyn_net_active = false;

static void EpochDynNetBuildConfig(DnNetworkConfig* cfg)
{
    if (cfg == nullptr) {
        return;
    }
    cfg->node_count = static_cast<int>(EpochGetTotalNodes());
    cfg->local_node_id = static_cast<int>(EpochGetNodeId());
    cfg->epoch_duration_ms = 10;
    cfg->lag_tolerance = g_epoch_dyn_net_lag_tolerance;
    cfg->queue_size_tolerance = g_epoch_dyn_net_queue_tolerance;
    cfg->catchup_factor_k = g_epoch_dyn_net_catchup_k;
    cfg->rtt_estimate_ms = g_epoch_dyn_net_rtt_ms;
    cfg->enable_proxy = g_epoch_dyn_net_proxy;

    if (g_epoch_dyn_net_catchup) {
        cfg->advancement = DnEpochAdvancement::CATCHUP;
    } else {
        cfg->advancement = DnEpochAdvancement::FIXED;
    }

    cfg->slow_node_ids.clear();
    if (g_epoch_dyn_net_slow_nodes != nullptr && g_epoch_dyn_net_slow_nodes[0] != '\0') {
        char* raw = pstrdup(g_epoch_dyn_net_slow_nodes);
        char* tok = strtok(raw, ",");
        while (tok != nullptr) {
            cfg->slow_node_ids.push_back(atoi(tok));
            tok = strtok(nullptr, ",");
        }
        pfree(raw);
    }

    switch (g_epoch_dyn_net_bw_mode) {
        case EPOCH_DYN_NET_BW_UP:
            cfg->bandwidth_mode = DnBandwidthLimitMode::UP_LIMIT;
            break;
        case EPOCH_DYN_NET_BW_DOWN:
            cfg->bandwidth_mode = DnBandwidthLimitMode::DOWN_LIMIT;
            break;
        case EPOCH_DYN_NET_BW_BI:
            cfg->bandwidth_mode = DnBandwidthLimitMode::BI_LIMIT;
            break;
        default:
            cfg->bandwidth_mode = DnBandwidthLimitMode::UNLIMIT;
            break;
    }
    cfg->upload_bandwidth_bps = g_epoch_dyn_net_upload_bps;
    cfg->download_bandwidth_bps = g_epoch_dyn_net_download_bps;
}

bool EpochDynNetIsEnabled(void)
{
    return g_enable_epoch_dyn_net && g_dyn_net_active;
}

bool EpochDynNetProxyEnabled(void)
{
    return EpochDynNetIsEnabled() && g_epoch_dyn_net_proxy;
}

bool EpochDynNetIsProxySlowNode(void)
{
    if (!EpochDynNetProxyEnabled()) {
        return false;
    }
    return DnIsConfiguredSlow(DnRuntime::Instance().GetConfig(), static_cast<int>(EpochGetNodeId()));
}

static void EpochDynNetTryEmitProxySnapshot(int32_t epoch)
{
    if (!EpochDynNetProxyEnabled()) {
        return;
    }
    DnRuntime& rt = DnRuntime::Instance();
    if (!DnIsFastNode(rt.GetConfig(), rt.GetConfig().local_node_id)) {
        return;
    }

    (void)rt.TryFastPathMerge(epoch, nullptr);
    DnPartialSnapshotMessage snap = rt.BuildProxySnapshot(epoch);
    if (snap.contributions.empty()) {
        return;
    }

    DnNetworkModule* net = rt.Net();
    if (net == nullptr) {
        return;
    }

    for (const auto& contrib : snap.contributions) {
        uint64_t origin_hash = 0;
        for (const auto& t : contrib.committed_txns) {
            origin_hash = DnCombineHashes(origin_hash, DnHashTxn(t));
        }
        for (uint64_t dh : contrib.dropped_hashes) {
            origin_hash = DnCombineHashes(origin_hash, dh);
        }

        E_WireTxn meta_txn;
        meta_txn.csn = 0;
        meta_txn.epoch = static_cast<uint64_t>(epoch);
        meta_txn.server_id = static_cast<uint32_t>(contrib.origin_node_id);

        std::string meta_wire;
        int32_t payload_size =
            static_cast<int32_t>(contrib.committed_txns.size() + contrib.dropped_hashes.size());
        if (!EpochWireSerializeMetadata(meta_txn, origin_hash, payload_size, &meta_wire)) {
            continue;
        }

        DnNetworkPacket meta_pkt;
        meta_pkt.kind = DnMsgKind::WIRE_PAYLOAD;
        meta_pkt.from_node_id = rt.GetConfig().local_node_id;
        meta_pkt.wire_payload = meta_wire;
        for (int slow_id : rt.GetConfig().slow_node_ids) {
            if (slow_id == rt.GetConfig().local_node_id) {
                continue;
            }
            net->SendMessage(meta_pkt, slow_id, false);
        }
    }

    DnNetworkPacket pkt;
    pkt.kind = DnMsgKind::PARTIAL_SNAPSHOT;
    pkt.partial_snapshot = snap;
    pkt.from_node_id = rt.GetConfig().local_node_id;

    for (int slow_id : rt.GetConfig().slow_node_ids) {
        if (slow_id == rt.GetConfig().local_node_id) {
            continue;
        }
        net->SendMessage(pkt, slow_id, false);
    }
    EpochDynNetFlushOutgoing();
}

void EpochDynNetInit(void)
{
    if (!g_enable_epoch_dyn_net || EpochGetTotalNodes() <= 1) {
        return;
    }

    DnNetworkConfig cfg;
    EpochDynNetBuildConfig(&cfg);
    DnInit(&cfg);

    DnRuntime& rt = DnRuntime::Instance();

    rt.SetPeerLogSyncFn([](int32_t epoch, int32_t origin_node_id, int32_t commit_index, size_t txn_count) {
        (void)commit_index;
        if (txn_count == 0) {
            return;
        }
        uint64_t ep = static_cast<uint64_t>(epoch);
        uint64_t origin = static_cast<uint64_t>(origin_node_id);
        uint64_t self = static_cast<uint64_t>(EpochGetNodeId());
        E_NodeManager::UpdateStoreReceivedPackCounters(ep, origin, txn_count);
    });

    rt.SetDispatchSideEffectFn([](const DnIncomingPacket& in, const DnDispatchResult& dr) {
        if (dr.snapshot_applied) {
            for (const auto& dn_txn : dr.applied_txns) {
                E_WireTxn wt = EpochDynNetDnTxnToWire(dn_txn, dr.snapshot_epoch);
                EpochPipelineInjectRemoteWireTxn(wt);
            }
        }
        if (dr.handled && in.packet.kind == DnMsgKind::APPEND_ENTRIES &&
            in.packet.append_req.IsMetadataOnly() && EpochDynNetIsProxySlowNode()) {
            const DnAppendEntriesRequest& req = in.packet.append_req;
            E_WireTxn pseudo;
            pseudo.csn = 0;
            pseudo.epoch = static_cast<uint64_t>(req.epoch);
            pseudo.server_id = static_cast<uint32_t>(req.channel_id);
            EpochPipelineInjectRemoteWireTxn(pseudo);
        }
        if (dr.need_try_fast_merge && dr.fast_merge_epoch > 0) {
            EpochDynNetTryEmitProxySnapshot(static_cast<uint64_t>(dr.fast_merge_epoch));
        }
    });

    DnTransport::Instance().SetReceiveHook([](DnNetworkModule* net) {
        (void)net;
        /* ZMQ listen thread feeds EnqueueIncoming; hook reserved for pull-model transports. */
    });

    DnTransport::Instance().SetSendFn([](int target_node_id, const DnNetworkPacket& pkt, bool is_reply) {
        (void)is_reply;
        std::string wire;
        if (pkt.kind == DnMsgKind::WIRE_PAYLOAD && !pkt.wire_payload.empty()) {
            wire = pkt.wire_payload;
        } else if (!DnWireSerializePacket(pkt, &wire)) {
            return;
        }
        if (wire.empty()) {
            return;
        }
        if (target_node_id >= 0) {
            (void)EpochNetPublishTo(static_cast<uint32_t>(target_node_id), wire);
        } else {
            (void)EpochNetPublishDirect(wire);
        }
    });

    g_dyn_net_active = true;
}

void EpochDynNetEnqueueIncoming(const std::string& payload, int from_node, size_t raw_bytes)
{
    if (!EpochDynNetIsEnabled()) {
        return;
    }
    DnNetworkModule* net = DnRuntime::Instance().Net();
    if (net == nullptr) {
        return;
    }

    DnNetworkPacket pkt;
    if (DnWireTryDecodePayload(payload.data(), payload.size(), from_node, &pkt)) {
        pkt.from_node_id = from_node;
    } else {
        pkt.kind = DnMsgKind::WIRE_PAYLOAD;
        pkt.wire_payload = payload;
        pkt.from_node_id = from_node;
    }

    long long now_ms = 0;
    struct timeval tv;
    if (gettimeofday(&tv, nullptr) == 0) {
        now_ms = static_cast<long long>(tv.tv_sec) * 1000LL + tv.tv_usec / 1000;
    }

    int latency = g_epoch_dyn_net_rtt_ms / 2;
    if (from_node >= 0 && from_node < static_cast<int>(EpochGetTotalNodes())) {
        latency = g_epoch_dyn_net_rtt_ms;
    }
    net->EnqueueIncoming(pkt, from_node, raw_bytes, now_ms + latency);
}

void EpochDynNetShutdown(void)
{
    if (!g_dyn_net_active) {
        return;
    }
    DnShutdown();
    g_dyn_net_active = false;
}

bool EpochDynNetPublish(const std::string& payload)
{
    if (!EpochDynNetIsEnabled()) {
        return EpochNetPublishDirect(payload);
    }

    DnNetworkModule* net = DnRuntime::Instance().Net();
    if (net == nullptr) {
        return EpochNetPublishDirect(payload);
    }

    DnNetworkPacket pkt;
    pkt.kind = DnMsgKind::WIRE_PAYLOAD;
    pkt.wire_payload = payload;
    pkt.from_node_id = static_cast<int32_t>(EpochGetNodeId());

    /* PUB/SUB 广播：target 用 0 占位 */
    net->SendMessage(pkt, 0, false);
    EpochDynNetFlushOutgoing();
    return true;
}

void EpochDynNetRecordWireForProxy(const E_WireTxn& txn, int32_t channel_id)
{
    if (!EpochDynNetProxyEnabled()) {
        return;
    }
    DnRuntime& rt = DnRuntime::Instance();
    const DnNetworkConfig& cfg = rt.GetConfig();
    if (!DnIsFastNode(cfg, cfg.local_node_id) || rt.PartialRegistry() == nullptr) {
        return;
    }
    DnRaftEntry entry = EpochDynNetWireToRaftEntry(txn);
    rt.PartialRegistry()->AppendFastNodeEntry(static_cast<int32_t>(txn.epoch), channel_id,
                                              entry);
}

bool EpochDynNetPublishWireTxn(const std::string& wire, const E_WireTxn& txn)
{
    if (!EpochDynNetProxyEnabled()) {
        return EpochDynNetPublish(wire);
    }

    DnRuntime& rt = DnRuntime::Instance();
    const DnNetworkConfig& cfg = rt.GetConfig();
    int self = cfg.local_node_id;
    (void)self;
    EpochDynNetRecordWireForProxy(txn, self);

    DnNetworkModule* net = rt.Net();
    if (net == nullptr) {
        return EpochNetPublishDirect(wire);
    }

    uint32_t total = EpochGetTotalNodes();
    for (uint32_t peer = 0; peer < total; ++peer) {
        if (static_cast<int>(peer) == self) {
            continue;
        }
        if (DnProxyMerge::ShouldSendMetadataOnly(cfg, static_cast<int>(peer))) {
            continue;
        }
        DnNetworkPacket pkt;
        pkt.kind = DnMsgKind::WIRE_PAYLOAD;
        pkt.from_node_id = self;
        pkt.wire_payload = wire;
        net->SendMessage(pkt, static_cast<int>(peer), false);
    }
    EpochDynNetFlushOutgoing();
    EpochDynNetTryEmitProxySnapshot(static_cast<int32_t>(txn.epoch));
    return true;
}

void EpochDynNetFlushOutgoing(void)
{
    if (!EpochDynNetIsEnabled()) {
        return;
    }
    DnNetworkModule* net = DnRuntime::Instance().Net();
    if (net != nullptr) {
        DnTransport::Instance().FlushOutgoing(net);
    }
}

void EpochDynNetDrainAndDispatch(void)
{
    if (!EpochDynNetIsEnabled()) {
        return;
    }
    DnNetworkModule* net = DnRuntime::Instance().Net();
    if (net != nullptr) {
        DnTransport::Instance().PollIncoming(net);
    }
}

bool EpochDynNetTryReceive(std::string* payload)
{
    if (payload == nullptr) {
        return false;
    }
    if (!EpochDynNetIsEnabled()) {
        return false;
    }

    EpochDynNetDrainAndDispatch();
    return DnTransport::Instance().PopPendingWirePayload(payload);
}

bool EpochDynNetHandleWireEnvelope(const std::string& wire)
{
    if (!EpochDynNetIsEnabled()) {
        return false;
    }

    DnNetworkModule* net = DnRuntime::Instance().Net();
    if (net == nullptr) {
        return false;
    }

    DnNetworkPacket pkt;
    if (!DnWireTryDecodePayload(wire.data(), wire.size(), -1, &pkt)) {
        return false;
    }

    net->EnqueueIncoming(pkt, pkt.from_node_id >= 0 ? pkt.from_node_id : 0, wire.size(), 0);
    DnTransport::Instance().PollIncoming(net);
    return true;
}

void EpochDynNetObserveWireTxn(const E_WireTxn& txn)
{
    if (!EpochDynNetIsEnabled()) {
        return;
    }
    DnEpochObserver* observer = DnRuntime::Instance().Observer();
    if (observer == nullptr) {
        return;
    }

    if (txn.epoch > 0) {
        observer->RecordObservedEpochDirect(static_cast<int32_t>(txn.epoch));
    }
}

void EpochDynNetObserveEpochEnd(uint64_t epoch)
{
    if (!EpochDynNetIsEnabled() || epoch == 0) {
        return;
    }
    DnEpochObserver* observer = DnRuntime::Instance().Observer();
    if (observer != nullptr) {
        observer->RecordObservedEpochDirect(static_cast<int32_t>(epoch));
    }
    if (EpochDynNetProxyEnabled()) {
        EpochDynNetTryEmitProxySnapshot(static_cast<int32_t>(epoch));
    }
}

void EpochDynNetMaybeCatchup(void)
{
    if (!EpochDynNetIsEnabled() || !g_epoch_dyn_net_catchup) {
        return;
    }

    DnNetworkConfig cfg = DnRuntime::Instance().GetConfig();
    if (!DnIsConfiguredSlow(cfg, cfg.local_node_id)) {
        return;
    }

    DnRuntime& rt = DnRuntime::Instance();
    DnNetworkModule* net = rt.Net();
    if (net == nullptr) {
        return;
    }

    uint64_t local_phys = E_NodeManager::GetPhysicalEpoch();
    DnProposalStrategy strategy =
        rt.ConsultProposal(static_cast<int32_t>(local_phys));

    if (strategy.action != DnStrategyAction::JUMP || strategy.jump_delta <= 0) {
        return;
    }

    (void)rt.ExecuteCatchupJump(strategy.target_epoch);
    E_NodeManager::SetPhysicalEpoch(static_cast<uint64_t>(strategy.target_epoch));
}

void EpochDynNetMaybeEmitProxySnapshot(uint64_t epoch)
{
    EpochDynNetTryEmitProxySnapshot(static_cast<int32_t>(epoch));
}

void EpochDynNetGcBeforeEpoch(uint64_t epoch)
{
    if (!EpochDynNetIsEnabled() || epoch == 0) {
        return;
    }
    DnRuntime::Instance().GcEpochState(static_cast<int32_t>(epoch));
}
