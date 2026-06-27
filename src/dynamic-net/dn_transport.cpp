#include "dn_transport.h"

#include "dn_api.h"
#include "dn_message_dispatch.h"
#include "dn_wire_codec.h"

DnTransport &DnTransport::Instance()
{
    static DnTransport instance;
    return instance;
}

DnTransport::DnTransport() {}

void DnTransport::SetSendFn(DnSendWireFn fn)
{
    m_sendFn = std::move(fn);
}

void DnTransport::SetReceiveHook(std::function<void(DnNetworkModule *net)> hook)
{
    m_receiveHook = std::move(hook);
}

void DnTransport::FlushOutgoing(DnNetworkModule *net)
{
    if (net == nullptr || !m_sendFn) {
        return;
    }
    std::vector<DnOutgoingPacket> ready = net->PopReadyOutgoingPackets();
    for (const auto &out : ready) {
        m_sendFn(out.target_node_id, out.packet, out.is_reply);
    }
}

void DnTransport::PushPendingWirePayload(std::string payload)
{
    std::lock_guard<std::mutex> lock(m_pendingWireMutex);
    m_pendingWire.push_back(std::move(payload));
}

bool DnTransport::PopPendingWirePayload(std::string *out)
{
    if (out == nullptr) {
        return false;
    }
    std::lock_guard<std::mutex> lock(m_pendingWireMutex);
    if (m_pendingWire.empty()) {
        return false;
    }
    *out = std::move(m_pendingWire.front());
    m_pendingWire.pop_front();
    return true;
}

void DnTransport::PollIncoming(DnNetworkModule *net)
{
    if (net == nullptr) {
        return;
    }

    if (m_receiveHook) {
        m_receiveHook(net);
    }

    DnRuntime &rt = DnRuntime::Instance();
    std::vector<DnIncomingPacket> ready = net->PopReadyIncomingPackets();
    for (const auto &in : ready) {
        DnIncomingPacket work = in;
        DnNetworkPacket &pkt = work.packet;

        if (pkt.kind == DnMsgKind::WIRE_PAYLOAD && !pkt.wire_payload.empty()) {
            DnNetworkPacket decoded;
            if (DnWireTryDecodePayload(pkt.wire_payload.data(), pkt.wire_payload.size(),
                                       in.from_node_id, &decoded)) {
                work.packet = decoded;
                work.packet.from_node_id = in.from_node_id;
                pkt = work.packet;
            }
        }

        if (pkt.kind == DnMsgKind::WIRE_PAYLOAD) {
            PushPendingWirePayload(pkt.wire_payload);
            continue;
        }

        DnDispatchResult dr = DnMessageDispatch::Dispatch(work);
        rt.ProcessDispatchFollowUp(net, dr);

        if (rt.GetDispatchSideEffectFn()) {
            rt.GetDispatchSideEffectFn()(work, dr);
        }
    }

    FlushOutgoing(net);
}
