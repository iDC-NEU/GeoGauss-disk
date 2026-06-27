//
// Pluggable transport callbacks — wire DnNetworkModule to real ZMQ/libcomm later.
//

#ifndef DYNAMIC_NET_DN_TRANSPORT_H
#define DYNAMIC_NET_DN_TRANSPORT_H

#include "dn_network_module.h"
#include "dn_types.h"

#include <deque>
#include <functional>
#include <mutex>
#include <string>

using DnSendWireFn = std::function<void(int target_node_id, const DnNetworkPacket &pkt, bool is_reply)>;

class DnTransport {
public:
    static DnTransport &Instance();

    void SetSendFn(DnSendWireFn fn);

    /*
     * Drain ready outgoing packets from virtual network module and invoke send callback.
     */
    void FlushOutgoing(DnNetworkModule *net);

    /*
     * Pull from wire (receive hook) then dispatch all ready incoming packets.
     */
    void PollIncoming(DnNetworkModule *net);

    void SetReceiveHook(std::function<void(DnNetworkModule *net)> hook);

    /* Opaque epoch wire blobs not consumed by dispatch (E_WireTxn path). */
    void PushPendingWirePayload(std::string payload);
    bool PopPendingWirePayload(std::string *out);

private:
    DnTransport();

    DnSendWireFn m_sendFn;
    std::function<void(DnNetworkModule *net)> m_receiveHook;

    std::mutex m_pendingWireMutex;
    std::deque<std::string> m_pendingWire;
};

#endif /* DYNAMIC_NET_DN_TRANSPORT_H */
