//
// Virtual network module implementation.
//

#include "dn_network_module.h"

#include <algorithm>
#include <chrono>
#include <cmath>

namespace {

long long DefaultNowMs()
{
    using clock = std::chrono::steady_clock;
    return static_cast<long long>(
        std::chrono::duration_cast<std::chrono::milliseconds>(clock::now().time_since_epoch()).count());
}

size_t DefaultPacketSize(const DnNetworkPacket &pkt)
{
    size_t size = 64;
    if (pkt.kind == DnMsgKind::WIRE_PAYLOAD) {
        return pkt.wire_payload.size() + 64;
    } else if (pkt.kind == DnMsgKind::APPEND_ENTRIES) {
        for (const auto &entry : pkt.append_req.entries) {
            size += 32;
            for (const auto &txn : entry.txns) {
                size += 16 + txn.client_key.size();
                for (const auto &row : txn.rows) {
                    size += row.key.size() + row.value.size() + 8;
                }
            }
        }
        size += static_cast<size_t>(pkt.append_req.payload_size);
    } else if (pkt.kind == DnMsgKind::PARTIAL_SNAPSHOT) {
        for (const auto &c : pkt.partial_snapshot.contributions) {
            for (const auto &txn : c.committed_txns) {
                size += 16 + txn.client_key.size();
            }
            size += c.dropped_hashes.size() * 8;
        }
    }
    return size;
}

}  // namespace

DnNetworkModule::DnNetworkModule(int node_id, DnTrafficShaper *shaper, const DnNetworkConfig *config)
    : m_nodeId(node_id),
      m_shaper(shaper),
      m_config(config),
      m_nowFn(DefaultNowMs),
      m_sizeFn(DefaultPacketSize),
      m_droppedTx(0),
      m_droppedRx(0)
{
}

void DnNetworkModule::SetClock(DnNowMsFn now_fn)
{
    if (now_fn) {
        m_nowFn = std::move(now_fn);
    }
}

void DnNetworkModule::SetPacketSizeFn(DnPacketSizeFn size_fn)
{
    if (size_fn) {
        m_sizeFn = std::move(size_fn);
    }
}

long long DnNetworkModule::NowMs() const
{
    return m_nowFn ? m_nowFn() : DefaultNowMs();
}

size_t DnNetworkModule::PacketSize(const DnNetworkPacket &pkt) const
{
    return m_sizeFn ? m_sizeFn(pkt) : DefaultPacketSize(pkt);
}

bool DnNetworkModule::IsHighPriority(const DnNetworkPacket &pkt) const
{
    switch (pkt.kind) {
        case DnMsgKind::APPEND_ENTRIES:
            return pkt.append_req.entries.empty();
        case DnMsgKind::APPEND_RESPONSE:
        case DnMsgKind::COMMIT_NOTIFY:
        case DnMsgKind::FAST_HASH:
        case DnMsgKind::SNAPSHOT_ACK:
            return true;
        case DnMsgKind::PARTIAL_SNAPSHOT:
        case DnMsgKind::WIRE_PAYLOAD:
            return false;
        default:
            return true;
    }
}

bool DnNetworkModule::IsDroppable(const DnNetworkPacket &pkt) const
{
    (void)pkt;
    return false;
}

void DnNetworkModule::SendMessage(const DnNetworkPacket &pkt, int target_node_id, bool is_reply)
{
    if (target_node_id < 0 && !is_reply) {
        return;
    }

    std::lock_guard<std::mutex> lock(m_mutex);
    long long now = NowMs();
    bool high = IsHighPriority(pkt);
    size_t size_bytes = PacketSize(pkt);

    bool limit_tx = m_shaper != nullptr && m_shaper->ShouldLimitTx();
    long long tx_cost = 0;
    if (limit_tx && m_shaper != nullptr) {
        tx_cost = m_shaper->CalculateTxCost(size_bytes, high);
    }

    long long start_time = now;
    if (m_shaper != nullptr) {
        start_time = std::max(now, m_shaper->GetLastFinishTime(high));
    }
    long long finish_time = start_time + tx_cost;

    if (limit_tx && m_config != nullptr && IsDroppable(pkt)) {
        long long queue_ahead = start_time - now;
        long long max_ahead =
            high ? m_config->max_ctrl_queue_ahead_ms : m_config->max_tx_queue_ahead_ms;
        if (queue_ahead > max_ahead) {
            m_droppedTx++;
            return;
        }
    }

    if (m_shaper != nullptr) {
        m_shaper->UpdateBusyState(finish_time, high);
    }

    DnOutgoingPacket out;
    out.release_time_ms = finish_time;
    out.packet = pkt;
    out.target_node_id = target_node_id;
    out.is_reply = is_reply;

    if (high) {
        m_controlQueue.push_back(std::move(out));
    } else {
        m_dataQueue.push_back(std::move(out));
    }
}

std::vector<DnOutgoingPacket> DnNetworkModule::PopReadyOutgoingPackets()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    std::vector<DnOutgoingPacket> ready;
    long long now = NowMs();

    auto pop_from = [&](std::deque<DnOutgoingPacket> &q) {
        while (!q.empty() && q.front().release_time_ms <= now) {
            ready.push_back(std::move(q.front()));
            q.pop_front();
        }
    };

    pop_from(m_controlQueue);
    pop_from(m_dataQueue);
    return ready;
}

std::map<int32_t, int32_t> DnNetworkModule::PredictiveEpochRewrite(int32_t base_epoch)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    std::map<int32_t, int32_t> updates;
    if (m_config == nullptr || m_config->epoch_duration_ms <= 0) {
        return updates;
    }

    long long now = NowMs();
    for (auto &pkt : m_dataQueue) {
        if (pkt.packet.kind != DnMsgKind::APPEND_ENTRIES) {
            continue;
        }
        DnAppendEntriesRequest &req = pkt.packet.append_req;
        if (req.entries.empty()) {
            continue;
        }

        long long queue_delay = pkt.release_time_ms - now;
        if (queue_delay < 0) {
            queue_delay = 0;
        }
        int epoch_offset =
            static_cast<int>(std::ceil(static_cast<double>(queue_delay) / m_config->epoch_duration_ms));
        int new_epoch = base_epoch + epoch_offset;
        req.epoch = new_epoch;

        for (auto &entry : req.entries) {
            entry.epoch = new_epoch;
            if (entry.index > 0) {
                updates[entry.index] = new_epoch;
            }
        }
    }
    return updates;
}

size_t DnNetworkModule::GetTxQueueSize() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_dataQueue.size();
}

size_t DnNetworkModule::GetRxQueueSize() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_incomingData.size();
}

long long DnNetworkModule::GetDroppedTx() const
{
    return m_droppedTx;
}

long long DnNetworkModule::GetDroppedRx() const
{
    return m_droppedRx;
}

void DnNetworkModule::EnqueueIncoming(const DnNetworkPacket &pkt, int from_node_id, size_t raw_size_bytes,
                                      long long arrival_at_nic_ms)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    bool high = IsHighPriority(pkt);
    long long deliver_time = arrival_at_nic_ms;

    if (m_shaper != nullptr && m_shaper->ShouldLimitRx()) {
        long long rx_ahead = m_shaper->GetRxQueueAheadMs(arrival_at_nic_ms, high);
        long long max_rx = m_config != nullptr ? m_config->max_rx_queue_ahead_ms : 800;
        if (rx_ahead > max_rx && IsDroppable(pkt)) {
            m_droppedRx++;
            return;
        }
        deliver_time = m_shaper->CalculateRxFinishTime(arrival_at_nic_ms, raw_size_bytes, high);
    }

    DnIncomingPacket in;
    in.deliver_at_ms = deliver_time;
    in.packet = pkt;
    in.from_node_id = from_node_id;
    in.is_high_priority = high;

    if (high) {
        m_incomingCtrl.push(in);
    } else {
        m_incomingData.push(in);
    }
}

std::vector<DnIncomingPacket> DnNetworkModule::PopReadyIncomingPackets()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    std::vector<DnIncomingPacket> ready;
    long long now = NowMs();

    while (true) {
        bool ctrl_ready = !m_incomingCtrl.empty() && m_incomingCtrl.top().deliver_at_ms <= now;
        bool data_ready = !m_incomingData.empty() && m_incomingData.top().deliver_at_ms <= now;
        if (!ctrl_ready && !data_ready) {
            break;
        }

        bool take_ctrl = ctrl_ready;
        if (ctrl_ready && data_ready) {
            take_ctrl = true;
        }

        if (take_ctrl) {
            ready.push_back(m_incomingCtrl.top());
            m_incomingCtrl.pop();
        } else {
            ready.push_back(m_incomingData.top());
            m_incomingData.pop();
        }
    }
    return ready;
}
