#include "postgres.h"

#include "epoch/e_net.h"
#include "epoch/e_config.h"
#include "epoch/e_statistician.h"
#include "epoch/e_types.h"
#include "epoch/e_txn_wire.h"

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>

#include "neu_concurrency_tools/blockingconcurrentqueue.h"

#ifdef EPOCH_USE_ZMQ
#include <zmq.hpp>
#include <mutex>
#endif

static std::atomic<bool> g_net_running{false};
static std::atomic<bool> g_peer_connected[EPOCH_MAX_NODES];
static moodycamel::BlockingConcurrentQueue<std::string> g_incoming_queue;

bool EpochPeerIsConnected(uint32_t nodeId)
{
    if (nodeId >= EPOCH_MAX_NODES) {
        return false;
    }
    return g_peer_connected[nodeId].load(std::memory_order_acquire);
}

void EpochMarkPeerConnected(uint32_t nodeId)
{
    if (nodeId >= EPOCH_MAX_NODES) {
        return;
    }
    bool was = g_peer_connected[nodeId].load(std::memory_order_acquire);
    g_peer_connected[nodeId].store(true, std::memory_order_release);
    if (!was && nodeId != EpochGetNodeId()) {
        EpochOnPeerConnected(nodeId);
    }
}

bool EpochAllPeersConnected(void)
{
    uint32_t total = EpochGetTotalNodes();
    uint32_t self = EpochGetNodeId();
    for (uint32_t node = 0; node < total; ++node) {
        if (node != self && !EpochPeerIsConnected(node)) {
            return false;
        }
    }
    return true;
}

#ifdef EPOCH_USE_ZMQ
static std::unique_ptr<zmq::context_t> g_zmq_ctx;
static std::unique_ptr<zmq::socket_t> g_zmq_pub;
static std::mutex g_zmq_pub_mutex;
static std::vector<std::unique_ptr<zmq::socket_t>> g_zmq_subs;
static std::thread g_listen_thread;

static int EpochNodePort(uint32_t node_id)
{
    return g_epoch_data_port_base + static_cast<int>(node_id);
}

static void EpochListenLoop(void)
{
    zmq::pollitem_t items[16];
    int item_count = 0;
    for (size_t i = 0; i < g_zmq_subs.size() && item_count < 16; ++i) {
        items[item_count].socket = static_cast<void*>(*g_zmq_subs[i]);
        items[item_count].fd = 0;
        items[item_count].events = ZMQ_POLLIN;
        items[item_count].revents = 0;
        item_count++;
    }

    while (g_net_running.load(std::memory_order_acquire)) {
        if (item_count == 0) {
            pg_usleep(1000);
            continue;
        }
        (void)zmq::poll(items, item_count, 1);
        for (int i = 0; i < item_count; ++i) {
            if ((items[i].revents & ZMQ_POLLIN) == 0) {
                continue;
            }
            zmq::message_t msg;
            if (!g_zmq_subs[i]->recv(&msg, ZMQ_DONTWAIT)) {
                continue;
            }
            if (msg.size() < sizeof(uint32_t)) {
                continue;
            }
            const char* data = static_cast<const char*>(msg.data());
            uint32_t len = 0;
            memcpy(&len, data, sizeof(uint32_t));
            if (len + sizeof(uint32_t) > msg.size()) {
                continue;
            }
            std::string payload(data + sizeof(uint32_t), len);
            g_incoming_queue.enqueue(std::move(payload));
        }
    }
}
#endif

static bool EpochNetBuildHelloPayload(std::string* out)
{
    if (out == nullptr) {
        return false;
    }
    E_WireTxn hello;
    hello.csn = 0;
    hello.start_epoch = 0;
    hello.commit_epoch = 0;
    hello.server_id = EpochGetNodeId();
    hello.txn_state = EPOCH_TXN_EMPTY;
    return E_WireSerializeTxn(hello, out);
}

void EpochNetMaybePublishHello(void)
{
    if (!g_net_running.load(std::memory_order_acquire) || EpochGetTotalNodes() <= 1) {
        return;
    }
    if (EpochAllPeersConnected()) {
        return;
    }
    /*
     * epoch worker 约 10ms 一轮；每 50 轮 (~500ms) 重发一次 hello。
     * 覆盖：node0 先起、node1 后起时 init 阶段 hello 已被丢弃的场景。
     */
    static int tick = 0;
    if (++tick % 50 != 0) {
        return;
    }
    std::string hello_payload;
    if (EpochNetBuildHelloPayload(&hello_payload)) {
        (void)EpochNetPublish(hello_payload);
        EpochRecvDiagLogHello("SEND_HELLO", EpochGetNodeId());
    }
}

bool EpochNetInit(void)
{
    if (g_net_running.load()) {
        return true;
    }
    if (EpochGetTotalNodes() <= 1) {
        return true;
    }

#ifdef EPOCH_USE_ZMQ
    try {
        g_zmq_ctx = std::make_unique<zmq::context_t>(1);
        g_zmq_pub = std::make_unique<zmq::socket_t>(*g_zmq_ctx, ZMQ_PUB);
        int hwm = 0;
        g_zmq_pub->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm));
        std::string bind_addr = "tcp://*:" + std::to_string(EpochNodePort(EpochGetNodeId()));
        g_zmq_pub->bind(bind_addr);

        if (g_epoch_peer_hosts != nullptr && g_epoch_peer_hosts[0] != '\0') {
            char* raw = pstrdup(g_epoch_peer_hosts);
            char* tok = strtok(raw, ",");
            uint32_t peer_id = 0;
            while (tok != nullptr) {
                if (peer_id != EpochGetNodeId()) {
                    auto sub = std::make_unique<zmq::socket_t>(*g_zmq_ctx, ZMQ_SUB);
                    sub->setsockopt(ZMQ_RCVHWM, &hwm, sizeof(hwm));
                    sub->setsockopt(ZMQ_SUBSCRIBE, "", 0);
                    std::string connect_addr =
                        std::string("tcp://") + tok + ":" + std::to_string(EpochNodePort(peer_id));
                    sub->connect(connect_addr);
                    g_zmq_subs.push_back(std::move(sub));
                }
                peer_id++;
                tok = strtok(nullptr, ",");
            }
            pfree(raw);
        }

        g_net_running.store(true, std::memory_order_release);
        EpochMarkPeerConnected(EpochGetNodeId());
        g_listen_thread = std::thread(EpochListenLoop);
        /* ZMQ PUB/SUB slow-joiner：等待对端 SUB 就绪 */
        pg_usleep(500000L);
        std::string hello_payload;
        if (EpochNetBuildHelloPayload(&hello_payload)) {
            (void)EpochNetPublish(hello_payload);
            EpochRecvDiagLogHello("SEND_HELLO", EpochGetNodeId());
        }
        return true;
    } catch (const zmq::error_t& e) {
        ereport(WARNING, (errmsg("epoch: ZMQ init failed: %s", e.what())));
        return false;
    }
#else
    ereport(WARNING, (errmsg("epoch: built without ZMQ (EPOCH_USE_ZMQ), multi-node networking disabled")));
    return false;
#endif
}

void EpochNetShutdown(void)
{
    if (!g_net_running.exchange(false)) {
        return;
    }
#ifdef EPOCH_USE_ZMQ
    if (g_listen_thread.joinable()) {
        g_listen_thread.join();
    }
    g_zmq_subs.clear();
    g_zmq_pub.reset();
    g_zmq_ctx.reset();
#endif
}

bool EpochNetPublish(const std::string& payload)
{
    if (EpochGetTotalNodes() <= 1) {
        return true;
    }
#ifdef EPOCH_USE_ZMQ
    if (g_zmq_pub == nullptr) {
        return false;
    }
    try {
        uint32_t len = static_cast<uint32_t>(payload.size());
        zmq::message_t msg(sizeof(uint32_t) + payload.size());
        memcpy(msg.data(), &len, sizeof(uint32_t));
        if (!payload.empty()) {
            memcpy(static_cast<char*>(msg.data()) + sizeof(uint32_t), payload.data(), payload.size());
        }
        std::lock_guard<std::mutex> lock(g_zmq_pub_mutex);
        return g_zmq_pub->send(msg);
    } catch (const zmq::error_t& e) {
        ereport(WARNING, (errmsg("epoch: ZMQ publish failed: %s", e.what())));
        return false;
    }
#else
    (void)payload;
    return false;
#endif
}

bool EpochNetTryReceive(std::string* payload)
{
    if (payload == nullptr) {
        return false;
    }
    return g_incoming_queue.try_dequeue(*payload);
}

