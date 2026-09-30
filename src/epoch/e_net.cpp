#include "postgres.h"

#include "epoch/e_net.h"
#include "epoch/e_config.h"
#include "epoch/e_log_path.h"
#include "epoch/e_statistician.h"
#include "epoch/e_types.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_dyn_net.h"
#include "epoch/e_perf.h"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <mutex>
#include <thread>
#include <vector>

#include <dirent.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <linux/sockios.h>

#ifndef SIOCOUTQNSD
#define SIOCOUTQNSD 0x894B
#endif

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
static std::unique_ptr<zmq::socket_t> g_zmq_pull;
static std::vector<std::unique_ptr<zmq::socket_t>> g_zmq_pushs;
static std::vector<std::unique_ptr<zmq::socket_t>> g_zmq_subs;
static std::thread g_listen_thread;

static constexpr int EPOCH_DIRECT_PORT_OFFSET = 5000;

static int EpochNodePort(uint32_t node_id)
{
    return g_epoch_data_port_base + static_cast<int>(node_id);
}

static int EpochNodeDirectPort(uint32_t node_id)
{
    return g_epoch_data_port_base + EPOCH_DIRECT_PORT_OFFSET + static_cast<int>(node_id);
}

static void EpochListenLoop(void)
{
    EpochSetAuxThreadName("epochlisten");
    zmq::pollitem_t items[32];
    int item_count = 0;
    for (size_t i = 0; i < g_zmq_subs.size() && item_count < 31; ++i) {
        items[item_count].socket = static_cast<void*>(*g_zmq_subs[i]);
        items[item_count].fd = 0;
        items[item_count].events = ZMQ_POLLIN;
        items[item_count].revents = 0;
        item_count++;
    }
    if (g_zmq_pull != nullptr && item_count < 32) {
        items[item_count].socket = static_cast<void*>(*g_zmq_pull);
        items[item_count].fd = 0;
        items[item_count].events = ZMQ_POLLIN;
        items[item_count].revents = 0;
        item_count++;
    }
    const int sub_count = static_cast<int>(g_zmq_subs.size());
    const int pull_index = sub_count;

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
            zmq::socket_t* sock = nullptr;
            if (i < sub_count) {
                sock = g_zmq_subs[i].get();
            } else if (i == pull_index && g_zmq_pull != nullptr) {
                sock = g_zmq_pull.get();
            }
            if (sock == nullptr || !sock->recv(&msg, ZMQ_DONTWAIT)) {
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
            int from_node = static_cast<int>(i);
            if (i == pull_index) {
                if (msg.size() < sizeof(uint32_t) * 2 + len) {
                    continue;
                }
                uint32_t sender = 0;
                memcpy(&sender, data + sizeof(uint32_t) + len, sizeof(uint32_t));
                from_node = static_cast<int>(sender);
            }
            if (EpochDynNetIsEnabled()) {
                EpochDynNetEnqueueIncoming(payload, from_node, payload.size());
            } else {
                g_incoming_queue.enqueue(std::move(payload));
            }
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
    hello.epoch = 0;
    hello.server_id = EpochGetNodeId();
    hello.txn_state = EPOCH_TXN_EMPTY;
    return E_WireSerializeTxn(hello, out);
}

void EpochNetPublishHello(void)
{
    if (!g_net_running.load(std::memory_order_acquire) || EpochGetTotalNodes() <= 1) {
        return;
    }
    std::string hello_payload;
    if (EpochNetBuildHelloPayload(&hello_payload)) {
        (void)EpochNetPublish(hello_payload);
        EpochPerfNoteHelloSend();
        EpochRecvDiagLogHello("SEND_HELLO", EpochGetNodeId());
    }
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
    EpochNetPublishHello();
}

bool EpochNetIsReady(void)
{
    return g_net_running.load(std::memory_order_acquire);
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

        if (g_epoch_dyn_net_proxy) {
            g_zmq_pull = std::make_unique<zmq::socket_t>(*g_zmq_ctx, ZMQ_PULL);
            g_zmq_pull->setsockopt(ZMQ_RCVHWM, &hwm, sizeof(hwm));
            std::string pull_addr =
                "tcp://*:" + std::to_string(EpochNodeDirectPort(EpochGetNodeId()));
            g_zmq_pull->bind(pull_addr);
            g_zmq_pushs.clear();
            g_zmq_pushs.resize(EPOCH_MAX_NODES);
        }

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

                    if (g_epoch_dyn_net_proxy) {
                        auto push = std::make_unique<zmq::socket_t>(*g_zmq_ctx, ZMQ_PUSH);
                        push->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm));
                        std::string push_addr = std::string("tcp://") + tok + ":" +
                                                std::to_string(EpochNodeDirectPort(peer_id));
                        push->connect(push_addr);
                        if (peer_id < EPOCH_MAX_NODES) {
                            g_zmq_pushs[peer_id] = std::move(push);
                        }
                    }
                }
                peer_id++;
                tok = strtok(nullptr, ",");
            }
            pfree(raw);
        } else if (g_epoch_dyn_net_proxy) {
            uint32_t total = EpochGetTotalNodes();
            uint32_t self = EpochGetNodeId();
            for (uint32_t peer_id = 0; peer_id < total; ++peer_id) {
                if (peer_id == self || peer_id >= EPOCH_MAX_NODES) {
                    continue;
                }
                auto push = std::make_unique<zmq::socket_t>(*g_zmq_ctx, ZMQ_PUSH);
                push->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm));
                std::string push_addr =
                    "tcp://127.0.0.1:" + std::to_string(EpochNodeDirectPort(peer_id));
                push->connect(push_addr);
                g_zmq_pushs[peer_id] = std::move(push);
            }
        }

        g_net_running.store(true, std::memory_order_release);
        EpochMarkPeerConnected(EpochGetNodeId());
        g_listen_thread = std::thread(EpochListenLoop);
        EpochDynNetInit();
        /* ZMQ PUB/SUB slow-joiner：等待对端 SUB 就绪 */
        pg_usleep(500000L);
        EpochNetPublishHello();
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
    EpochDynNetShutdown();
#ifdef EPOCH_USE_ZMQ
    if (g_listen_thread.joinable()) {
        g_listen_thread.join();
    }
    g_zmq_subs.clear();
    g_zmq_pushs.clear();
    g_zmq_pull.reset();
    g_zmq_pub.reset();
    g_zmq_ctx.reset();
#endif
}

#ifdef EPOCH_USE_ZMQ
static int64_t EpochMonoUs(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<int64_t>(ts.tv_sec) * 1000000 + static_cast<int64_t>(ts.tv_nsec) / 1000;
}

static constexpr int EPOCH_UPLINK_FD_MAX = 8;
static constexpr int64_t EPOCH_UPLINK_BUDGET_HOLD_US = 200;
static int g_uplink_fds[EPOCH_UPLINK_FD_MAX];
static int g_uplink_fd_count = 0;
static int64_t g_uplink_fd_scan_us = 0;
static uint64_t g_uplink_handed = 0;
static bool g_uplink_budget_hold = false;
static int64_t g_uplink_budget_hold_us = 0;
static bool g_uplink_congested_logged = false;
static std::atomic<bool> g_uplink_defer{false};
static std::atomic<bool> g_uplink_congested{false};

static bool EpochParseProcPort(const char* token, unsigned* port_out)
{
    const char* colon = strrchr(token, ':');
    if (colon == nullptr || colon[1] == '\0' || port_out == nullptr) {
        return false;
    }
    char* end = nullptr;
    unsigned long port = strtoul(colon + 1, &end, 16);
    if (end == colon + 1) {
        return false;
    }
    *port_out = static_cast<unsigned>(port);
    return true;
}

static void EpochCollectPubInodes(int port, unsigned long* inodes, int* inode_count)
{
    const char* paths[2] = {"/proc/net/tcp", "/proc/net/tcp6"};
    *inode_count = 0;
    for (int p = 0; p < 2; ++p) {
        FILE* fp = fopen(paths[p], "r");
        if (fp == nullptr) {
            continue;
        }
        char line[512];
        if (fgets(line, sizeof(line), fp) == nullptr) {
            fclose(fp);
            continue;
        }
        while (fgets(line, sizeof(line), fp) != nullptr && *inode_count < 32) {
            char local[80];
            char remote[80];
            unsigned state = 0;
            unsigned long inode = 0;
            int sl = 0;
            int n = sscanf(line,
                "%d: %79s %79s %x %*s %*s %*s %*u %*u %lu",
                &sl,
                local,
                remote,
                &state,
                &inode);
            unsigned lport = 0;
            if (n != 5 || state != 0x01 || !EpochParseProcPort(local, &lport)) {
                continue;
            }
            if (static_cast<int>(lport) != port) {
                continue;
            }
            inodes[*inode_count] = inode;
            (*inode_count)++;
        }
        fclose(fp);
    }
}

static void EpochNetRescanPubFds(void)
{
    g_uplink_fd_count = 0;
    unsigned long inodes[32];
    int inode_count = 0;
    EpochCollectPubInodes(EpochNodePort(EpochGetNodeId()), inodes, &inode_count);
    if (inode_count == 0) {
        return;
    }
    DIR* dir = opendir("/proc/self/fd");
    if (dir == nullptr) {
        return;
    }
    struct dirent* de;
    while ((de = readdir(dir)) != nullptr && g_uplink_fd_count < EPOCH_UPLINK_FD_MAX) {
        if (de->d_name[0] < '0' || de->d_name[0] > '9') {
            continue;
        }
        int fd = atoi(de->d_name);
        char path[64];
        int rc = snprintf_s(path, sizeof(path), sizeof(path) - 1, "/proc/self/fd/%d", fd);
        securec_check_ss(rc, "\0", "\0");
        char link[80];
        ssize_t nread = readlink(path, link, sizeof(link) - 1);
        if (nread <= 0) {
            continue;
        }
        link[nread] = '\0';
        unsigned long inode = 0;
        if (sscanf(link, "socket:[%lu]", &inode) != 1) {
            continue;
        }
        for (int i = 0; i < inode_count; ++i) {
            if (inodes[i] == inode) {
                g_uplink_fds[g_uplink_fd_count++] = fd;
                break;
            }
        }
    }
    closedir(dir);
}

static size_t EpochNetReadUnsent(void)
{
    size_t max_unsent = 0;
    bool any = false;
    for (int i = 0; i < g_uplink_fd_count; ++i) {
        int val = 0;
        if (ioctl(g_uplink_fds[i], SIOCOUTQNSD, &val) == 0 && val >= 0) {
            any = true;
            if (static_cast<size_t>(val) > max_unsent) {
                max_unsent = static_cast<size_t>(val);
            }
        }
    }
    if (!any) {
        g_uplink_fd_count = 0;
    }
    return max_unsent;
}
#endif

void EpochNetPollUplink(void)
{
#ifdef EPOCH_USE_ZMQ
    if (!g_enable_epoch_multimaster || !g_epoch_dyn_net_catchup || EpochGetTotalNodes() <= 1) {
        g_uplink_defer.store(false, std::memory_order_relaxed);
        g_uplink_congested.store(false, std::memory_order_relaxed);
        g_uplink_handed = 0;
        g_uplink_budget_hold = false;
        return;
    }
    int64_t now = EpochMonoUs();
    bool need_scan = g_uplink_fd_count == 0 ? (now - g_uplink_fd_scan_us >= 100000)
                                            : (now - g_uplink_fd_scan_us >= 1000000);
    if (need_scan) {
        EpochNetRescanPubFds();
        g_uplink_fd_scan_us = now;
    }
    size_t unsent = EpochNetReadUnsent();
    bool kernel_high = unsent >= EPOCH_UPLINK_HIGH_BYTES;
    bool kernel_low = unsent <= EPOCH_UPLINK_LOW_BYTES;

    if (kernel_high) {
        if (!g_uplink_congested.load(std::memory_order_relaxed) && !g_uplink_congested_logged) {
            g_uplink_congested_logged = true;
            ereport(LOG,
                (errmsg("epoch pacer: uplink congested, tcp unsent=%zu", unsent)));
        }
        g_uplink_congested.store(true, std::memory_order_relaxed);
        return;
    }
    if (kernel_low) {
        g_uplink_congested.store(false, std::memory_order_relaxed);
        g_uplink_handed = 0;
    }
#endif
}

bool EpochNetDeferPayload(void)
{
#ifdef EPOCH_USE_ZMQ
    return g_uplink_defer.load(std::memory_order_relaxed);
#else
    return false;
#endif
}

bool EpochNetUplinkCongested(void)
{
#ifdef EPOCH_USE_ZMQ
    return g_uplink_congested.load(std::memory_order_relaxed);
#else
    return false;
#endif
}

void EpochNetNotePayloadSent(size_t nbytes)
{
#ifdef EPOCH_USE_ZMQ
    g_uplink_handed += nbytes;
#else
    (void)nbytes;
#endif
}

bool EpochNetPublishDirect(const std::string& payload)
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

bool EpochNetPublishTo(uint32_t target_node, const std::string& payload)
{
    if (EpochGetTotalNodes() <= 1 || target_node >= EpochGetTotalNodes()) {
        return true;
    }
#ifdef EPOCH_USE_ZMQ
    uint32_t self = EpochGetNodeId();
    if (target_node == self) {
        return true;
    }
    if (target_node >= g_zmq_pushs.size() || g_zmq_pushs[target_node] == nullptr) {
        return EpochNetPublishDirect(payload);
    }
    try {
        uint32_t len = static_cast<uint32_t>(payload.size());
        uint32_t sender = self;
        zmq::message_t msg(sizeof(uint32_t) + payload.size() + sizeof(uint32_t));
        memcpy(msg.data(), &len, sizeof(uint32_t));
        if (!payload.empty()) {
            memcpy(static_cast<char*>(msg.data()) + sizeof(uint32_t), payload.data(), payload.size());
        }
        memcpy(static_cast<char*>(msg.data()) + sizeof(uint32_t) + payload.size(), &sender,
               sizeof(uint32_t));
        std::lock_guard<std::mutex> lock(g_zmq_pub_mutex);
        return g_zmq_pushs[target_node]->send(msg);
    } catch (const zmq::error_t& e) {
        ereport(WARNING, (errmsg("epoch: ZMQ direct publish failed: %s", e.what())));
        return false;
    }
#else
    (void)target_node;
    (void)payload;
    return false;
#endif
}

bool EpochNetPublish(const std::string& payload)
{
    if (EpochDynNetIsEnabled()) {
        return EpochDynNetPublish(payload);
    }
    return EpochNetPublishDirect(payload);
}

bool EpochNetTryReceive(std::string* payload, int* from_node)
{
    if (payload == nullptr) {
        return false;
    }
    if (from_node != nullptr) {
        *from_node = -1;
    }
    if (EpochDynNetIsEnabled() && EpochDynNetTryReceive(payload)) {
        return true;
    }
    return g_incoming_queue.try_dequeue(*payload);
}

