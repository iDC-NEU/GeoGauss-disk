#include "postgres.h"

#include "epoch/e_net.h"

#include "epoch/e_recv_ledger.h"
#include "epoch/e_shmem.h"

#include "epoch/e_pacer.h"
#include "epoch/e_config.h"
#include "epoch/e_log_path.h"
#include "epoch/e_statistician.h"
#include "epoch/e_types.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_epoch_capacity.h"
#include "miscadmin.h"
#include "epoch/e_perf.h"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <condition_variable>
#include <deque>
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
/*
 * 接收队列按时间片分格：发送端入队时就知道 epoch（本端写集）；
 * 远端包在收包线程里先解析出 epoch 再入对应格。
 */
static moodycamel::ConcurrentQueue<std::string> g_incoming_queues[EPOCH_TXN_EPOCH_COUNT];

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
static std::unique_ptr<zmq::socket_t> g_zmq_pull;
/*
 * 每 peer 一把锁：每个 PUSH socket 只被它自己的发送线程使用，
 * 用全局锁会让"一条 peer 阻塞在 send()"时把其它 peer 的发送线程也挡在锁上，
 * per-peer 线程就失去意义了。
 */
static std::mutex g_zmq_peer_mutex[EPOCH_MAX_NODES];
static std::vector<std::unique_ptr<zmq::socket_t>> g_zmq_pushs;
static std::thread g_listen_thread;

static constexpr int EPOCH_DIRECT_PORT_OFFSET = 5000;

static int EpochNodeDirectPort(uint32_t node_id)
{
    return g_epoch_data_port_base + EPOCH_DIRECT_PORT_OFFSET + static_cast<int>(node_id);
}

/*
 * 收包循环：只轮询 PULL（每个对端一条 PUSH 连过来）。
 * PUB/SUB 通道已删除，因此这里不再有“多路 SUB + 一路 PULL”的分支。
 */
static int64_t EpochMonoUs(void);

static void EpochListenLoop(void)
{
    EpochSetAuxThreadName("epochlisten");

    /*
     * 收包心跳：每 5 秒一行，无论有没有数据都打。
     * 用来判断“对端在发但这边收不动”还是“这边根本没在收”。
     */
    uint64_t hb_msgs = 0;
    uint64_t hb_bytes = 0;
    /*
     * 静默丢弃计数：接收路径上有三处 continue 会把消息直接扔掉、不留痕迹，
     * 一旦丢掉的是"收尾包"，对应节点永远等不到该片的 EOF → recv 永久卡死。
     * 这三个计数用来区分"没收到"和"收到了但被丢"。
     */
    uint64_t drop_len = 0;      /* 长度字段非法 */
    uint64_t drop_parse = 0;    /* 反序列化失败 */
    uint64_t drop_epoch0 = 0;   /* 解析出来 epoch == 0 */
    int64_t hb_last_us = EpochMonoUs();

    while (g_net_running.load(std::memory_order_acquire)) {
        const int64_t hb_now_us = EpochMonoUs();
        if (hb_now_us - hb_last_us >= 5000000) {
            hb_last_us = hb_now_us;
            EPOCH_DBG_FPRINTF(
                "EPOCH_DBG listen_hb node=%u msgs=%lu bytes=%lu drop_len=%lu drop_parse=%lu drop_epoch0=%lu\n",
                EpochGetNodeId(),
                static_cast<unsigned long>(hb_msgs),
                static_cast<unsigned long>(hb_bytes),
                static_cast<unsigned long>(drop_len),
                static_cast<unsigned long>(drop_parse),
                static_cast<unsigned long>(drop_epoch0));
            fflush(stderr);
        }
        if (g_zmq_pull == nullptr) {
            pg_usleep(1000);
            continue;
        }

        /* 每 100ms 做一次 EOF 可靠性维护（发 ack + 补发未确认的 EOF） */
        {
            static int64_t eof_tick_us = 0;
            if (hb_now_us - eof_tick_us >= 100000) {
                eof_tick_us = hb_now_us;
                EpochNetTickEofReliability();
            }
        }

        zmq::pollitem_t item;
        item.socket = static_cast<void*>(*g_zmq_pull);
        item.fd = 0;
        item.events = ZMQ_POLLIN;
        item.revents = 0;
        (void)zmq::poll(&item, 1, 1);
        if ((item.revents & ZMQ_POLLIN) == 0) {
            continue;
        }

        zmq::message_t msg;
        if (!g_zmq_pull->recv(&msg, ZMQ_DONTWAIT)) {
            continue;
        }
        /* 报文格式：[len:4][payload:len][sender:4] */
        if (msg.size() < sizeof(uint32_t) * 2) {
            ++drop_len;
            continue;
        }
        const char* data = static_cast<const char*>(msg.data());
        uint32_t len = 0;
        memcpy(&len, data, sizeof(uint32_t));
        if (static_cast<size_t>(len) + sizeof(uint32_t) * 2 > msg.size()) {
            ++drop_len;
            continue;
        }
        std::string payload(data + sizeof(uint32_t), len);
        uint32_t sender = 0;
        memcpy(&sender, data + sizeof(uint32_t) + len, sizeof(uint32_t));
        const int from_node = static_cast<int>(sender);

        hb_msgs++;
        hb_bytes += msg.size();

        /* 按 epoch 分格：解析一次只为拿到 epoch 字段 */
        E_WireTxn probe;
        const bool parsed = E_WireDeserializeTxn(payload.data(), payload.size(), &probe);
        if (!parsed) {
            ++drop_parse;
            continue;
        }
        if (probe.epoch == 0) {
            ++drop_epoch0;
            continue;
        }
        {
            /* PA 控制消息：慢节点广播会话状态（session / e_f / proxy），就地处理、不入接收队列 */
            bool congested = false;
            if (E_WireTxnIsPaState(probe, &congested)) {
                (void)congested;
                EpochPacerPaNoteSlowState(probe);
                continue;
            }
            /* EOF 确认：对端告诉我"它连续收到我的 EOF 到第几片" */
            if (E_WireTxnIsEofAck(probe)) {
                EpochNetNoteEofAck(static_cast<uint32_t>(from_node), probe.epoch);
                continue;
            }
            /* PA：代理对本片的路由结论 */
            bool skip = false;
            if (E_WireTxnIsPaEpochDecision(probe, &skip)) {
                EpochPacerPaNoteEpochDecision(probe.epoch, skip, static_cast<uint32_t>(from_node));
                continue;
            }
            /* PA 控制消息：其它发送端催代理对该片立即给结论 */
            if (E_WireTxnIsPaDecideRequest(probe)) {
                EpochPacerPaNoteDecideRequest(probe.epoch);
                continue;
            }
            const uint32_t enq_slot = EpochSlotOf(probe.epoch);
            /* 诊断：只对环形边界 epoch 打印，确认入队槽位 */
            if (probe.epoch + 1 == EPOCH_TXN_EPOCH_COUNT ||
                (probe.epoch % (EPOCH_TXN_EPOCH_COUNT - 1)) == 0) {
                EPOCH_DBG_FPRINTF("EPOCH_DBG enq epoch=%lu slot=%u from=%u\n",
                    static_cast<unsigned long>(probe.epoch), enq_slot, probe.server_id);
                fflush(stderr);
            }
            g_incoming_queues[enq_slot].enqueue(std::move(payload));
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
        g_zmq_ctx = std::make_unique<zmq::context_t>(4);
        /*
         * 每个 peer 一条 PUSH（HWM=1024，约 1MB 缓冲）+ 一条 PULL 接收。
         * 原来的 PUB/SUB 通道已删除：它是 fire-and-forget，5Mbps 下 TCP
         * 缓冲一满就丢包，导致收尾包丢失、merge 永久互等。
         */
        /*
         * HWM=0：不设上限。加了快照闸门后每片只有 24 笔在飞，队列不会堆积；
         * 设成 1024 反而会在慢 peer 上堆 3.8MB 并触发 send 阻塞。
         */
        int hwm = 0;

        /*
         * 传输可靠性选项（关键）：
         *   ZMQ_IMMEDIATE=1        —— 无可用连接时 send 直接返回 EAGAIN（而不是
         *                             先排队、等 pipe 销毁时把消息静默丢掉）。
         *                             我们已有 while(!send()) 重试循环，会自动等重连。
         *   ZMQ_TCP_KEEPALIVE      —— 让内核也帮忙探测半开连接。
         *
         * 背景：不加这些时，PUSH 在 pipe 重建期间会静默丢弃消息；
         * 而接收端把 EOF 当一次性事件，丢了就永久等（实测 node2 曾整段 10 秒
         * 收不到任何消息，导致 snap_to 线性增长）。
         */
        int one = 1;
        /* libzmq 4.1 没有 ZMQ_HEARTBEAT_*，用内核 TCP keepalive 承担断链探测 */
        int ka_idle = 60;      /* 空闲 60s 开始探测 */
        int ka_intvl = 10;     /* 每 10s 一次 */
        int ka_cnt = 3;        /* 3 次无响应判定断链 */

        /* 接收通道：每个对端的 PUSH 连到这里，HWM 与发送侧一致 */
        g_zmq_pull = std::make_unique<zmq::socket_t>(*g_zmq_ctx, ZMQ_PULL);
        g_zmq_pull->setsockopt(ZMQ_RCVHWM, &hwm, sizeof(hwm));
        g_zmq_pull->setsockopt(ZMQ_TCP_KEEPALIVE, &one, sizeof(one));
        g_zmq_pull->setsockopt(ZMQ_TCP_KEEPALIVE_IDLE, &ka_idle, sizeof(ka_idle));
        g_zmq_pull->setsockopt(ZMQ_TCP_KEEPALIVE_INTVL, &ka_intvl, sizeof(ka_intvl));
        g_zmq_pull->setsockopt(ZMQ_TCP_KEEPALIVE_CNT, &ka_cnt, sizeof(ka_cnt));
        {
            std::string pull_addr =
                "tcp://*:" + std::to_string(EpochNodeDirectPort(EpochGetNodeId()));
            g_zmq_pull->bind(pull_addr);
        }
        g_zmq_pushs.clear();
        g_zmq_pushs.resize(EPOCH_MAX_NODES);

        if (g_epoch_peer_hosts != nullptr && g_epoch_peer_hosts[0] != '\0') {
            char* raw = pstrdup(g_epoch_peer_hosts);
            char* tok = strtok(raw, ",");
            uint32_t peer_id = 0;
            while (tok != nullptr) {
                if (peer_id != EpochGetNodeId()) {
                    {
                        /* 每个 peer 一条 PUSH：可靠投递，慢 peer 会在 send 处阻塞 */
                        auto push = std::make_unique<zmq::socket_t>(*g_zmq_ctx, ZMQ_PUSH);
                        push->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm));
                        push->setsockopt(ZMQ_IMMEDIATE, &one, sizeof(one));
                        push->setsockopt(ZMQ_TCP_KEEPALIVE, &one, sizeof(one));
                        push->setsockopt(ZMQ_TCP_KEEPALIVE_IDLE, &ka_idle, sizeof(ka_idle));
                        push->setsockopt(ZMQ_TCP_KEEPALIVE_INTVL, &ka_intvl, sizeof(ka_intvl));
                        push->setsockopt(ZMQ_TCP_KEEPALIVE_CNT, &ka_cnt, sizeof(ka_cnt));
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
                push->setsockopt(ZMQ_IMMEDIATE, &one, sizeof(one));
                push->setsockopt(ZMQ_TCP_KEEPALIVE, &one, sizeof(one));
                push->setsockopt(ZMQ_TCP_KEEPALIVE_IDLE, &ka_idle, sizeof(ka_idle));
                push->setsockopt(ZMQ_TCP_KEEPALIVE_INTVL, &ka_intvl, sizeof(ka_intvl));
                push->setsockopt(ZMQ_TCP_KEEPALIVE_CNT, &ka_cnt, sizeof(ka_cnt));
                std::string push_addr =
                    "tcp://127.0.0.1:" + std::to_string(EpochNodeDirectPort(peer_id));
                push->connect(push_addr);
                g_zmq_pushs[peer_id] = std::move(push);
            }
        }

        g_net_running.store(true, std::memory_order_release);
        EpochMarkPeerConnected(EpochGetNodeId());
        g_listen_thread = std::thread(EpochListenLoop);
        /* 等对端的 PULL 建立完成再发 hello */
        pg_usleep(500000L);
        EpochNetPublishHello();
        return true;
    } catch (const zmq::error_t& e) {
        fprintf(stderr, "epoch: ZMQ init failed: %s\n", e.what());
        return false;
    }
#else
    fprintf(stderr, "epoch: built without ZMQ, multi-node networking disabled\n");
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
    g_zmq_pushs.clear();
    g_zmq_pull.reset();
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

static constexpr int64_t EPOCH_UPLINK_BUDGET_HOLD_US = 200;
static uint64_t g_uplink_handed = 0;
static bool g_uplink_budget_hold = false;
static int64_t g_uplink_budget_hold_us = 0;
static bool g_uplink_congested_logged = false;
static std::atomic<bool> g_uplink_defer{false};
static std::atomic<bool> g_uplink_congested{false};
static std::atomic<int64_t> g_uplink_bps{0};




#endif

/*
 * 读网卡累计发送字节（跳过 lo 环回）。
 * 内核只提供累计值，“瞬时速率”必须用两次采样求差得到。
 */

/*
 * 上行带宽的“上限”：
 *   配置了限速 → 用配置值（5Mbps 等）；
 *   没配置     → 用网卡物理速率（/sys/class/net/<dev>/speed，千兆=1000）。
 * 拥塞与否只看“实际发了多少 / 上限”，与是否配限速无关：
 * 即使不限速，打满物理带宽也算拥塞。
 */
static double EpochNetUplinkLimitBps(void)
{
    if (g_epoch_dyn_net_upload_bps > 0.0) {
        return g_epoch_dyn_net_upload_bps;
    }

    /* 取第一个非 lo 接口名 */
    FILE* fp = fopen("/proc/net/dev", "r");
    if (fp == nullptr) {
        return 0.0;
    }
    char line[512];
    char dev[64] = {0};
    int lineno = 0;
    while (fgets(line, sizeof(line), fp) != nullptr) {
        if (++lineno <= 2) {
            continue;
        }
        char* colon = strchr(line, ':');
        if (colon == nullptr) {
            continue;
        }
        char* name = line;
        while (*name == ' ' || *name == '\t') {
            name++;
        }
        if (strncmp(name, "lo:", 3) == 0) {
            continue;
        }
        size_t n = static_cast<size_t>(colon - name);
        if (n == 0 || n >= sizeof(dev)) {
            continue;
        }
        memcpy(dev, name, n);
        dev[n] = '\0';
        break;
    }
    fclose(fp);
    if (dev[0] == '\0') {
        return 0.0;
    }

    char path[256];
    snprintf(path, sizeof(path), "/sys/class/net/%s/speed", dev);
    fp = fopen(path, "r");
    if (fp == nullptr) {
        return 0.0;
    }
    int speed_mbps = 0;
    if (fscanf(fp, "%d", &speed_mbps) != 1 || speed_mbps <= 0) {
        speed_mbps = 0;
    }
    fclose(fp);
    return static_cast<double>(speed_mbps) * 1e6;
}

/*
 * 上行拥塞判据：直接读 /proc/net/dev 的累计发送字节，算实时速率。
 *
 * 为什么不用 ZMQ 的 POLLOUT：加了快照闸门后 backend 的生产被 apply 拖住，
 * ZMQ 队列基本不会满，POLLOUT 永远可写 → DBR 永远不会触发。
 * 网卡速率才反映"这个方向到底有没有被 tc 限住"。
 *
 * 判据：当前发送速率 >= 配置的上行带宽 * 阈值比例（默认 90%）。
 * upload_bps 为 0 表示没有配置上限（或 DBR 未启用），此时一律判为不拥塞。
 */
static uint64_t EpochNetReadTxBytes(void)
{
    FILE* fp = fopen("/proc/net/dev", "r");
    if (fp == nullptr) {
        return 0;
    }
    char line[512];
    uint64_t total = 0;
    /* 跳过两行表头 */
    if (fgets(line, sizeof(line), fp) == nullptr || fgets(line, sizeof(line), fp) == nullptr) {
        fclose(fp);
        return 0;
    }
    while (fgets(line, sizeof(line), fp) != nullptr) {
        char* colon = strchr(line, ':');
        if (colon == nullptr) {
            continue;
        }
        const char* name = line;
        while (*name == ' ') {
            ++name;
        }
        if (strncmp(name, "lo:", 3) == 0) {
            continue;
        }
        /* rx: bytes packets errs drop fifo frame compressed multicast
         * tx: bytes packets errs drop fifo colls carrier compressed */
        unsigned long long rx_bytes = 0, rx_pkts = 0, rx_errs = 0, rx_drop = 0;
        unsigned long long rx_fifo = 0, rx_frame = 0, rx_comp = 0, rx_multi = 0;
        unsigned long long tx_bytes = 0;
        if (sscanf(colon + 1, "%llu %llu %llu %llu %llu %llu %llu %llu %llu",
                &rx_bytes, &rx_pkts, &rx_errs, &rx_drop, &rx_fifo, &rx_frame, &rx_comp,
                &rx_multi, &tx_bytes) == 9) {
            total += tx_bytes;
        }
    }
    fclose(fp);
    return total;
}

void EpochNetPollUplink(void)
{
    if (g_epoch_dyn_net_upload_bps <= 0) {
        g_uplink_congested.store(false, std::memory_order_relaxed);
        return;
    }

    static uint64_t last_tx_bytes = 0;
    static int64_t last_us = 0;

    const int64_t now = EpochMonoUs();
    const uint64_t tx = EpochNetReadTxBytes();
    if (last_us == 0 || tx < last_tx_bytes) {
        last_us = now;
        last_tx_bytes = tx;
        return;
    }
    /* 采样窗口至少 20ms，避免抖动 */
    if (now - last_us < 20000) {
        return;
    }
    const double dt_s = static_cast<double>(now - last_us) / 1000000.0;
    const double bps = static_cast<double>(tx - last_tx_bytes) * 8.0 / dt_s;

    last_us = now;
    last_tx_bytes = tx;

    g_uplink_bps.store(static_cast<int64_t>(bps), std::memory_order_relaxed);

    const double cap = g_epoch_dyn_net_upload_bps;
    int pct = g_epoch_dbr_threshold_pct;
    if (pct < 1) {
        pct = 1;
    } else if (pct > 100) {
        pct = 100;
    }
    const bool congested = (bps >= cap * (static_cast<double>(pct) / 100.0));
    if (congested != g_uplink_congested.load(std::memory_order_relaxed)) {
        g_uplink_congested.store(congested, std::memory_order_relaxed);
        if (congested) {
            EPOCH_DBG_FPRINTF("EPOCH_DBG uplink_congested: bps=%.2fMbps cap=%.2fMbps\n",
                bps / 1e6, cap / 1e6);
            fflush(stderr);
        }
    }
}

/* 当前上行速率（bps），供状态日志观察 */
int64_t EpochNetGetUplinkBps(void)
{
    return g_uplink_bps.load(std::memory_order_relaxed);
}


bool EpochNetDeferPayload(void)
{
#ifdef EPOCH_USE_ZMQ
    return g_uplink_defer.load(std::memory_order_relaxed);
#else
    return false;
#endif
}

/*
 * 某个 peer 的发送通道当前是否可写。
 * 供 DBR 判断"这一笔写集能不能直接入 ZMQ"：不可写即视为该 peer 方向拥塞。
 */

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
    try {
        uint32_t len = static_cast<uint32_t>(payload.size());
        uint32_t self = EpochGetNodeId();
        uint32_t total = EpochGetTotalNodes();

        for (uint32_t peer = 0; peer < total; ++peer) {
            if (peer == self || peer >= EPOCH_MAX_NODES || g_zmq_pushs[peer] == nullptr) {
                continue;
            }
            zmq::message_t msg(sizeof(uint32_t) + payload.size() + sizeof(uint32_t));
            memcpy(msg.data(), &len, sizeof(uint32_t));
            if (!payload.empty()) {
                memcpy(static_cast<char*>(msg.data()) + sizeof(uint32_t),
                    payload.data(), payload.size());
            }
            memcpy(static_cast<char*>(msg.data()) + sizeof(uint32_t) + payload.size(),
                &self, sizeof(uint32_t));

            /* 每个 peer 各用自己的锁：慢 peer 不会挡住其它 peer 的发送线程 */
            std::lock_guard<std::mutex> peer_lock(g_zmq_peer_mutex[peer]);
            while (!g_zmq_pushs[peer]->send(msg)) {
                pg_usleep(1000);
                CHECK_FOR_INTERRUPTS();
            }
        }
        return true;
    } catch (const zmq::error_t& e) {
        static std::atomic<uint32_t> zb_err_logged{0};
        if (zb_err_logged.fetch_add(1, std::memory_order_relaxed) < 20) {
            EPOCH_DBG_FPRINTF("EPOCH_DBG zmq_send_err: %s\n", e.what());
        }
        return false;
    }
#else
    (void)payload;
    return false;
#endif
}

/* ---------------------------------------------------------------------------
 * EOF 可靠送达（ACK 式）：
 *   接收端每 100ms 把"我连续收到的你的 EOF 到第几片"回执给发送端；
 *   发送端缓存最近 8 片 EOF 的报文，未被确认就补发，直到被确认。
 *
 *   ack 由接收线程主动发出，不依赖 epoch 推进/apply，所以即使对端卡在
 *   某一片的 merge 上，发送端也能收到"你那条 EOF 我没收到"的明确信号。
 * ------------------------------------------------------------------------- */
static constexpr uint32_t EPOCH_EOF_RING = 8;

static std::atomic<uint64_t> g_peer_eof_acked[EPOCH_MAX_NODES];   /* 对端已确认到第几片 */
static std::mutex g_eof_ring_mu[EPOCH_MAX_NODES];
static std::string g_eof_ring[EPOCH_MAX_NODES][EPOCH_EOF_RING];
static uint64_t g_eof_ring_epoch[EPOCH_MAX_NODES][EPOCH_EOF_RING];
static std::atomic<uint64_t> g_eof_resend{0};
static std::atomic<uint64_t> g_eof_ack_sent{0};

uint64_t EpochNetEofResent(void)
{
    return g_eof_resend.load(std::memory_order_relaxed);
}

uint64_t EpochNetEofRequestSent(void)
{
    return g_eof_ack_sent.load(std::memory_order_relaxed);   /* 口径改为 ack 发送数 */
}

/* 发送成功后，把该片 EOF 报文存进环形缓冲，供未确认时补发 */
void EpochNetNoteEofSent(uint32_t peer, uint64_t epoch, const std::string& payload)
{
    if (peer >= EPOCH_MAX_NODES) {
        return;
    }
    std::lock_guard<std::mutex> lk(g_eof_ring_mu[peer]);
    const uint32_t slot = static_cast<uint32_t>(epoch % EPOCH_EOF_RING);
    g_eof_ring[peer][slot] = payload;
    g_eof_ring_epoch[peer][slot] = epoch;
}

/* 收到对端的 ack：记录它连续收到我的 EOF 到第几片 */
void EpochNetNoteEofAck(uint32_t peer, uint64_t upto)
{
    if (peer >= EPOCH_MAX_NODES) {
        return;
    }
    uint64_t cur = g_peer_eof_acked[peer].load(std::memory_order_relaxed);
    while (upto > cur &&
           !g_peer_eof_acked[peer].compare_exchange_weak(cur, upto,
               std::memory_order_acq_rel, std::memory_order_relaxed)) {
    }
}

/*
 * 每 100ms 调一次：
 *   ① 给每个 peer 发 ack（我连续收到你的 EOF 到第几片）
 *   ② 检查自己发出的 EOF 有没有被确认，没有就补发（最多补到 cursor-1）
 */
void EpochNetTickEofReliability(void)
{
    const uint32_t self = EpochGetNodeId();
    const uint32_t total = EpochGetTotalNodes();
    std::string payload;

    for (uint32_t peer = 0; peer < total; ++peer) {
        if (peer == self || peer >= EPOCH_MAX_NODES || g_zmq_pushs[peer] == nullptr) {
            continue;
        }

        /* ① 发 ack */
        const uint64_t mine = EpochRecvAckUpto(peer);
        if (mine > 0) {
            E_WireTxn ack;
            ack.epoch = mine;
            ack.server_id = self;
            ack.txn_state = EPOCH_TXN_EMPTY;
            ack.csn = EPOCH_WIRE_EOF_ACK_CSN;
            ack.rows.clear();
            payload.clear();
            if (E_WireSerializeTxn(ack, &payload)) {
                if (EpochNetPublishTo(peer, payload)) {
                    g_eof_ack_sent.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }

        /*
         * ② 补发未被确认的 EOF
         *
         * 注意不能"acked 落后就补"——对端本来就会晚 1~2 片才 ack（它按顺序处理）。
         * 正确的判据是"acked 已经停滞超过 1 秒"：正常滞后会在一个 epoch（~300ms）
         * 内追上；停 1 秒还没动，才说明那条 EOF 真丢了。
         */
        static uint64_t last_ack_val[EPOCH_MAX_NODES] = {0};
        static int64_t ack_change_us[EPOCH_MAX_NODES] = {0};

        struct timespec now_ts;
        clock_gettime(CLOCK_MONOTONIC, &now_ts);
        const int64_t now_us = static_cast<int64_t>(now_ts.tv_sec) * 1000000 +
                               static_cast<int64_t>(now_ts.tv_nsec / 1000);

        const uint64_t cursor = EpochGetSendEpochPeer(peer);      /* 已发到 cursor-1 */
        const uint64_t acked = g_peer_eof_acked[peer].load(std::memory_order_acquire);
        if (acked != last_ack_val[peer]) {
            last_ack_val[peer] = acked;
            ack_change_us[peer] = now_us;
        }
        if (acked + 1 >= cursor) {
            continue;                                             /* 全部确认过 */
        }
        if (ack_change_us[peer] != 0 && (now_us - ack_change_us[peer]) < 1000000) {
            continue;                                             /* ack 还在动：正常滞后 */
        }
        uint64_t from = acked + 1;
        if (from + EPOCH_EOF_RING < cursor) {
            from = cursor - EPOCH_EOF_RING;                       /* 只保留最近 8 片 */
        }
        for (uint64_t e = from; e < cursor; ++e) {
            std::string resend;
            {
                std::lock_guard<std::mutex> lk(g_eof_ring_mu[peer]);
                const uint32_t slot = static_cast<uint32_t>(e % EPOCH_EOF_RING);
                if (g_eof_ring_epoch[peer][slot] != e || g_eof_ring[peer][slot].empty()) {
                    continue;
                }
                resend = g_eof_ring[peer][slot];
            }
            if (EpochNetPublishTo(peer, resend)) {
                g_eof_resend.fetch_add(1, std::memory_order_relaxed);
                EPOCH_DBG_FPRINTF(
                    "EPOCH_DBG eof_resend peer=%u epoch=%lu acked=%lu cursor=%lu\n",
                    peer, static_cast<unsigned long>(e),
                    static_cast<unsigned long>(acked),
                    static_cast<unsigned long>(cursor));
                fflush(stderr);
                break;                                                /* 一次只补一片，下轮再看 */
            }
        }
    }
}



/*
 * 阻塞式单发：只发给指定 peer，发不出去就一直等。
 * 每个 peer 有独立的发送线程，所以这里的阻塞只影响该 peer 自己。
 */
bool EpochNetPublishOne(uint32_t peer, const std::string& payload)
{
    if (EpochGetTotalNodes() <= 1 || peer >= EPOCH_MAX_NODES ||
        g_zmq_pushs[peer] == nullptr) {
        return true;
    }
    try {
        const uint32_t len = static_cast<uint32_t>(payload.size());
        const uint32_t self = EpochGetNodeId();

        zmq::message_t msg(sizeof(uint32_t) + payload.size() + sizeof(uint32_t));
        memcpy(msg.data(), &len, sizeof(uint32_t));
        if (!payload.empty()) {
            memcpy(static_cast<char*>(msg.data()) + sizeof(uint32_t), payload.data(), payload.size());
        }
        memcpy(static_cast<char*>(msg.data()) + sizeof(uint32_t) + payload.size(), &self,
            sizeof(uint32_t));

        std::lock_guard<std::mutex> lock(g_zmq_peer_mutex[peer]);
        while (!g_zmq_pushs[peer]->send(msg)) {
            pg_usleep(1000);
            CHECK_FOR_INTERRUPTS();
        }
        return true;
    } catch (const zmq::error_t& e) {
        EPOCH_DBG_FPRINTF("EPOCH_DBG publish_one_err peer=%u: %s\n", peer, e.what());
        return false;
    }
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
        std::lock_guard<std::mutex> lock(g_zmq_peer_mutex[target_node]);
        return g_zmq_pushs[target_node]->send(msg);
    } catch (const zmq::error_t& e) {
        EPOCH_DBG_FPRINTF("EPOCH_DBG zmq_publish_to_err: %s\n", e.what());
        return false;
    }
#else
    (void)payload;
    return false;
#endif
}

bool EpochNetPublish(const std::string& payload)
{
    return EpochNetPublishDirect(payload);
}

/*
 * 本端写集的“自我接收”：把刚发出去的那一条原样放进接收队列，
 * 接收线程会像处理远端包一样处理它（写集进 merge、EOF 走收尾逻辑）。
 * ZMQ 的 PUB/SUB 不回环，本机事务必须靠这条路径才能落地。
 */
bool EpochNetEnqueueSelf(uint64_t epoch, const std::string& payload)
{
    if (epoch == 0) {
        return false;
    }
    const uint32_t slot = EpochSlotOf(epoch);
    static std::atomic<uint64_t> self_cnt{0};
    const uint64_t n = self_cnt.fetch_add(1) + 1;
    if (epoch + 1 == EPOCH_TXN_EPOCH_COUNT || (epoch % (EPOCH_TXN_EPOCH_COUNT - 1)) == 0) {
        EPOCH_DBG_FPRINTF("EPOCH_DBG enq_self epoch=%lu slot=%u total=%lu\n",
            static_cast<unsigned long>(epoch), slot, static_cast<unsigned long>(n));
        fflush(stderr);
    }
    return g_incoming_queues[slot].enqueue(payload);
}

/* 诊断用：该 epoch 对应接收队列的近似长度 */
size_t EpochIncomingQueueSize(uint64_t epoch)
{
    return g_incoming_queues[EpochSlotOf(epoch)].size_approx();
}

bool EpochIncomingTryDequeue(uint64_t epoch, std::string* payload)
{
    if (payload == nullptr || epoch == 0) {
        return false;
    }
    return g_incoming_queues[EpochSlotOf(epoch)].try_dequeue(*payload);
}

