#include "postgres.h"

#include <atomic>
#include <cstdio>
#include <cstring>

#include "epoch/e_config.h"
#include "epoch/e_epoch_capacity.h"
#include "epoch/e_recv_ledger.h"

namespace {

/* declared 的默认值：远大于任何一片的事务数，表示“该节点还没冻结” */
constexpr uint64_t DECLARED_UNKNOWN = 99999;

/*
 * 账本按 epoch % EPOCH_TXN_EPOCH_COUNT 分格，三个字段各自一个数组。
 *
 * 只有接收线程写，且严格按 recv_epoch 顺序推进，所以写入无需加锁。
 * 换片时由推进动作顺手清空下一格，因此读旧 epoch 不会读到新片的数据
 * ——环形约束（physical 领先 logical 不到格数）保证旧片在被复用前就已处理完。
 */
uint64_t g_received[EPOCH_TXN_EPOCH_COUNT][EPOCH_MAX_NODES];
uint64_t g_declared[EPOCH_TXN_EPOCH_COUNT][EPOCH_MAX_NODES];
bool g_eof[EPOCH_TXN_EPOCH_COUNT][EPOCH_MAX_NODES];
/*
 * g_eof 的 epoch 标签：记录"这个槽里的标记属于哪一片"。
 *
 * 没有它时，槽位复用会让 EpochRecvHasEof(k + N) 读到第 k 片的旧 true，
 * ack 的"连续推进"循环会一路扫到天荒地老（实测 recv 永久卡在回绕点）。
 */
uint64_t g_eof_epoch[EPOCH_TXN_EPOCH_COUNT][EPOCH_MAX_NODES];

/* 接收游标 */
std::atomic<uint64_t> g_recv_epoch{1};

/* 迟到（epoch 与当前接收片不一致）被丢弃的消息数，正常应为 0 */
static std::atomic<uint64_t> g_stale_msgs{0};

/* 每个节点"连续收到 EOF"的最高片号；发送端据此判断要不要补发 */
static std::atomic<uint64_t> g_ack_upto[EPOCH_MAX_NODES];

inline void ResetSlot(uint32_t slot)
{
    memset(g_received[slot], 0, sizeof(g_received[slot]));
    memset(g_eof[slot], 0, sizeof(g_eof[slot]));
    memset(g_eof_epoch[slot], 0, sizeof(g_eof_epoch[slot]));
    for (uint32_t i = 0; i < EPOCH_MAX_NODES; ++i) {
        g_declared[slot][i] = DECLARED_UNKNOWN;
    }
}

inline uint32_t SlotOf(uint64_t epoch)
{
    return EpochSlotOf(epoch);
}

}  // namespace

void EpochRecvLedgerInit(void)
{
    g_recv_epoch.store(1, std::memory_order_release);
    /* 准备好第一格 */
    ResetSlot(SlotOf(1));
}

void EpochRecvNoteWire(uint64_t epoch, uint32_t node)
{
    if (epoch == 0 || node >= EPOCH_MAX_NODES) {
        return;
    }
    g_received[SlotOf(epoch)][node]++;
}

void EpochRecvNoteEof(uint64_t epoch, uint32_t node)
{
    if (epoch == 0 || node >= EPOCH_MAX_NODES) {
        return;
    }
    const uint32_t s = SlotOf(epoch);
    g_eof[s][node] = true;
    g_eof_epoch[s][node] = epoch;
    {
        /* 连续推进 ack_upto：只关心"从 1 开始不间断"的部分 */
        uint64_t cur = g_ack_upto[node].load(std::memory_order_relaxed);
        while (EpochRecvHasEof(cur + 1, node)) {
            ++cur;
        }
        if (cur > 0) {
            g_ack_upto[node].store(cur, std::memory_order_release);
        }
    }
    /* 应收 = 收尾包到达那一刻的实收 */
    g_declared[s][node] = g_received[s][node];
}

bool EpochRecvHasEof(uint64_t epoch, uint32_t node)
{
    if (epoch == 0 || node >= EPOCH_MAX_NODES) {
        return false;
    }
    const uint32_t s = SlotOf(epoch);
    return g_eof_epoch[s][node] == epoch && g_eof[s][node];
}


uint64_t EpochRecvDeclaredCount(uint64_t epoch, uint32_t node)
{
    if (epoch == 0 || node >= EPOCH_MAX_NODES) {
        return DECLARED_UNKNOWN;
    }
    return g_declared[SlotOf(epoch)][node];
}

uint64_t EpochRecvReceivedCount(uint64_t epoch, uint32_t node)
{
    if (epoch == 0 || node >= EPOCH_MAX_NODES) {
        return 0;
    }
    return g_received[SlotOf(epoch)][node];
}

bool EpochRecvAllEof(uint64_t epoch)
{
    if (epoch == 0) {
        return false;
    }
    const uint32_t s = SlotOf(epoch);
    const uint32_t total = EpochGetTotalNodes();
    for (uint32_t node = 0; node < total; ++node) {
        if (!EpochRecvHasEof(epoch, node)) {
            return false;
        }
    }
    return true;
}

uint64_t EpochRecvGetEpoch(void)
{
    return g_recv_epoch.load(std::memory_order_acquire);
}

/*
 * 推进接收游标。
 *
 * “所有节点 eof 到齐”放在 CAS 之前一起判断：多个线程同时发现到齐时，
 * CAS 只会让其中一个把游标 +1。
 * 推进成功后顺手清空下一格，这样换片时不需要额外的“格子归属”标记。
 */
bool EpochRecvTryAdvanceEpoch(uint64_t epoch)
{
    uint64_t expected = epoch;
    if (!EpochRecvAllEof(epoch)) {
        return false;
    }

    if (!g_recv_epoch.compare_exchange_strong(expected, epoch + 1,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        return false;
    }
    /* 抢到游标之后才清下一格：失败的线程不会碰到它 */
    ResetSlot(SlotOf(epoch + 1));
    return true;
}

/* 对外接口：不能放在匿名命名空间里，否则是内部链接，其它 TU 链接不到 */
uint64_t EpochRecvAckUpto(uint32_t node)
{
    if (node >= EPOCH_MAX_NODES) {
        return 0;
    }
    return g_ack_upto[node].load(std::memory_order_acquire);
}

/* 对外接口：必须放在匿名命名空间外，否则是内部链接，其它 TU 链接不到 */
void EpochRecvNoteStale(void)
{
    g_stale_msgs.fetch_add(1, std::memory_order_relaxed);
}

uint64_t EpochRecvStaleCount(void)
{
    return g_stale_msgs.load(std::memory_order_relaxed);
}
