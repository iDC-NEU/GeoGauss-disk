#include "postgres.h"

#include <atomic>

#include "epoch/e_statistician.h"
#include "epoch/e_types.h"
#include "epoch/e_config.h"

#include "miscadmin.h"
#include "gs_thread.h"

/*
 * 这个文件原本承担了大量职责：收尾包构造、seal 判据、重连补发、
 * 以及十几种诊断打点。现在这些都已迁出或废弃：
 *
 *   收尾包构造与入队  → 内联进 EpochAdvancePhysicalOnce
 *   seal / replay     → 整个机制已删除
 *   诊断打点          → 已删除
 *
 * 只剩“对端已封口到哪个 epoch”这一项，因为 pacer 还在读它。
 */

static std::atomic<uint64_t> g_peer_max_sealed_epoch[EPOCH_MAX_NODES];

uint64_t EpochGetPeerMaxSealedEpoch(uint32_t peer_node)
{
    if (peer_node >= EPOCH_MAX_NODES) {
        return 0;
    }
    return g_peer_max_sealed_epoch[peer_node].load(std::memory_order_acquire);
}

void EpochNotePeerSealedEpoch(uint32_t peer_node, uint64_t epoch)
{
    if (peer_node >= EPOCH_MAX_NODES || epoch == 0) {
        return;
    }
    uint64_t cur = g_peer_max_sealed_epoch[peer_node].load(std::memory_order_relaxed);
    while (epoch > cur &&
           !g_peer_max_sealed_epoch[peer_node].compare_exchange_weak(
               cur, epoch, std::memory_order_release, std::memory_order_relaxed)) {
    }
}

/*
 * 多主同步模式下，backend 不写 COMMIT/ABORT WAL、不等刷盘 —— 数据持久化由 apply 完成。
 *
 * 注意这个分支不只是"省掉 WAL"：它还显式推进内存 CLOG 与提交 CSN
 * （setCommitCsn(getLocalNextCSN()) / TransactionIdCommitTree）。
 * 少了这一步，epoch 侧带出去的 csn 不会真正进入可见性判定，
 * 表现就是"行写进去了，但 SELECT 查不到"。
 */
bool EpochShouldSkipBackendWal(void)
{
    if (!(g_enable_epoch_multimaster && EpochGetTotalNodes() > 1)) {
        return false;
    }
    /* epoch 系列 worker 自己要走完整流程（它们负责真正落盘） */
    if (AmEpochWorkerProcess()) {
        return false;
    }
    if (t_thrd.role == EPOCHWORKER || t_thrd.role == EPOCHAPPLYWORKER ||
        t_thrd.role == EPOCHMERGEWORKER || t_thrd.role == EPOCHPACKWORKER ||
        t_thrd.role == EPOCHSENDWORKER || t_thrd.role == EPOCHTIPFLUSHER) {
        return false;
    }
    return true;
}
