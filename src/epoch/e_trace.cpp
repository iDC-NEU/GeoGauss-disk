#include "postgres.h"

#include "epoch/e_trace.h"
#include "epoch/e_config.h"
#include "epoch/e_log_path.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_statistician.h"

#include "miscadmin.h"
#include "knl/knl_variable.h"

#include <cstdio>
#include <ctime>
#include <unordered_map>
#include <unordered_set>

static std::unordered_set<uint64_t> g_trace_peer_recv_logged;
static std::unordered_set<uint64_t> g_trace_merge_logged;
static std::unordered_set<uint64_t> g_trace_apply_logged;
static std::unordered_map<uint64_t, int> g_trace_seal_block_tick;

static bool EpochTraceEnabled(void)
{
    return EPOCH_SYNC_TRACE_ENABLED && g_enable_epoch_multimaster && EpochGetTotalNodes() > 1;
}

static void EpochTraceAppendLine(const char* line)
{
    if (line == nullptr) {
        return;
    }
    char path[MAXPGPATH];
    if (!EpochFormatLogPath(path, sizeof(path), "epoch_sync_trace")) {
        return;
    }

    FILE* fp = fopen(path, "a");
    if (fp == nullptr) {
        return;
    }

    time_t now = time(nullptr);
    struct tm tm_buf;
    (void)localtime_r(&now, &tm_buf);
    char ts[32];
    (void)strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_buf);

    fprintf(fp, "[%s] node=%u %s\n", ts, EpochGetNodeId(), line);
    (void)fclose(fp);
}

static uint64_t EpochTracePeerReceivedTotal(uint64_t epoch)
{
    uint32_t self = EpochGetNodeId();
    uint64_t total = 0;
    for (uint32_t n = 0; n < EpochGetTotalNodes(); ++n) {
        if (n == self) {
            continue;
        }
        total += E_NodeManager::GetStoreReceivedPackCounters(epoch, n);
    }
    return total;
}

static uint64_t EpochTraceLocalPackTotal(uint64_t epoch)
{
    return E_NodeManager::GetLocalPackTxnCounters(epoch, EpochGetNodeId());
}

static bool EpochTraceEpochIsIdle(uint64_t epoch)
{
    return EpochTraceLocalPackTotal(epoch) == 0 && EpochTracePeerReceivedTotal(epoch) == 0 &&
           E_NodeManager::GetChangeSetTxnCounters(epoch, EpochGetNodeId()) == 0;
}

void EpochTraceMaybeLogSealBlocked(uint64_t epoch, const char* reason, uint64_t executing,
    uint64_t change_set, uint64_t packed)
{
    if (!EpochTraceEnabled() || reason == nullptr) {
        return;
    }
    int& tick = g_trace_seal_block_tick[epoch];
    if (++tick % 200 != 1) {
        return;
    }
    char buf[256];
    int rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1,
                        "SEAL_BLOCK epoch=%lu reason=%s executing=%lu change_set=%lu pack=%lu",
                        epoch, reason, executing, change_set, packed);
    securec_check_ss(rc, "\0", "\0");
    EpochTraceAppendLine(buf);
}

void EpochTraceLogSealPrepare(uint64_t epoch, uint64_t txn_total)
{
    if (!EpochTraceEnabled()) {
        return;
    }
    if (txn_total == 0 && EpochTraceEpochIsIdle(epoch)) {
        return;
    }
    uint64_t change_set = E_NodeManager::GetChangeSetTxnCounters(epoch, EpochGetNodeId());
    uint64_t executing = E_NodeManager::GetExecutingTxnCounters(epoch, EpochGetNodeId());
    char buf[224];
    int rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1,
                        "SEAL epoch=%lu 执行的事务总数 pack=%lu change_set=%lu executing=%lu，准备打包发送给 peer 节点",
                        epoch, txn_total, change_set, executing);
    securec_check_ss(rc, "\0", "\0");
    EpochTraceAppendLine(buf);
}

void EpochTraceMaybeLogPeerReceived(uint64_t epoch)
{
    if (!EpochTraceEnabled()) {
        return;
    }
    if (!EpochIsAllPeersReceiveCompleted(epoch)) {
        return;
    }
    if (g_trace_peer_recv_logged.find(epoch) != g_trace_peer_recv_logged.end()) {
        return;
    }
    g_trace_peer_recv_logged.insert(epoch);

    uint64_t total = EpochTracePeerReceivedTotal(epoch);
    if (total == 0 || EpochTraceEpochIsIdle(epoch)) {
        return;
    }
    char buf[128];
    int rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1,
                        "RECV epoch=%lu 收到了来自其他 node 的事务总数为 %lu",
                        epoch, total);
    securec_check_ss(rc, "\0", "\0");
    EpochTraceAppendLine(buf);
}

void EpochTraceMaybeLogMergeSummary(uint64_t epoch, uint64_t need_merge, uint64_t merged_success)
{
    if (!EpochTraceEnabled()) {
        return;
    }
    if (g_trace_merge_logged.find(epoch) != g_trace_merge_logged.end()) {
        return;
    }
    g_trace_merge_logged.insert(epoch);

    if ((need_merge == 0 && merged_success == 0) || EpochTraceEpochIsIdle(epoch)) {
        return;
    }
    char buf[192];
    int rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1,
                        "MERGE epoch=%lu 一共有 %lu 事务的写集需要 merge，最后成功 merge 的总数为 %lu",
                        epoch, need_merge, merged_success);
    securec_check_ss(rc, "\0", "\0");
    EpochTraceAppendLine(buf);
}

void EpochTraceLogApplyDone(uint64_t epoch, uint64_t applied_count)
{
    if (!EpochTraceEnabled()) {
        return;
    }
    if (g_trace_apply_logged.find(epoch) != g_trace_apply_logged.end()) {
        return;
    }
    g_trace_apply_logged.insert(epoch);

    if (applied_count == 0 || EpochTraceEpochIsIdle(epoch)) {
        return;
    }
    char buf[128];
    int rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1,
                        "APPLY epoch=%lu 最后 apply 了 %lu 个事务写集",
                        epoch, applied_count);
    securec_check_ss(rc, "\0", "\0");
    EpochTraceAppendLine(buf);
}
