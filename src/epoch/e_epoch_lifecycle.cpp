#include "postgres.h"

#include "epoch/e_recv_ledger.h"
#include "epoch/e_epoch_lifecycle.h"
#include "epoch/e_config.h"
#include "epoch/e_log_path.h"
#include "epoch/e_node_manager.h"
#include "epoch/e_concurrent_map.h"

#include "portability/instr_time.h"

#include <cstdio>
#include <ctime>

struct EpochLifecycleEntry {
    instr_time t_start{};
    instr_time t_seal{};
    instr_time t_merge{};
    instr_time t_apply_first_submit{};
    instr_time t_apply{};
    bool has_start = false;
    bool has_seal = false;
    bool has_merge = false;
    bool has_apply_first_submit = false;
    bool has_apply = false;
    uint64_t apply_row_count = 0;
    uint64_t apply_cpu_us = 0;
    uint64_t apply_batch_count = 0;
    uint64_t apply_starve_ticks = 0; /* merge 已完、队列 inflight=0、apply 未完 */
};

// HCJ ADDBY: GeoGauss aum::concurrent_unordered_map 风格，替代单 mutex + unordered_map。
static EpochConcurrent::UnorderedMap<uint64_t, EpochLifecycleEntry> g_lifecycle;

static bool EpochLifecycleEnabled(void)
{
    return g_enable_epoch_multimaster && g_epoch_lifecycle_log;
}

static void EpochLifecycleAppendLine(const char* line)
{
    if (line == nullptr) {
        return;
    }
    char path[MAXPGPATH];
    if (!EpochFormatLogPath(path, sizeof(path), "epoch_lifecycle")) {
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

static uint64_t EpochLifecyclePeerReceivedTotal(uint64_t epoch)
{
    uint32_t self = EpochGetNodeId();
    uint64_t total = 0;
    for (uint32_t peer = 0; peer < EpochGetTotalNodes(); ++peer) {
        if (peer == self) {
            continue;
        }
        total += EpochRecvReceivedCount(epoch, peer);
    }
    return total;
}

static uint64_t EpochLifecycleInstrUs(const instr_time& start, const instr_time& end)
{
    instr_time delta = end;
    INSTR_TIME_SUBTRACT(delta, start);
    if (INSTR_TIME_GET_MICROSEC(delta) < 0) {
        return 0;
    }
    return (uint64_t)INSTR_TIME_GET_MICROSEC(delta);
}

static void EpochLifecycleEnsureEntryStarted(EpochLifecycleEntry& entry)
{
    if (!entry.has_start) {
        INSTR_TIME_SET_CURRENT(entry.t_start);
        entry.has_start = true;
    }
}

void EpochLifecycleReset(void)
{
    g_lifecycle.Clear();
}

void EpochLifecycleEnsureStarted(uint64_t epoch)
{
    if (!EpochLifecycleEnabled() || epoch == 0) {
        return;
    }
    g_lifecycle.Modify(epoch, [](EpochLifecycleEntry& entry, bool /*is_new*/) {
        if (entry.has_start) {
            return;
        }
        INSTR_TIME_SET_CURRENT(entry.t_start);
        entry.has_start = true;
    });
}

void EpochLifecycleNoteSeal(uint64_t epoch)
{
    if (!EpochLifecycleEnabled() || epoch == 0) {
        return;
    }
    g_lifecycle.Modify(epoch, [](EpochLifecycleEntry& entry, bool /*is_new*/) {
        EpochLifecycleEnsureEntryStarted(entry);
        if (entry.has_seal) {
            return;
        }
        INSTR_TIME_SET_CURRENT(entry.t_seal);
        entry.has_seal = true;
    });
}

void EpochLifecycleNoteMergeComplete(uint64_t epoch)
{
    if (!EpochLifecycleEnabled() || epoch == 0) {
        return;
    }
    g_lifecycle.Modify(epoch, [](EpochLifecycleEntry& entry, bool /*is_new*/) {
        EpochLifecycleEnsureEntryStarted(entry);
        if (entry.has_merge) {
            return;
        }
        INSTR_TIME_SET_CURRENT(entry.t_merge);
        entry.has_merge = true;
    });
}

void EpochLifecycleNoteApplyComplete(uint64_t epoch)
{
    if (!EpochLifecycleEnabled() || epoch == 0) {
        return;
    }
    g_lifecycle.Modify(epoch, [](EpochLifecycleEntry& entry, bool /*is_new*/) {
        EpochLifecycleEnsureEntryStarted(entry);
        if (entry.has_apply) {
            return;
        }
        INSTR_TIME_SET_CURRENT(entry.t_apply);
        entry.has_apply = true;
    });
}

void EpochLifecycleNoteApplyFirstSubmit(uint64_t epoch)
{
    if (!EpochLifecycleEnabled() || epoch == 0) {
        return;
    }
    g_lifecycle.Modify(epoch, [](EpochLifecycleEntry& entry, bool /*is_new*/) {
        EpochLifecycleEnsureEntryStarted(entry);
        if (entry.has_apply_first_submit) {
            return;
        }
        INSTR_TIME_SET_CURRENT(entry.t_apply_first_submit);
        entry.has_apply_first_submit = true;
    });
}

void EpochLifecycleNoteApplyStarveTick(uint64_t epoch)
{
    if (!EpochLifecycleEnabled() || epoch == 0) {
        return;
    }
    g_lifecycle.Modify(epoch, [](EpochLifecycleEntry& entry, bool /*is_new*/) {
        EpochLifecycleEnsureEntryStarted(entry);
        if (entry.has_apply) {
            return;
        }
        entry.apply_starve_ticks += 1;
    });
}

void EpochLifecycleNoteApplyBatch(uint64_t epoch, uint64_t row_count, uint64_t apply_us, bool ok)
{
    if (!EpochLifecycleEnabled() || epoch == 0) {
        return;
    }
    if (row_count == 0 && apply_us == 0) {
        return;
    }
    g_lifecycle.Modify(epoch, [row_count, apply_us, ok](EpochLifecycleEntry& entry, bool /*is_new*/) {
        EpochLifecycleEnsureEntryStarted(entry);
        entry.apply_row_count += row_count;
        entry.apply_cpu_us += apply_us;
        entry.apply_batch_count += 1;
        (void)ok;
    });
}

void EpochLifecycleEmitOnAdvance(uint64_t epoch)
{
    if (!EpochLifecycleEnabled() || epoch == 0) {
        return;
    }

    uint32_t self = EpochGetNodeId();
    uint64_t local_pack = E_NodeManager::GetLocalPackTxnCounters(epoch, self);
    uint64_t peer_rcv = EpochLifecyclePeerReceivedTotal(epoch);

    instr_time t_end;
    INSTR_TIME_SET_CURRENT(t_end);

    EpochLifecycleEntry entry;
    if (!g_lifecycle.Extract(epoch, &entry)) {
        if (local_pack == 0 && peer_rcv == 0) {
            return;
        }
        INSTR_TIME_SET_CURRENT(entry.t_start);
        entry.has_start = true;
    }

    if (local_pack == 0 && peer_rcv == 0) {
        return;
    }

    if (!entry.has_start) {
        entry.t_start = t_end;
        entry.has_start = true;
    }

    uint64_t total_us = EpochLifecycleInstrUs(entry.t_start, t_end);
    uint64_t seal_us = entry.has_seal ? EpochLifecycleInstrUs(entry.t_start, entry.t_seal) : 0;
    uint64_t merge_us = entry.has_merge ? EpochLifecycleInstrUs(entry.t_start, entry.t_merge) : 0;
    uint64_t apply_us = entry.has_apply ? EpochLifecycleInstrUs(entry.t_start, entry.t_apply) : 0;
    uint64_t seal_to_merge_us = (entry.has_seal && entry.has_merge) ?
        EpochLifecycleInstrUs(entry.t_seal, entry.t_merge) :
        0;
    /*
     * merge_to_apply_us / merge_to_first_submit_us：含 merge 超前于 logical 的等待。
     * apply 严格只 execute 当前 logical，故关窗后可能仍需等前序 epoch apply 完才能
     * 入队本 epoch；这两项会表观很大，不适合作为 apply 执行/调度瓶颈的参考。
     * 看 apply 请用 first_submit_to_apply_us、apply_cpu_us、apply_starve_ticks。
     */
    uint64_t merge_to_apply_us = (entry.has_merge && entry.has_apply) ?
        EpochLifecycleInstrUs(entry.t_merge, entry.t_apply) :
        0;
    uint64_t merge_to_first_submit_us = (entry.has_merge && entry.has_apply_first_submit) ?
        EpochLifecycleInstrUs(entry.t_merge, entry.t_apply_first_submit) :
        0;
    /* 本 epoch 首次入队 → apply_complete：真正执行窗（可参考） */
    uint64_t first_submit_to_apply_us = (entry.has_apply_first_submit && entry.has_apply) ?
        EpochLifecycleInstrUs(entry.t_apply_first_submit, entry.t_apply) :
        0;

    char buf[768];
    int rc = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "EPOCH_LIFECYCLE epoch=%lu local_pack=%lu peer_rcv=%lu "
        "seal_us=%lu merge_us=%lu apply_us=%lu total_us=%lu "
        "seal_to_merge_us=%lu merge_to_apply_us=%lu "
        "merge_to_first_submit_us=%lu first_submit_to_apply_us=%lu apply_starve_ticks=%lu "
        "apply_rows=%lu apply_cpu_us=%lu apply_batches=%lu "
        "seal=%d merge=%d apply=%d",
        epoch,
        local_pack,
        peer_rcv,
        seal_us,
        merge_us,
        apply_us,
        total_us,
        seal_to_merge_us,
        merge_to_apply_us,
        merge_to_first_submit_us,
        first_submit_to_apply_us,
        entry.apply_starve_ticks,
        entry.apply_row_count,
        entry.apply_cpu_us,
        entry.apply_batch_count,
        entry.has_seal ? 1 : 0,
        entry.has_merge ? 1 : 0,
        entry.has_apply ? 1 : 0);
    securec_check_ss(rc, "\0", "\0");
    EpochLifecycleAppendLine(buf);
}
