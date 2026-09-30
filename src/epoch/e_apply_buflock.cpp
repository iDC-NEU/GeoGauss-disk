#include "postgres.h"

#include "epoch/e_apply_buflock.h"
#include "epoch/e_config.h"

#include "gs_thread.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "storage/buf/bufmgr.h"
#include "utils/elog.h"

struct EpochApplyBufLockRow {
    bool collecting;
    bool excl_held;
    uint64_t epoch;
    uint64_t csn;
    Oid relid;
    uint32_t op;
    BlockNumber hint_blk;
    uint64_t excl_wait_us;
    uint64_t excl_hold_us;
    uint64_t follow_tip_us;
    uint64_t prune_us;
    uint64_t getbuf_us;
    uint64_t crit_us;
    uint64_t share_wait_us;
    uint64_t rb_us;
    uint64_t rb_waitio_us;
    uint64_t bt_lock_us;
    uint32_t share_wait_n;
    uint32_t excl_wait_n;
    uint32_t rb_n;
    uint32_t rb_miss_n;
    uint32_t rb_waitio_n;
    uint32_t bt_lock_n;
    instr_time excl_hold_start;
};

struct EpochApplyBufLockTxn {
    bool active;
    uint64_t epoch;
    uint64_t csn;
    uint64_t excl_wait_us;
    uint64_t excl_hold_us;
    uint64_t follow_tip_us;
    uint64_t prune_us;
    uint64_t getbuf_us;
    uint64_t crit_us;
    uint64_t share_wait_us;
    uint64_t rb_us;
    uint64_t rb_waitio_us;
    uint64_t bt_lock_us;
    uint32_t rb_n;
    uint32_t rb_miss_n;
    uint32_t rows;
    uint32_t slow_rows;
};

static thread_local EpochApplyBufLockRow g_buflock_row;
static thread_local EpochApplyBufLockTxn g_buflock_txn;

static uint64_t EpochApplyBufLockInterestingUs(const EpochApplyBufLockRow* row)
{
    return row->excl_wait_us + row->excl_hold_us + row->share_wait_us + row->rb_us + row->bt_lock_us;
}

bool EpochApplyBufLockEnabled(void)
{
    return g_epoch_apply_lock_trace_ms > 0 && t_thrd.role == EPOCHAPPLYWORKER;
}

bool EpochApplyBufLockCollecting(void)
{
    return g_buflock_row.collecting;
}

void EpochApplyBufLockBeginTxn(uint64_t epoch, uint64_t csn)
{
    g_buflock_txn = {};
    if (!EpochApplyBufLockEnabled()) {
        return;
    }
    g_buflock_txn.active = true;
    g_buflock_txn.epoch = epoch;
    g_buflock_txn.csn = csn;
}

void EpochApplyBufLockEndTxn(void)
{
    if (!g_buflock_txn.active) {
        return;
    }
    const uint64_t threshold_us = static_cast<uint64_t>(g_epoch_apply_lock_trace_ms) * 1000ULL;
    const uint64_t interesting = g_buflock_txn.excl_wait_us + g_buflock_txn.excl_hold_us +
        g_buflock_txn.share_wait_us + g_buflock_txn.rb_us + g_buflock_txn.bt_lock_us;
    if (interesting >= threshold_us) {
        ereport(LOG,
            (errmsg(
                "APPLY_BUFLOCK_TXN epoch=%lu csn=%lu rows=%u slow_rows=%u "
                "excl_wait_us=%lu excl_hold_us=%lu follow_tip_us=%lu prune_us=%lu "
                "getbuf_us=%lu crit_us=%lu share_wait_us=%lu "
                "rb_us=%lu rb_n=%u rb_miss_n=%u rb_waitio_us=%lu bt_lock_us=%lu pid=%lu",
                g_buflock_txn.epoch,
                g_buflock_txn.csn,
                g_buflock_txn.rows,
                g_buflock_txn.slow_rows,
                g_buflock_txn.excl_wait_us,
                g_buflock_txn.excl_hold_us,
                g_buflock_txn.follow_tip_us,
                g_buflock_txn.prune_us,
                g_buflock_txn.getbuf_us,
                g_buflock_txn.crit_us,
                g_buflock_txn.share_wait_us,
                g_buflock_txn.rb_us,
                g_buflock_txn.rb_n,
                g_buflock_txn.rb_miss_n,
                g_buflock_txn.rb_waitio_us,
                g_buflock_txn.bt_lock_us,
                (unsigned long)t_thrd.proc_cxt.MyProcPid)));
    }
    g_buflock_txn = {};
}

void EpochApplyBufLockBeginRow(uint64_t epoch, uint64_t csn, Oid relid, uint32_t op, BlockNumber hint_blk)
{
    g_buflock_row = {};
    if (!EpochApplyBufLockEnabled()) {
        return;
    }
    g_buflock_row.collecting = true;
    g_buflock_row.epoch = epoch;
    g_buflock_row.csn = csn;
    g_buflock_row.relid = relid;
    g_buflock_row.op = op;
    g_buflock_row.hint_blk = hint_blk;
}

void EpochApplyBufLockEndRow(const char* relname)
{
    if (!g_buflock_row.collecting) {
        return;
    }
    if (g_buflock_row.excl_held) {
        EpochApplyBufLockMarkExclReleased();
    }

    const uint64_t threshold_us = static_cast<uint64_t>(g_epoch_apply_lock_trace_ms) * 1000ULL;
    const uint64_t interesting = EpochApplyBufLockInterestingUs(&g_buflock_row);
    if (g_buflock_txn.active) {
        g_buflock_txn.rows++;
        g_buflock_txn.excl_wait_us += g_buflock_row.excl_wait_us;
        g_buflock_txn.excl_hold_us += g_buflock_row.excl_hold_us;
        g_buflock_txn.follow_tip_us += g_buflock_row.follow_tip_us;
        g_buflock_txn.prune_us += g_buflock_row.prune_us;
        g_buflock_txn.getbuf_us += g_buflock_row.getbuf_us;
        g_buflock_txn.crit_us += g_buflock_row.crit_us;
        g_buflock_txn.share_wait_us += g_buflock_row.share_wait_us;
        g_buflock_txn.rb_us += g_buflock_row.rb_us;
        g_buflock_txn.rb_waitio_us += g_buflock_row.rb_waitio_us;
        g_buflock_txn.bt_lock_us += g_buflock_row.bt_lock_us;
        g_buflock_txn.rb_n += g_buflock_row.rb_n;
        g_buflock_txn.rb_miss_n += g_buflock_row.rb_miss_n;
        if (interesting >= threshold_us) {
            g_buflock_txn.slow_rows++;
        }
    }
    if (interesting >= threshold_us) {
        /*
         * 解读：
         * excl_wait 高 → 写被 SHARE/EXCL 堵住
         * excl_hold/crit 高 → 持锁慢（反堵读者）
         * share_wait 高 → 堆 find 等 EXCLUSIVE
         * rb_us 高 + rb_miss 高 / rb_waitio 高 → 磁盘或等别人 IO
         * rb_us 高 + miss≈0 + waitio≈0 → bufmgr/哈希/自旋争用或 CPU
         * bt_lock 高 → 索引页 content 锁争用
         */
        ereport(LOG,
            (errmsg(
                "APPLY_BUFLOCK_DETAIL epoch=%lu csn=%lu rel=%s(%u) op=%u hint_blk=%u "
                "excl_wait_us=%lu excl_wait_n=%u excl_hold_us=%lu "
                "follow_tip_us=%lu prune_us=%lu getbuf_us=%lu crit_us=%lu "
                "share_wait_us=%lu share_wait_n=%u "
                "rb_us=%lu rb_n=%u rb_miss_n=%u rb_waitio_us=%lu rb_waitio_n=%u "
                "bt_lock_us=%lu bt_lock_n=%u pid=%lu",
                g_buflock_row.epoch,
                g_buflock_row.csn,
                relname != nullptr && relname[0] != '\0' ? relname : "?",
                g_buflock_row.relid,
                g_buflock_row.op,
                static_cast<unsigned>(g_buflock_row.hint_blk),
                g_buflock_row.excl_wait_us,
                g_buflock_row.excl_wait_n,
                g_buflock_row.excl_hold_us,
                g_buflock_row.follow_tip_us,
                g_buflock_row.prune_us,
                g_buflock_row.getbuf_us,
                g_buflock_row.crit_us,
                g_buflock_row.share_wait_us,
                g_buflock_row.share_wait_n,
                g_buflock_row.rb_us,
                g_buflock_row.rb_n,
                g_buflock_row.rb_miss_n,
                g_buflock_row.rb_waitio_us,
                g_buflock_row.rb_waitio_n,
                g_buflock_row.bt_lock_us,
                g_buflock_row.bt_lock_n,
                (unsigned long)t_thrd.proc_cxt.MyProcPid)));
    }
    g_buflock_row = {};
}

void EpochApplyBufLockNoteExclWait(uint64_t wait_us)
{
    if (!g_buflock_row.collecting) {
        return;
    }
    g_buflock_row.excl_wait_us += wait_us;
    g_buflock_row.excl_wait_n++;
}

void EpochApplyBufLockMarkExclHeld(void)
{
    if (!g_buflock_row.collecting) {
        return;
    }
    if (g_buflock_row.excl_held) {
        return;
    }
    INSTR_TIME_SET_CURRENT(g_buflock_row.excl_hold_start);
    g_buflock_row.excl_held = true;
}

void EpochApplyBufLockMarkExclReleased(void)
{
    if (!g_buflock_row.collecting || !g_buflock_row.excl_held) {
        return;
    }
    instr_time now;
    INSTR_TIME_SET_CURRENT(now);
    INSTR_TIME_SUBTRACT(now, g_buflock_row.excl_hold_start);
    g_buflock_row.excl_hold_us += static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(now));
    g_buflock_row.excl_held = false;
}

void EpochApplyBufLockAddFollowTipUs(uint64_t us)
{
    if (g_buflock_row.collecting) {
        g_buflock_row.follow_tip_us += us;
    }
}

void EpochApplyBufLockAddPruneUs(uint64_t us)
{
    if (g_buflock_row.collecting) {
        g_buflock_row.prune_us += us;
    }
}

void EpochApplyBufLockAddGetBufUs(uint64_t us)
{
    if (g_buflock_row.collecting) {
        g_buflock_row.getbuf_us += us;
    }
}

void EpochApplyBufLockAddCritUs(uint64_t us)
{
    if (g_buflock_row.collecting) {
        g_buflock_row.crit_us += us;
    }
}

void EpochApplyBufLockNoteShareWait(uint64_t wait_us)
{
    if (!g_buflock_row.collecting) {
        return;
    }
    g_buflock_row.share_wait_us += wait_us;
    g_buflock_row.share_wait_n++;
}

void EpochApplyBufLockNoteReadBuf(uint64_t us, bool hit)
{
    if (!g_buflock_row.collecting) {
        return;
    }
    g_buflock_row.rb_us += us;
    g_buflock_row.rb_n++;
    if (!hit) {
        g_buflock_row.rb_miss_n++;
    }
}

void EpochApplyBufLockNoteWaitIO(uint64_t us)
{
    if (!g_buflock_row.collecting) {
        return;
    }
    g_buflock_row.rb_waitio_us += us;
    g_buflock_row.rb_waitio_n++;
}

void EpochApplyBufLockNoteBtLock(uint64_t wait_us)
{
    if (!g_buflock_row.collecting) {
        return;
    }
    g_buflock_row.bt_lock_us += wait_us;
    g_buflock_row.bt_lock_n++;
}

void EpochApplyBufLockExclusive(Buffer buffer)
{
    if (!g_buflock_row.collecting) {
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        return;
    }
    instr_time t0;
    instr_time t1;
    INSTR_TIME_SET_CURRENT(t0);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    INSTR_TIME_SET_CURRENT(t1);
    INSTR_TIME_SUBTRACT(t1, t0);
    EpochApplyBufLockNoteExclWait(static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(t1)));
    EpochApplyBufLockMarkExclHeld();
}

void EpochApplyBufLockUnlock(Buffer buffer)
{
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    EpochApplyBufLockMarkExclReleased();
}

void EpochApplyBufLockShare(Buffer buffer)
{
    if (!g_buflock_row.collecting) {
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        return;
    }
    instr_time t0;
    instr_time t1;
    INSTR_TIME_SET_CURRENT(t0);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    INSTR_TIME_SET_CURRENT(t1);
    INSTR_TIME_SUBTRACT(t1, t0);
    EpochApplyBufLockNoteShareWait(static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(t1)));
}

void EpochApplyBufLockBtLockBuffer(Buffer buffer, int access)
{
    if (!g_buflock_row.collecting) {
        LockBuffer(buffer, access);
        return;
    }
    instr_time t0;
    instr_time t1;
    INSTR_TIME_SET_CURRENT(t0);
    LockBuffer(buffer, access);
    INSTR_TIME_SET_CURRENT(t1);
    INSTR_TIME_SUBTRACT(t1, t0);
    EpochApplyBufLockNoteBtLock(static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(t1)));
}
