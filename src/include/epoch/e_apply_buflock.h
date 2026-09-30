#ifndef OPENGAUSS_E_APPLY_BUFLOCK_H
#define OPENGAUSS_E_APPLY_BUFLOCK_H

#include "postgres.h"

#include "storage/buf/buf.h"
#include "storage/buf/block.h"

#include <cstdint>

/*
 * apply 堆页锁 + find 路径分段计时（epoch_apply_lock_trace_ms > 0 时）：
 *
 * 写路径：
 * - excl_wait：LockBuffer(EXCLUSIVE) 阻塞 → 高则被 SHARE/其它 EXCL 堵住
 * - excl_hold / prune / getbuf / follow_tip / crit：持锁干活 → 高则自身慢并反堵读者
 *
 * find / 读路径：
 * - share_wait：堆页 LockBuffer(SHARE) 阻塞 → 等别人的 EXCLUSIVE
 * - rb_us / rb_miss_n：ReadBuffer_common 墙钟；miss 多为磁盘/替换读入
 * - rb_waitio_us：WaitIO（等别人 IO_IN_PROGRESS）
 * - bt_lock_us：btree _bt_getbuf LockBuffer 等待（索引页 content 锁）
 *
 * 若 find 秒级而 share_wait≈0、rb/bt 拆开可区分：IO vs bufmgr 争用 vs btree 锁 vs 纯 CPU
 */

extern bool EpochApplyBufLockEnabled(void);
extern bool EpochApplyBufLockCollecting(void);

extern void EpochApplyBufLockBeginRow(uint64_t epoch, uint64_t csn, Oid relid, uint32_t op, BlockNumber hint_blk);
extern void EpochApplyBufLockEndRow(const char* relname);
extern void EpochApplyBufLockBeginTxn(uint64_t epoch, uint64_t csn);
extern void EpochApplyBufLockEndTxn(void);

extern void EpochApplyBufLockNoteExclWait(uint64_t wait_us);
extern void EpochApplyBufLockMarkExclHeld(void);
extern void EpochApplyBufLockMarkExclReleased(void);

extern void EpochApplyBufLockAddFollowTipUs(uint64_t us);
extern void EpochApplyBufLockAddPruneUs(uint64_t us);
extern void EpochApplyBufLockAddGetBufUs(uint64_t us);
extern void EpochApplyBufLockAddCritUs(uint64_t us);

extern void EpochApplyBufLockNoteShareWait(uint64_t wait_us);
extern void EpochApplyBufLockNoteReadBuf(uint64_t us, bool hit);
extern void EpochApplyBufLockNoteWaitIO(uint64_t us);
extern void EpochApplyBufLockNoteBtLock(uint64_t wait_us);

extern void EpochApplyBufLockExclusive(Buffer buffer);
extern void EpochApplyBufLockUnlock(Buffer buffer);
extern void EpochApplyBufLockShare(Buffer buffer);
/* btree 页：计时 LockBuffer(access)，access 为 BT_READ/BT_WRITE */
extern void EpochApplyBufLockBtLockBuffer(Buffer buffer, int access);

#endif /* OPENGAUSS_E_APPLY_BUFLOCK_H */
