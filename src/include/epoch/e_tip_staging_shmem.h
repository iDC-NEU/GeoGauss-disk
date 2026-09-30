#ifndef OPENGAUSS_E_TIP_STAGING_SHMEM_H
#define OPENGAUSS_E_TIP_STAGING_SHMEM_H

#include "postgres.h"

#include "access/htup.h"
#include "storage/item/itemptr.h"
#include "utils/relcache.h"

#include <cstdint>

/*
 * Cross-process tip cache for epoch local apply (g_epoch_local_tip_staging).
 * Key = (relid, block, offset) = wire root TID. Fixed capacity at postmaster start.
 *
 * Reclaim: EpochTipFlushWorkers (PG aux, count = epoch_tip_flush_threads) watch watermarks
 * and flush by dirty page (same-block aggregation). Apply upsert must NOT do heavy
 * shard flush on the hot path.
 */
static constexpr uint32_t EPOCH_TIP_STAGING_MAX_TUPLE = 2048;
/* ~1GB tip store: buy time while flusher drains under 50-terminal TPCC. */
static constexpr uint32_t EPOCH_TIP_STAGING_NUM_ENTRIES = 524288;
static constexpr uint32_t EPOCH_TIP_STAGING_NUM_SHARDS = 1999;
static constexpr uint32_t EPOCH_TIP_STAGING_SKEW_SLACK = 64;

/*
 * Dirty-page index (buffer-pool style): one slot = one heap page's complete
 * dirty-TID set. Page size is fixed → offset bitmap covers MaxHeapTuplesPerPage;
 * there is no per-page "overflow". Only failure mode is too many distinct dirty
 * pages for the table — then DirtyTouch sync flush-evicts a victim (no sleep).
 */
/* 4×：脏页窗口满时 DirtyTouch 会在 apply 热路径上同步淘汰。 */
static constexpr uint32_t EPOCH_TIP_DIRTY_NUM_PAGES = 131072;
/* Round-robin dirty scan stride (avoid 32k spinlocks every tick). */
static constexpr uint32_t EPOCH_TIP_DIRTY_SCAN_STRIDE = 4096;
static constexpr uint32_t EPOCH_TIP_FLUSH_BUDGET_MAX = 256;

/*
 * Start reclaim earlier and press harder — tip fill outran flush at 50%/70%/85%.
 */
static constexpr uint32_t EPOCH_TIP_FLUSH_LOW_PCT = 25;
static constexpr uint32_t EPOCH_TIP_FLUSH_HIGH_PCT = 45;
static constexpr uint32_t EPOCH_TIP_FLUSH_CRITICAL_PCT = 65;

extern Size EpochTipStagingShmemSize(void);
extern void EpochTipStagingShmemInit(void);

/* Write path: table-owned copy in shmem (apply hot path, no page lock). */
extern bool EpochTipStagingUpsert(Relation rel, ItemPointer tid, HeapTuple tuple, uint64_t csn);
extern bool EpochTipStagingDelete(Relation rel, ItemPointer tid, uint64_t csn);

/*
 * Read path: true when entry exists.
 * *out_copy is CurrentMemoryContext (heap_freetuple); nullptr when deleted.
 */
extern bool EpochTipStagingLookupCopy(Relation rel, ItemPointer tid, HeapTuple* out_copy, bool* is_deleted);

extern bool EpochTipStagingHasEntry(Relation rel, ItemPointer tid);

/* Proactive flusher API (EpochTipFlushWorker). */
extern void EpochTipStagingRequestFlush(void);
extern uint32_t EpochTipStagingTotalUsed(void);
extern size_t EpochTipStagingFlushPage(Oid relid, BlockNumber block);
extern size_t EpochTipStagingFlusherTick(void);

/*
 * Pick up to max_pages dirty pages into out_relid/out_block (caller arrays).
 * Returns number of pages selected. Prefers high dirty_count when critical.
 */
extern uint32_t EpochTipStagingPickDirtyPages(Oid* out_relid, BlockNumber* out_block, uint32_t max_pages);

extern bool EpochTipStagingFlushUrgent(void);
extern void EpochTipStagingClearUrgent(void);

/* Counters for flusher LOG (inplace/force/fail/pages/shard_full). */
extern void EpochTipStagingGetFlushCounters(uint64_t* inplace, uint64_t* force, uint64_t* deleted, uint64_t* fail,
    uint64_t* pages, uint64_t* shard_full);

/*
 * Cumulative flush-path profile (µs + counts). Worker logs 5s deltas.
 * Phases: tick/pick/find_page/collect/open/inplace/force/delete/xact.
 * dirty_evict / dirty_evict_us = DirtyTouch sync FlushPage count & time.
 * dirty_touch_full = hash window full; dirty_touch_fail = still no slot.
 * find_miss = Pick→Flush race (slot reused); should be rare.
 */
struct EpochTipFlushProfile {
    uint64_t tick_us;
    uint64_t pick_us;
    uint64_t find_page_us;
    uint64_t collect_us;
    uint64_t open_us;
    uint64_t inplace_us;
    uint64_t force_us;
    uint64_t delete_us;
    uint64_t xact_us;
    uint64_t dirty_evict;
    uint64_t dirty_evict_us;
    uint64_t dirty_touch_full;
    uint64_t dirty_touch_fail;
    uint64_t find_miss;
    uint64_t inplace_try;
    uint64_t force_try;
    uint64_t tips_done;
};
extern void EpochTipStagingGetFlushProfile(EpochTipFlushProfile* out);

#endif /* OPENGAUSS_E_TIP_STAGING_SHMEM_H */
