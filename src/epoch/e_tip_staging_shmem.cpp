#include "postgres.h"

#include "epoch/e_tip_staging_shmem.h"
#include "epoch/e_config.h"
#include "epoch/e_heap_version.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "access/xact.h"
#include "occ/o_heapam.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/item/itemid.h"
#include "storage/item/itemptr.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "portability/instr_time.h"

#include "securec.h"
#include "securec_check.h"

#include <atomic>
#include <cstring>
#include <functional>

/*
 * Fixed-capacity tip cache in shmem.
 *
 * Hot path: Upsert/Lookup touch one shard + DirtyPageTouch.
 * Dirty index is buffer-pool style: complete per-page offset bitmap; hash
 * window full → RequestFlush + sync flush-evict a victim page (needed so tip
 * stays reclaimable; no sleep on the writer). Reclaim: EpochTipFlushWorker
 * picks dirty pages and FlushPage. Never call simple_heap_update/delete.
 */

/*
 * DIAG A/B (crash isolation): when false, tip flush only does inplace writeback.
 * Skips EpochTipForceUpdate/Delete (O_heap_*), so flush cannot create CSN/HOT
 * version chains.
 *
 * 刷失败保留 tip，供下一次 locate 命中，避免退回索引扫描。
 * 不立刻重新标脏：inplace 失败再刷只会反复拿 RowExclusiveLock，挡住 apply。
 */
static constexpr bool EPOCH_TIP_FLUSH_FORCE_ENABLED = false;
static constexpr bool EPOCH_TIP_FLUSH_DROP_ON_FAIL = false;

struct EpochTipEntry {
    uint8_t used;
    uint8_t deleted;
    uint16_t t_len;
    Oid relid;
    BlockNumber block;
    OffsetNumber offset;
    uint64_t csn;
    char bytes[EPOCH_TIP_STAGING_MAX_TUPLE];
};

struct EpochTipShardHeader {
    slock_t lock;
    uint32_t used_count;
};

/* OffsetNumber is 1-based; bit (off-1) marks tip dirty on this page. */
static constexpr uint32_t EPOCH_TIP_DIRTY_BITMAP_BITS = static_cast<uint32_t>(MaxHeapTuplesPerPage);
static constexpr uint32_t EPOCH_TIP_DIRTY_BITMAP_BYTES = (EPOCH_TIP_DIRTY_BITMAP_BITS + 7U) / 8U;
/* Open-address probe from page hash — Touch and Find MUST use the same bound. */
static constexpr uint32_t EPOCH_TIP_DIRTY_PROBE = 512;

struct EpochTipDirtyPage {
    Oid relid;
    BlockNumber block;
    uint32_t dirty_count; /* number of bits set in offset_bitmap */
    uint64_t last_dirty_us;
    uint8_t used;
    uint8_t flushing;
    uint8_t offset_bitmap[EPOCH_TIP_DIRTY_BITMAP_BYTES];
    slock_t lock;
};

struct EpochTipStagingHeader {
    uint32_t num_shards;
    uint32_t entries_per_shard;
    uint32_t dirty_num_pages;
    std::atomic<uint32_t> total_used;
    std::atomic<uint32_t> flush_urgent;
    std::atomic<uint32_t> dirty_scan_cursor;
    std::atomic<uint32_t> dirty_used_slots; /* occupied dirty-page slots (buffer-pool occupancy) */
    std::atomic<uint64_t> tips_inplace;
    std::atomic<uint64_t> tips_force;
    std::atomic<uint64_t> tips_deleted_flush;
    std::atomic<uint64_t> tips_flush_fail;
    std::atomic<uint64_t> pages_flushed;
    std::atomic<uint64_t> shard_full_events;
    std::atomic<uint64_t> shard_full_last_log_us;
    /* Flush path profile (cumulative µs / counts) — for saturated-tip bottleneck. */
    std::atomic<uint64_t> prof_tick_us;
    std::atomic<uint64_t> prof_pick_us;
    std::atomic<uint64_t> prof_find_page_us;
    std::atomic<uint64_t> prof_collect_us;
    std::atomic<uint64_t> prof_open_us;
    std::atomic<uint64_t> prof_inplace_us;
    std::atomic<uint64_t> prof_force_us;
    std::atomic<uint64_t> prof_delete_us;
    std::atomic<uint64_t> prof_xact_us;
    std::atomic<uint64_t> prof_dirty_evict;
    std::atomic<uint64_t> prof_dirty_evict_us; /* time in DirtyTouch sync FlushPage */
    std::atomic<uint64_t> prof_dirty_touch_full; /* hash window full before evict */
    std::atomic<uint64_t> prof_dirty_touch_fail; /* still no slot after evict */
    std::atomic<uint64_t> prof_find_miss;
    std::atomic<uint64_t> prof_inplace_try;
    std::atomic<uint64_t> prof_force_try;
    std::atomic<uint64_t> prof_tips_done; /* cleared tips in profiled ticks */
};

struct EpochTipStagingShmem {
    EpochTipStagingHeader header;
    char blob[1];
};

static constexpr const char* EPOCH_TIP_STAGING_SHMEM_NAME = "Epoch Tip Staging Store";

static EpochTipStagingShmem* g_epochTipStaging = nullptr;

static uint32_t EpochTipEntriesPerShard(void)
{
    return (EPOCH_TIP_STAGING_NUM_ENTRIES + EPOCH_TIP_STAGING_NUM_SHARDS - 1U) / EPOCH_TIP_STAGING_NUM_SHARDS +
           EPOCH_TIP_STAGING_SKEW_SLACK;
}

static size_t EpochTipShardStride(uint32_t entries_per_shard)
{
    return sizeof(EpochTipShardHeader) + static_cast<size_t>(entries_per_shard) * sizeof(EpochTipEntry);
}

static size_t EpochTipShardsBytes(uint32_t eps)
{
    return static_cast<size_t>(EPOCH_TIP_STAGING_NUM_SHARDS) * EpochTipShardStride(eps);
}

static size_t EpochTipDirtyBytes(void)
{
    return static_cast<size_t>(EPOCH_TIP_DIRTY_NUM_PAGES) * sizeof(EpochTipDirtyPage);
}

Size EpochTipStagingShmemSize(void)
{
    uint32_t eps = EpochTipEntriesPerShard();
    return sizeof(EpochTipStagingHeader) + EpochTipShardsBytes(eps) + EpochTipDirtyBytes();
}

static EpochTipShardHeader* EpochTipShardAt(uint32_t shard_idx)
{
    uint32_t eps = g_epochTipStaging->header.entries_per_shard;
    char* base = g_epochTipStaging->blob;
    return reinterpret_cast<EpochTipShardHeader*>(base + static_cast<size_t>(shard_idx) * EpochTipShardStride(eps));
}

static EpochTipEntry* EpochTipShardEntries(EpochTipShardHeader* shard)
{
    return reinterpret_cast<EpochTipEntry*>(shard + 1);
}

static EpochTipDirtyPage* EpochTipDirtyBase(void)
{
    uint32_t eps = g_epochTipStaging->header.entries_per_shard;
    return reinterpret_cast<EpochTipDirtyPage*>(g_epochTipStaging->blob + EpochTipShardsBytes(eps));
}

static size_t EpochTipKeyHash(Oid relid, BlockNumber block, OffsetNumber offset)
{
    size_t h = std::hash<Oid>{}(relid);
    h ^= std::hash<BlockNumber>{}(block) + 0x9e3779b9 + (h << 6) + (h >> 2);
    h ^= std::hash<OffsetNumber>{}(offset) + 0x9e3779b9 + (h << 6) + (h >> 2);
    return h;
}

static size_t EpochTipPageHash(Oid relid, BlockNumber block)
{
    size_t h = std::hash<Oid>{}(relid);
    h ^= std::hash<BlockNumber>{}(block) + 0x9e3779b9 + (h << 6) + (h >> 2);
    return h;
}

static uint32_t EpochTipShardIndex(Oid relid, BlockNumber block, OffsetNumber offset)
{
    return static_cast<uint32_t>(EpochTipKeyHash(relid, block, offset) % g_epochTipStaging->header.num_shards);
}

static uint32_t EpochTipSlotStart(Oid relid, BlockNumber block, OffsetNumber offset, uint32_t eps)
{
    return static_cast<uint32_t>(EpochTipKeyHash(relid, block, offset) % eps);
}

static bool EpochTipKeyEqual(const EpochTipEntry& e, Oid relid, BlockNumber block, OffsetNumber offset)
{
    return e.used != 0 && e.relid == relid && e.block == block && e.offset == offset;
}

static uint64_t EpochTipNowUs(void)
{
    instr_time t;
    INSTR_TIME_SET_CURRENT(t);
    return static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(t));
}

void EpochTipStagingShmemInit(void)
{
    bool found = false;
    g_epochTipStaging = static_cast<EpochTipStagingShmem*>(
        ShmemInitStruct(EPOCH_TIP_STAGING_SHMEM_NAME, EpochTipStagingShmemSize(), &found));
    if (g_epochTipStaging == nullptr) {
        return;
    }
    if (found) {
        return;
    }

    uint32_t eps = EpochTipEntriesPerShard();
    g_epochTipStaging->header.num_shards = EPOCH_TIP_STAGING_NUM_SHARDS;
    g_epochTipStaging->header.entries_per_shard = eps;
    g_epochTipStaging->header.dirty_num_pages = EPOCH_TIP_DIRTY_NUM_PAGES;
    g_epochTipStaging->header.total_used.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.flush_urgent.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.dirty_scan_cursor.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.dirty_used_slots.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.tips_inplace.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.tips_force.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.tips_deleted_flush.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.tips_flush_fail.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.pages_flushed.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.shard_full_events.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.shard_full_last_log_us.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_tick_us.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_pick_us.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_find_page_us.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_collect_us.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_open_us.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_inplace_us.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_force_us.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_delete_us.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_xact_us.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_dirty_evict.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_dirty_evict_us.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_dirty_touch_full.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_dirty_touch_fail.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_find_miss.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_inplace_try.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_force_try.store(0, std::memory_order_relaxed);
    g_epochTipStaging->header.prof_tips_done.store(0, std::memory_order_relaxed);

    for (uint32_t s = 0; s < g_epochTipStaging->header.num_shards; ++s) {
        EpochTipShardHeader* shard = EpochTipShardAt(s);
        SpinLockInit(&shard->lock);
        shard->used_count = 0;
        EpochTipEntry* entries = EpochTipShardEntries(shard);
        for (uint32_t i = 0; i < eps; ++i) {
            entries[i].used = 0;
            entries[i].deleted = 0;
            entries[i].t_len = 0;
            entries[i].relid = InvalidOid;
            entries[i].block = 0;
            entries[i].offset = InvalidOffsetNumber;
            entries[i].csn = 0;
        }
    }

    EpochTipDirtyPage* dirty = EpochTipDirtyBase();
    for (uint32_t i = 0; i < EPOCH_TIP_DIRTY_NUM_PAGES; ++i) {
        SpinLockInit(&dirty[i].lock);
        dirty[i].used = 0;
        dirty[i].flushing = 0;
        dirty[i].dirty_count = 0;
        dirty[i].last_dirty_us = 0;
        dirty[i].relid = InvalidOid;
        dirty[i].block = 0;
        errno_t rc = memset_s(dirty[i].offset_bitmap, sizeof(dirty[i].offset_bitmap), 0, sizeof(dirty[i].offset_bitmap));
        securec_check(rc, "\0", "\0");
    }
}

static bool EpochTipFillEntry(EpochTipEntry* e, Oid relid, BlockNumber block, OffsetNumber offset, HeapTuple tuple,
    uint64_t csn, bool deleted)
{
    if (deleted) {
        e->used = 1;
        e->deleted = 1;
        e->t_len = 0;
        e->relid = relid;
        e->block = block;
        e->offset = offset;
        e->csn = csn;
        return true;
    }
    if (tuple == nullptr || tuple->t_data == nullptr) {
        return false;
    }
    if (tuple->t_len == 0 || tuple->t_len > EPOCH_TIP_STAGING_MAX_TUPLE) {
        return false;
    }
    e->used = 1;
    e->deleted = 0;
    e->t_len = static_cast<uint16_t>(tuple->t_len);
    e->relid = relid;
    e->block = block;
    e->offset = offset;
    e->csn = csn;
    errno_t rc = memcpy_s(e->bytes, EPOCH_TIP_STAGING_MAX_TUPLE, tuple->t_data, tuple->t_len);
    securec_check(rc, "\0", "\0");
    return true;
}

static int EpochTipFindSlotLocked(EpochTipEntry* entries, uint32_t eps, Oid relid, BlockNumber block,
    OffsetNumber offset, bool* out_was_used)
{
    int empty = -1;
    uint32_t start = EpochTipSlotStart(relid, block, offset, eps);
    for (uint32_t n = 0; n < eps; ++n) {
        uint32_t i = (start + n) % eps;
        if (EpochTipKeyEqual(entries[i], relid, block, offset)) {
            if (out_was_used != nullptr) {
                *out_was_used = true;
            }
            return static_cast<int>(i);
        }
        if (entries[i].used == 0 && empty < 0) {
            empty = static_cast<int>(i);
        }
    }
    if (out_was_used != nullptr) {
        *out_was_used = false;
    }
    return empty;
}

static bool EpochTipDirtyBitSet(uint8_t* bm, OffsetNumber off)
{
    if (off < FirstOffsetNumber || static_cast<uint32_t>(off) > EPOCH_TIP_DIRTY_BITMAP_BITS) {
        return false;
    }
    uint32_t bit = static_cast<uint32_t>(off) - 1U;
    uint8_t mask = static_cast<uint8_t>(1U << (bit % 8U));
    if ((bm[bit / 8U] & mask) != 0) {
        return false;
    }
    bm[bit / 8U] = static_cast<uint8_t>(bm[bit / 8U] | mask);
    return true;
}

static void EpochTipDirtyClearAll(EpochTipDirtyPage* p)
{
    bool was_used = p->used != 0;
    p->used = 0;
    p->flushing = 0;
    p->dirty_count = 0;
    p->relid = InvalidOid;
    p->block = 0;
    errno_t rc = memset_s(p->offset_bitmap, sizeof(p->offset_bitmap), 0, sizeof(p->offset_bitmap));
    securec_check(rc, "\0", "\0");
    if (was_used && g_epochTipStaging != nullptr) {
        uint32_t prev = g_epochTipStaging->header.dirty_used_slots.load(std::memory_order_relaxed);
        if (prev > 0) {
            g_epochTipStaging->header.dirty_used_slots.store(prev - 1, std::memory_order_relaxed);
        }
    }
}

static void EpochTipDirtyInitPage(EpochTipDirtyPage* p, Oid relid, BlockNumber block, OffsetNumber offset, uint64_t now)
{
    p->used = 1;
    p->flushing = 0;
    p->relid = relid;
    p->block = block;
    p->dirty_count = 0;
    p->last_dirty_us = now;
    errno_t rc = memset_s(p->offset_bitmap, sizeof(p->offset_bitmap), 0, sizeof(p->offset_bitmap));
    securec_check(rc, "\0", "\0");
    if (EpochTipDirtyBitSet(p->offset_bitmap, offset)) {
        p->dirty_count = 1;
    }
    if (g_epochTipStaging != nullptr) {
        g_epochTipStaging->header.dirty_used_slots.fetch_add(1, std::memory_order_relaxed);
    }
}

static void EpochTipDirtyMarkOffset(EpochTipDirtyPage* p, OffsetNumber offset, uint64_t now)
{
    p->last_dirty_us = now;
    if (EpochTipDirtyBitSet(p->offset_bitmap, offset)) {
        p->dirty_count++;
    }
}

static void EpochTipProfAdd(std::atomic<uint64_t>* counter, uint64_t t0_us);

/*
 * Mark (relid,block,offset) dirty. Completes the per-page bitmap (no overflow).
 *
 * Placement is open-addressing from page-hash only (same as FlushPage find).
 * Never park a dirty page in an arbitrary distant slot — that made find miss
 * millions of times.
 *
 * When the hash probe window is full: RequestFlush + sync flush-evict one
 * victim in the same window (no pg_usleep). Experiment showed "RequestFlush
 * + sleep only" left tips unreclaimable (used→370k) and slowed writers.
 * Counters: touch_full / dirty_evict / dirty_evict_us / touch_fail.
 */
static void EpochTipDirtyTouch(Oid relid, BlockNumber block, OffsetNumber offset)
{
    if (g_epochTipStaging == nullptr || !OidIsValid(relid)) {
        return;
    }
    if (offset < FirstOffsetNumber || static_cast<uint32_t>(offset) > EPOCH_TIP_DIRTY_BITMAP_BITS) {
        return;
    }

    static thread_local int touch_depth = 0;
    EpochTipDirtyPage* pages = EpochTipDirtyBase();
    uint32_t n = g_epochTipStaging->header.dirty_num_pages;
    uint32_t start = static_cast<uint32_t>(EpochTipPageHash(relid, block) % n);
    uint64_t now = EpochTipNowUs();

    auto try_probe = [&]() -> bool {
        for (uint32_t probe = 0; probe < EPOCH_TIP_DIRTY_PROBE; ++probe) {
            uint32_t i = (start + probe) % n;
            EpochTipDirtyPage* p = &pages[i];
            SpinLockAcquire(&p->lock);
            if (p->used != 0 && p->relid == relid && p->block == block) {
                EpochTipDirtyMarkOffset(p, offset, now);
                SpinLockRelease(&p->lock);
                return true;
            }
            if (p->used == 0) {
                EpochTipDirtyInitPage(p, relid, block, offset, now);
                SpinLockRelease(&p->lock);
                return true;
            }
            SpinLockRelease(&p->lock);
        }
        return false;
    };

    if (try_probe()) {
        return;
    }

    g_epochTipStaging->header.prof_dirty_touch_full.fetch_add(1, std::memory_order_relaxed);
    EpochTipStagingRequestFlush();
    if (touch_depth > 0) {
        g_epochTipStaging->header.prof_dirty_touch_fail.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    Oid victim_rel = InvalidOid;
    BlockNumber victim_blk = 0;
    bool found_victim = false;
    for (uint32_t probe = 0; probe < EPOCH_TIP_DIRTY_PROBE && !found_victim; ++probe) {
        uint32_t i = (start + probe) % n;
        EpochTipDirtyPage* p = &pages[i];
        SpinLockAcquire(&p->lock);
        if (p->used != 0 && p->flushing == 0 && !(p->relid == relid && p->block == block)) {
            victim_rel = p->relid;
            victim_blk = p->block;
            found_victim = true;
        }
        SpinLockRelease(&p->lock);
    }
    if (!found_victim) {
        g_epochTipStaging->header.prof_dirty_touch_fail.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    touch_depth++;
    g_epochTipStaging->header.prof_dirty_evict.fetch_add(1, std::memory_order_relaxed);
    {
        uint64_t t0 = EpochTipNowUs();
        (void)EpochTipStagingFlushPage(victim_rel, victim_blk);
        EpochTipProfAdd(&g_epochTipStaging->header.prof_dirty_evict_us, t0);
    }
    touch_depth--;

    now = EpochTipNowUs();
    if (!try_probe()) {
        g_epochTipStaging->header.prof_dirty_touch_fail.fetch_add(1, std::memory_order_relaxed);
    }
}

void EpochTipStagingRequestFlush(void)
{
    if (g_epochTipStaging == nullptr) {
        return;
    }
    g_epochTipStaging->header.flush_urgent.store(1, std::memory_order_release);
}

bool EpochTipStagingFlushUrgent(void)
{
    if (g_epochTipStaging == nullptr) {
        return false;
    }
    return g_epochTipStaging->header.flush_urgent.load(std::memory_order_acquire) != 0;
}

void EpochTipStagingClearUrgent(void)
{
    if (g_epochTipStaging == nullptr) {
        return;
    }
    g_epochTipStaging->header.flush_urgent.store(0, std::memory_order_release);
}

uint32_t EpochTipStagingTotalUsed(void)
{
    if (g_epochTipStaging == nullptr) {
        return 0;
    }
    return g_epochTipStaging->header.total_used.load(std::memory_order_relaxed);
}

static void EpochTipProfAdd(std::atomic<uint64_t>* counter, uint64_t t0_us)
{
    if (counter == nullptr) {
        return;
    }
    uint64_t now = EpochTipNowUs();
    if (now >= t0_us) {
        counter->fetch_add(now - t0_us, std::memory_order_relaxed);
    }
}

/*
 * Tip keys are wire/root TIDs. After prune/HOT the LP may be redirect or dead.
 * simple_heap_* PANICs on non-Normal; map to a Normal start TID or fail soft
 * (keep tip in staging).
 */
static bool EpochTipMapToNormalTid(Relation rel, ItemPointer in_tid, ItemPointerData* out_tid)
{
    if (rel == nullptr || in_tid == nullptr || out_tid == nullptr || !ItemPointerIsValid(in_tid)) {
        return false;
    }

    ItemPointerData cur = *in_tid;
    for (int depth = 0; depth < 16; ++depth) {
        BlockNumber blk = ItemPointerGetBlockNumber(&cur);
        if (blk >= RelationGetNumberOfBlocks(rel)) {
            return false;
        }
        Buffer buf = ReadBuffer(rel, blk);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        Page page = BufferGetPage(buf);
        OffsetNumber off = ItemPointerGetOffsetNumber(&cur);
        OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
        if (off < FirstOffsetNumber || off > maxoff) {
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buf);
            return false;
        }
        ItemId lp = PageGetItemId(page, off);
        if (ItemIdIsRedirected(lp)) {
            OffsetNumber next = (OffsetNumber)ItemIdGetRedirect(lp);
            ItemPointerSet(&cur, blk, next);
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buf);
            continue;
        }
        if (!ItemIdIsNormal(lp)) {
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buf);
            return false;
        }
        *out_tid = cur;
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buf);
        return true;
    }
    return false;
}

/*
 * Prepare tip header bytes for heap write: valid t_ctid=self, not HOT/heap-only.
 * Staging copies can carry stale HOT bits; O_heap_update+prune would then TRAP.
 */
static void EpochTipSanitizeHeaderForFlush(char* bytes, uint16_t t_len, ItemPointer tid)
{
    if (bytes == nullptr || t_len < SizeofHeapTupleHeader || tid == nullptr || !ItemPointerIsValid(tid)) {
        return;
    }
    HeapTupleHeader hdr = reinterpret_cast<HeapTupleHeader>(bytes);
    if (hdr->t_hoff > t_len) {
        return;
    }
    hdr->t_ctid = *tid;
    hdr->is_latest = true;
    HeapTupleHeaderClearHotUpdated(hdr);
    HeapTupleHeaderClearHeapOnly(hdr);
}

/*
 * Repair on-page broken HOT before flush write/prune: invalid or cross-page t_ctid.
 */
static void EpochTipRepairOnPageHot(Relation rel, ItemPointer tid)
{
    if (rel == nullptr || tid == nullptr || !ItemPointerIsValid(tid)) {
        return;
    }
    BlockNumber blk = ItemPointerGetBlockNumber(tid);
    Buffer buf = ReadBuffer(rel, blk);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    Page page = BufferGetPage(buf);
    OffsetNumber off = ItemPointerGetOffsetNumber(tid);
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    if (off >= FirstOffsetNumber && off <= maxoff) {
        ItemId lp = PageGetItemId(page, off);
        if (ItemIdIsNormal(lp)) {
            HeapTupleHeader htup = (HeapTupleHeader)PageGetItem(page, lp);
            bool bad_ctid = !ItemPointerIsValid(&htup->t_ctid);
            bool cross_page = ItemPointerIsValid(&htup->t_ctid) && ItemPointerGetBlockNumber(&htup->t_ctid) != blk;
            bool bad_hot = HeapTupleHeaderIsHotUpdated(htup) && (bad_ctid || cross_page);
            if (bad_hot || bad_ctid || cross_page) {
                if (bad_hot || cross_page) {
                    HeapTupleHeaderClearHotUpdated(htup);
                }
                if (bad_ctid || cross_page) {
                    htup->t_ctid = *tid;
                }
                MarkBufferDirty(buf);
            }
        }
    }
    UnlockReleaseBuffer(buf);
}

/* Inplace only onto the live tip (Normal + is_latest); never rewrite old chain versions. */
static bool EpochTipInplaceOne(Relation rel, Oid relid, BlockNumber block, OffsetNumber offset, const char* bytes,
    uint16_t t_len)
{
    if (t_len == 0 || bytes == nullptr) {
        return false;
    }

    ItemPointerData tid;
    ItemPointerSet(&tid, block, offset);

    ItemPointerData normal_tid;
    if (!EpochTipMapToNormalTid(rel, &tid, &normal_tid)) {
        return false;
    }
    /* Inplace must hit the staging key itself when it is already the tip. */
    if (!ItemPointerEquals(&tid, &normal_tid)) {
        return false;
    }

    char local[EPOCH_TIP_STAGING_MAX_TUPLE];
    if (t_len > sizeof(local)) {
        return false;
    }
    errno_t rc = memcpy_s(local, sizeof(local), bytes, t_len);
    securec_check(rc, "\0", "\0");
    EpochTipSanitizeHeaderForFlush(local, t_len, &tid);

    HeapTupleData htup;
    htup.t_len = t_len;
    htup.t_self = tid;
    htup.t_tableOid = relid;
    htup.t_data = reinterpret_cast<HeapTupleHeader>(local);

    Buffer buf = ReadBuffer(rel, block);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    Page page = BufferGetPage(buf);
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    bool len_ok = false;
    if (offset >= FirstOffsetNumber && offset <= maxoff) {
        ItemId lp = PageGetItemId(page, offset);
        if (ItemIdIsNormal(lp)) {
            HeapTupleHeader onpage = (HeapTupleHeader)PageGetItem(page, lp);
            bool is_tip = onpage->is_latest || ItemPointerEquals(&tid, &onpage->t_ctid) ||
                          !ItemPointerIsValid(&onpage->t_ctid);
            if (is_tip) {
                uint32 oldlen = ItemIdGetLength(lp) - onpage->t_hoff;
                uint32 newlen = htup.t_len - htup.t_data->t_hoff;
                len_ok = (oldlen == newlen && onpage->t_hoff == htup.t_data->t_hoff);
            }
        }
    }
    LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buf);

    if (!len_ok) {
        return false;
    }

    EpochTipRepairOnPageHot(rel, &tid);

    bool ok = false;
    PG_TRY();
    {
        heap_inplace_update(rel, &htup, false);
        ok = true;
    }
    PG_CATCH();
    {
        FlushErrorState();
        ok = false;
    }
    PG_END_TRY();
    return ok;
}

/*
 * Materialize tip via epoch CSN version chain (O_heap_update), not simple_heap_*.
 * O_heap_update returns TM_Deleted on non-Normal and re-follows to is_latest under EXCLUSIVE.
 */
static bool EpochTipForceUpdate(Relation rel, ItemPointer tid, const char* bytes, uint16_t t_len, uint64_t csn)
{
    if (bytes == nullptr || t_len == 0 || tid == nullptr || !ItemPointerIsValid(tid)) {
        return false;
    }

    ItemPointerData normal_tid;
    if (!EpochTipMapToNormalTid(rel, tid, &normal_tid)) {
        return false;
    }

    char local[EPOCH_TIP_STAGING_MAX_TUPLE];
    if (t_len > sizeof(local)) {
        return false;
    }
    errno_t rc = memcpy_s(local, sizeof(local), bytes, t_len);
    securec_check(rc, "\0", "\0");
    EpochTipSanitizeHeaderForFlush(local, t_len, &normal_tid);

    HeapTupleData td;
    td.t_len = t_len;
    td.t_self = normal_tid;
    td.t_tableOid = RelationGetRelid(rel);
    td.t_data = reinterpret_cast<HeapTupleHeader>(local);
    HeapTuple tup = heap_copytuple(&td);
    if (tup == nullptr) {
        return false;
    }
    tup->t_data->m_rowCSN = csn;
    tup->t_data->is_latest = true;
    tup->t_data->t_ctid = normal_tid;
    HeapTupleClearHotUpdated(tup);
    HeapTupleClearHeapOnly(tup);

    EpochTipRepairOnPageHot(rel, &normal_tid);

    bool ok = false;
    PG_TRY();
    {
        CommandId cid = GetCurrentCommandId(true);
        TM_Result res = O_heap_update(rel, &normal_tid, tup, cid, csn, nullptr, nullptr);
        if (res == TM_Ok) {
            if (ItemPointerIsValid(&tup->t_self)) {
                O_heap_commit_insert_csn(rel, &tup->t_self, csn);
            }
            ok = true;
        }
    }
    PG_CATCH();
    {
        FlushErrorState();
        ok = false;
    }
    PG_END_TRY();
    heap_freetuple(tup);
    return ok;
}

static bool EpochTipForceDelete(Relation rel, ItemPointer tid, uint64_t csn)
{
    if (tid == nullptr || !ItemPointerIsValid(tid)) {
        return false;
    }

    ItemPointerData normal_tid;
    if (!EpochTipMapToNormalTid(rel, tid, &normal_tid)) {
        return false;
    }

    /* Delete must target the live tip; O_heap_delete does not follow the chain. */
    ItemPointerData latest_tid = normal_tid;
    HeapTuple latest = nullptr;
    if (EpochHeapFetchLatestCopy(rel, &normal_tid, &latest, nullptr) && latest != nullptr) {
        latest_tid = latest->t_self;
        heap_freetuple(latest);
    }

    ItemPointerData check_tid;
    if (!EpochTipMapToNormalTid(rel, &latest_tid, &check_tid)) {
        return false;
    }
    latest_tid = check_tid;

    EpochTipRepairOnPageHot(rel, &latest_tid);

    bool ok = false;
    PG_TRY();
    {
        CommandId cid = GetCurrentCommandId(true);
        TM_Result res = O_heap_delete(rel, &latest_tid, cid, csn);
        ok = (res == TM_Ok);
    }
    PG_CATCH();
    {
        FlushErrorState();
        ok = false;
    }
    PG_END_TRY();
    return ok;
}

static bool EpochTipClearSlot(Oid relid, BlockNumber block, OffsetNumber offset, uint64_t expect_csn)
{
    uint32_t shard_idx = EpochTipShardIndex(relid, block, offset);
    EpochTipShardHeader* shard = EpochTipShardAt(shard_idx);
    EpochTipEntry* entries = EpochTipShardEntries(shard);
    uint32_t eps = g_epochTipStaging->header.entries_per_shard;

    SpinLockAcquire(&shard->lock);
    int slot = EpochTipFindSlotLocked(entries, eps, relid, block, offset, nullptr);
    if (slot < 0 || entries[slot].used == 0) {
        SpinLockRelease(&shard->lock);
        return false;
    }
    if (expect_csn != 0 && entries[slot].csn != expect_csn) {
        /* Newer tip arrived while flushing — keep staging, re-dirty. */
        SpinLockRelease(&shard->lock);
        EpochTipDirtyTouch(relid, block, offset);
        return false;
    }
    entries[slot].used = 0;
    entries[slot].deleted = 0;
    entries[slot].t_len = 0;
    if (shard->used_count > 0) {
        shard->used_count--;
    }
    uint32_t prev = g_epochTipStaging->header.total_used.load(std::memory_order_relaxed);
    if (prev > 0) {
        g_epochTipStaging->header.total_used.store(prev - 1, std::memory_order_relaxed);
    }
    SpinLockRelease(&shard->lock);
    return true;
}

struct EpochTipFlushCand {
    OffsetNumber offset;
    uint8_t deleted;
    uint16_t t_len;
    uint64_t csn;
    char bytes[EPOCH_TIP_STAGING_MAX_TUPLE];
};

/* Collect tip candidates for offsets marked in the page's complete dirty bitmap. */
static uint32_t EpochTipCollectPageCands(Oid relid, BlockNumber block, const uint8_t* bitmap,
    EpochTipFlushCand* cands, uint32_t max_cands)
{
    uint32_t n = 0;
    if (bitmap == nullptr || cands == nullptr || max_cands == 0) {
        return 0;
    }

    for (uint32_t bit = 0; bit < EPOCH_TIP_DIRTY_BITMAP_BITS && n < max_cands; ++bit) {
        if ((bitmap[bit / 8U] & static_cast<uint8_t>(1U << (bit % 8U))) == 0) {
            continue;
        }
        OffsetNumber off = static_cast<OffsetNumber>(bit + 1U);
        uint32_t shard_idx = EpochTipShardIndex(relid, block, off);
        EpochTipShardHeader* shard = EpochTipShardAt(shard_idx);
        EpochTipEntry* entries = EpochTipShardEntries(shard);
        uint32_t eps = g_epochTipStaging->header.entries_per_shard;
        SpinLockAcquire(&shard->lock);
        int slot = EpochTipFindSlotLocked(entries, eps, relid, block, off, nullptr);
        if (slot >= 0 && entries[slot].used != 0) {
            cands[n].offset = off;
            cands[n].deleted = entries[slot].deleted;
            cands[n].csn = entries[slot].csn;
            cands[n].t_len = entries[slot].t_len;
            if (entries[slot].deleted == 0 && entries[slot].t_len > 0) {
                errno_t rc =
                    memcpy_s(cands[n].bytes, sizeof(cands[n].bytes), entries[slot].bytes, entries[slot].t_len);
                securec_check(rc, "\0", "\0");
            }
            n++;
        }
        SpinLockRelease(&shard->lock);
    }
    return n;
}

size_t EpochTipStagingFlushPage(Oid relid, BlockNumber block)
{
    if (g_epochTipStaging == nullptr || !OidIsValid(relid)) {
        return 0;
    }

    EpochTipDirtyPage* pages = EpochTipDirtyBase();
    uint32_t np = g_epochTipStaging->header.dirty_num_pages;
    uint32_t start = static_cast<uint32_t>(EpochTipPageHash(relid, block) % np);
    EpochTipDirtyPage* page = nullptr;
    uint8_t local_bm[EPOCH_TIP_DIRTY_BITMAP_BYTES];
    errno_t rc = memset_s(local_bm, sizeof(local_bm), 0, sizeof(local_bm));
    securec_check(rc, "\0", "\0");

    uint64_t t_find = EpochTipNowUs();
    for (uint32_t probe = 0; probe < EPOCH_TIP_DIRTY_PROBE; ++probe) {
        uint32_t i = (start + probe) % np;
        EpochTipDirtyPage* p = &pages[i];
        SpinLockAcquire(&p->lock);
        if (p->used != 0 && p->relid == relid && p->block == block) {
            if (p->flushing != 0) {
                SpinLockRelease(&p->lock);
                EpochTipProfAdd(&g_epochTipStaging->header.prof_find_page_us, t_find);
                return 0;
            }
            /*
             * Claim: take ownership of current dirty set (like buffer IO).
             * Concurrent DirtyTouch on this page re-dirties into a fresh bitmap.
             */
            p->flushing = 1;
            rc = memcpy_s(local_bm, sizeof(local_bm), p->offset_bitmap, sizeof(p->offset_bitmap));
            securec_check(rc, "\0", "\0");
            rc = memset_s(p->offset_bitmap, sizeof(p->offset_bitmap), 0, sizeof(p->offset_bitmap));
            securec_check(rc, "\0", "\0");
            p->dirty_count = 0;
            page = p;
            SpinLockRelease(&p->lock);
            break;
        }
        SpinLockRelease(&p->lock);
    }
    EpochTipProfAdd(&g_epochTipStaging->header.prof_find_page_us, t_find);
    if (page == nullptr) {
        g_epochTipStaging->header.prof_find_miss.fetch_add(1, std::memory_order_relaxed);
        return 0;
    }

    static constexpr uint32_t MAX_CANDS = EPOCH_TIP_DIRTY_BITMAP_BITS;
    EpochTipFlushCand* cands = static_cast<EpochTipFlushCand*>(palloc(sizeof(EpochTipFlushCand) * MAX_CANDS));
    uint64_t t_collect = EpochTipNowUs();
    uint32_t nc = EpochTipCollectPageCands(relid, block, local_bm, cands, MAX_CANDS);
    EpochTipProfAdd(&g_epochTipStaging->header.prof_collect_us, t_collect);

    size_t flushed = 0;

    if (nc > 0) {
        bool in_xact = IsTransactionState();
        if (!in_xact) {
            uint64_t t_x = EpochTipNowUs();
            StartTransactionCommand();
            PushActiveSnapshot(GetTransactionSnapshot());
            EpochTipProfAdd(&g_epochTipStaging->header.prof_xact_us, t_x);
        }
        uint64_t t_open = EpochTipNowUs();
        Relation rel = try_relation_open(relid, RowExclusiveLock);
        EpochTipProfAdd(&g_epochTipStaging->header.prof_open_us, t_open);
        if (RelationIsValid(rel)) {
            for (uint32_t i = 0; i < nc; ++i) {
                ItemPointerData tid;
                ItemPointerSet(&tid, block, cands[i].offset);
                bool ok = false;
                if (cands[i].deleted != 0) {
                    if (EPOCH_TIP_FLUSH_FORCE_ENABLED) {
                        uint64_t t0 = EpochTipNowUs();
                        ok = EpochTipForceDelete(rel, &tid, cands[i].csn);
                        EpochTipProfAdd(&g_epochTipStaging->header.prof_delete_us, t0);
                        if (ok) {
                            g_epochTipStaging->header.tips_deleted_flush.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                    /* else: force off — delete tip not materialized here */
                } else if (cands[i].t_len > 0) {
                    g_epochTipStaging->header.prof_inplace_try.fetch_add(1, std::memory_order_relaxed);
                    uint64_t t0 = EpochTipNowUs();
                    ok = EpochTipInplaceOne(rel, relid, block, cands[i].offset, cands[i].bytes, cands[i].t_len);
                    EpochTipProfAdd(&g_epochTipStaging->header.prof_inplace_us, t0);
                    if (ok) {
                        g_epochTipStaging->header.tips_inplace.fetch_add(1, std::memory_order_relaxed);
                    } else if (EPOCH_TIP_FLUSH_FORCE_ENABLED) {
                        g_epochTipStaging->header.prof_force_try.fetch_add(1, std::memory_order_relaxed);
                        uint64_t t1 = EpochTipNowUs();
                        ok = EpochTipForceUpdate(rel, &tid, cands[i].bytes, cands[i].t_len, cands[i].csn);
                        EpochTipProfAdd(&g_epochTipStaging->header.prof_force_us, t1);
                        if (ok) {
                            g_epochTipStaging->header.tips_force.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                }
                if (ok) {
                    if (EpochTipClearSlot(relid, block, cands[i].offset, cands[i].csn)) {
                        flushed++;
                    } else {
                        /* Newer tip won the race — already re-dirtied inside ClearSlot. */
                    }
                } else if (EPOCH_TIP_FLUSH_DROP_ON_FAIL) {
                    g_epochTipStaging->header.tips_flush_fail.fetch_add(1, std::memory_order_relaxed);
                    if (EpochTipClearSlot(relid, block, cands[i].offset, cands[i].csn)) {
                        flushed++;
                    }
                } else {
                    /* 堆没写上，tip 仍留在 shard 里给 locate 用。不重新标脏。 */
                    g_epochTipStaging->header.tips_flush_fail.fetch_add(1, std::memory_order_relaxed);
                }
            }
            relation_close(rel, RowExclusiveLock);
        } else {
            /* Could not open relation — put claimed bits back. */
            for (uint32_t bit = 0; bit < EPOCH_TIP_DIRTY_BITMAP_BITS; ++bit) {
                if ((local_bm[bit / 8U] & static_cast<uint8_t>(1U << (bit % 8U))) != 0) {
                    EpochTipDirtyTouch(relid, block, static_cast<OffsetNumber>(bit + 1U));
                }
            }
        }
        if (!in_xact) {
            uint64_t t_x = EpochTipNowUs();
            PopActiveSnapshot();
            CommitTransactionCommand();
            EpochTipProfAdd(&g_epochTipStaging->header.prof_xact_us, t_x);
        } else {
            CommandCounterIncrement();
        }
    }
    pfree(cands);

    SpinLockAcquire(&page->lock);
    page->flushing = 0;
    if (page->used != 0 && page->relid == relid && page->block == block && page->dirty_count == 0) {
        EpochTipDirtyClearAll(page);
    }
    SpinLockRelease(&page->lock);

    if (flushed > 0) {
        g_epochTipStaging->header.pages_flushed.fetch_add(1, std::memory_order_relaxed);
        g_epochTipStaging->header.prof_tips_done.fetch_add(flushed, std::memory_order_relaxed);
    }
    return flushed;
}

uint32_t EpochTipStagingPickDirtyPages(Oid* out_relid, BlockNumber* out_block, uint32_t max_pages)
{
    if (g_epochTipStaging == nullptr || out_relid == nullptr || out_block == nullptr || max_pages == 0) {
        return 0;
    }
    EpochTipDirtyPage* pages = EpochTipDirtyBase();
    uint32_t np = g_epochTipStaging->header.dirty_num_pages;
    uint32_t used = EpochTipStagingTotalUsed();
    bool critical = used * 100U >= EPOCH_TIP_STAGING_NUM_ENTRIES * EPOCH_TIP_FLUSH_CRITICAL_PCT;

    struct Cand {
        uint32_t idx;
        uint32_t dirty_count;
        uint64_t last_dirty_us;
    };
    Cand top[EPOCH_TIP_FLUSH_BUDGET_MAX];
    uint32_t ntop = 0;
    uint32_t cap = max_pages < EPOCH_TIP_FLUSH_BUDGET_MAX ? max_pages : EPOCH_TIP_FLUSH_BUDGET_MAX;

    /*
     * Round-robin window: critical scans a wider stride so hot pages are found
     * quickly without locking all dirty slots every tick.
     */
    uint32_t stride = critical ? (EPOCH_TIP_DIRTY_SCAN_STRIDE * 2U) : EPOCH_TIP_DIRTY_SCAN_STRIDE;
    if (stride > np) {
        stride = np;
    }
    /* Parallel flushers: each tick claims a disjoint window via fetch_add. */
    uint32_t start =
        g_epochTipStaging->header.dirty_scan_cursor.fetch_add(stride, std::memory_order_relaxed) % np;

    uint64_t t_pick = EpochTipNowUs();
    for (uint32_t probe = 0; probe < stride; ++probe) {
        uint32_t i = (start + probe) % np;
        EpochTipDirtyPage* p = &pages[i];
        SpinLockAcquire(&p->lock);
        bool take = p->used != 0 && p->flushing == 0 && p->dirty_count > 0;
        Cand c{};
        if (take) {
            c.idx = i;
            c.dirty_count = p->dirty_count;
            c.last_dirty_us = p->last_dirty_us;
        }
        SpinLockRelease(&p->lock);
        if (!take) {
            continue;
        }
        if (ntop < cap) {
            top[ntop++] = c;
        } else {
            uint32_t weak = 0;
            for (uint32_t j = 1; j < ntop; ++j) {
                if (critical) {
                    if (top[j].dirty_count < top[weak].dirty_count) {
                        weak = j;
                    }
                } else if (top[j].last_dirty_us > top[weak].last_dirty_us) {
                    weak = j;
                }
            }
            bool better = critical ? (c.dirty_count > top[weak].dirty_count)
                                   : (c.last_dirty_us < top[weak].last_dirty_us);
            if (better) {
                top[weak] = c;
            }
        }
    }

    uint32_t out_n = 0;
    for (uint32_t j = 0; j < ntop && out_n < max_pages; ++j) {
        EpochTipDirtyPage* p = &pages[top[j].idx];
        SpinLockAcquire(&p->lock);
        if (p->used != 0 && p->flushing == 0) {
            out_relid[out_n] = p->relid;
            out_block[out_n] = p->block;
            out_n++;
        }
        SpinLockRelease(&p->lock);
    }
    EpochTipProfAdd(&g_epochTipStaging->header.prof_pick_us, t_pick);
    return out_n;
}

size_t EpochTipStagingFlusherTick(void)
{
    if (g_epochTipStaging == nullptr || !g_epoch_local_tip_staging) {
        return 0;
    }
    uint64_t t_tick = EpochTipNowUs();
    uint32_t used = EpochTipStagingTotalUsed();
    uint32_t low = (EPOCH_TIP_STAGING_NUM_ENTRIES * EPOCH_TIP_FLUSH_LOW_PCT) / 100U;
    uint32_t high = (EPOCH_TIP_STAGING_NUM_ENTRIES * EPOCH_TIP_FLUSH_HIGH_PCT) / 100U;
    uint32_t critical = (EPOCH_TIP_STAGING_NUM_ENTRIES * EPOCH_TIP_FLUSH_CRITICAL_PCT) / 100U;
    bool urgent = EpochTipStagingFlushUrgent();

    if (!urgent && used < low) {
        return 0;
    }

    uint32_t budget = 16;
    uint32_t passes = 1;
    if (used >= critical || urgent) {
        budget = EPOCH_TIP_FLUSH_BUDGET_MAX;
        passes = 8;
    } else if (used >= high) {
        budget = 128;
        passes = 4;
    } else if (used >= low) {
        budget = 64;
        passes = 2;
    }

    Oid relids[EPOCH_TIP_FLUSH_BUDGET_MAX];
    BlockNumber blocks[EPOCH_TIP_FLUSH_BUDGET_MAX];
    size_t total = 0;
    bool started_xact = false;

    for (uint32_t pass = 0; pass < passes; ++pass) {
        uint32_t n = EpochTipStagingPickDirtyPages(relids, blocks, budget);
        if (n == 0) {
            break;
        }
        if (!started_xact) {
            uint64_t t_x = EpochTipNowUs();
            StartTransactionCommand();
            PushActiveSnapshot(GetTransactionSnapshot());
            EpochTipProfAdd(&g_epochTipStaging->header.prof_xact_us, t_x);
            started_xact = true;
        }
        for (uint32_t i = 0; i < n; ++i) {
            total += EpochTipStagingFlushPage(relids[i], blocks[i]);
        }
        used = EpochTipStagingTotalUsed();
        if (!urgent && used < high) {
            break;
        }
        if (pass + 1 < passes && (used >= critical || urgent)) {
            uint64_t t_x = EpochTipNowUs();
            PopActiveSnapshot();
            CommitTransactionCommand();
            StartTransactionCommand();
            PushActiveSnapshot(GetTransactionSnapshot());
            EpochTipProfAdd(&g_epochTipStaging->header.prof_xact_us, t_x);
        }
    }

    if (started_xact) {
        uint64_t t_x = EpochTipNowUs();
        PopActiveSnapshot();
        CommitTransactionCommand();
        EpochTipProfAdd(&g_epochTipStaging->header.prof_xact_us, t_x);
    }

    if (urgent && EpochTipStagingTotalUsed() < high) {
        EpochTipStagingClearUrgent();
    }
    EpochTipProfAdd(&g_epochTipStaging->header.prof_tick_us, t_tick);
    return total;
}

void EpochTipStagingGetFlushCounters(uint64_t* inplace, uint64_t* force, uint64_t* deleted, uint64_t* fail,
    uint64_t* pages, uint64_t* shard_full)
{
    if (g_epochTipStaging == nullptr) {
        if (inplace != nullptr) {
            *inplace = 0;
        }
        if (force != nullptr) {
            *force = 0;
        }
        if (deleted != nullptr) {
            *deleted = 0;
        }
        if (fail != nullptr) {
            *fail = 0;
        }
        if (pages != nullptr) {
            *pages = 0;
        }
        if (shard_full != nullptr) {
            *shard_full = 0;
        }
        return;
    }
    if (inplace != nullptr) {
        *inplace = g_epochTipStaging->header.tips_inplace.load(std::memory_order_relaxed);
    }
    if (force != nullptr) {
        *force = g_epochTipStaging->header.tips_force.load(std::memory_order_relaxed);
    }
    if (deleted != nullptr) {
        *deleted = g_epochTipStaging->header.tips_deleted_flush.load(std::memory_order_relaxed);
    }
    if (fail != nullptr) {
        *fail = g_epochTipStaging->header.tips_flush_fail.load(std::memory_order_relaxed);
    }
    if (pages != nullptr) {
        *pages = g_epochTipStaging->header.pages_flushed.load(std::memory_order_relaxed);
    }
    if (shard_full != nullptr) {
        *shard_full = g_epochTipStaging->header.shard_full_events.load(std::memory_order_relaxed);
    }
}

void EpochTipStagingGetFlushProfile(EpochTipFlushProfile* out)
{
    if (out == nullptr) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (g_epochTipStaging == nullptr) {
        return;
    }
    out->tick_us = g_epochTipStaging->header.prof_tick_us.load(std::memory_order_relaxed);
    out->pick_us = g_epochTipStaging->header.prof_pick_us.load(std::memory_order_relaxed);
    out->find_page_us = g_epochTipStaging->header.prof_find_page_us.load(std::memory_order_relaxed);
    out->collect_us = g_epochTipStaging->header.prof_collect_us.load(std::memory_order_relaxed);
    out->open_us = g_epochTipStaging->header.prof_open_us.load(std::memory_order_relaxed);
    out->inplace_us = g_epochTipStaging->header.prof_inplace_us.load(std::memory_order_relaxed);
    out->force_us = g_epochTipStaging->header.prof_force_us.load(std::memory_order_relaxed);
    out->delete_us = g_epochTipStaging->header.prof_delete_us.load(std::memory_order_relaxed);
    out->xact_us = g_epochTipStaging->header.prof_xact_us.load(std::memory_order_relaxed);
    out->dirty_evict = g_epochTipStaging->header.prof_dirty_evict.load(std::memory_order_relaxed);
    out->dirty_evict_us = g_epochTipStaging->header.prof_dirty_evict_us.load(std::memory_order_relaxed);
    out->dirty_touch_full = g_epochTipStaging->header.prof_dirty_touch_full.load(std::memory_order_relaxed);
    out->dirty_touch_fail = g_epochTipStaging->header.prof_dirty_touch_fail.load(std::memory_order_relaxed);
    out->find_miss = g_epochTipStaging->header.prof_find_miss.load(std::memory_order_relaxed);
    out->inplace_try = g_epochTipStaging->header.prof_inplace_try.load(std::memory_order_relaxed);
    out->force_try = g_epochTipStaging->header.prof_force_try.load(std::memory_order_relaxed);
    out->tips_done = g_epochTipStaging->header.prof_tips_done.load(std::memory_order_relaxed);
}

static void EpochTipLogShardFullThrottled(Oid relid, BlockNumber block, OffsetNumber offset)
{
    uint64_t n = g_epochTipStaging->header.shard_full_events.fetch_add(1, std::memory_order_relaxed) + 1;
    uint64_t now = EpochTipNowUs();
    uint64_t last = g_epochTipStaging->header.shard_full_last_log_us.load(std::memory_order_relaxed);
    if (last != 0 && now - last < 5000000ULL && (n % 10000ULL) != 0) {
        return;
    }
    if (g_epochTipStaging->header.shard_full_last_log_us.compare_exchange_strong(last, now, std::memory_order_relaxed)) {
        ereport(WARNING,
            (errmsg("epoch tip staging shard full (events=%lu sample rel=%u blk=%u off=%u used=%u)",
                (unsigned long)n,
                relid,
                block,
                offset,
                EpochTipStagingTotalUsed())));
    }
}

static bool EpochTipUpsertOrDelete(Relation rel, ItemPointer tid, HeapTuple tuple, uint64_t csn, bool deleted)
{
    if (g_epochTipStaging == nullptr || rel == nullptr || tid == nullptr || !ItemPointerIsValid(tid)) {
        return false;
    }
    Oid relid = RelationGetRelid(rel);
    BlockNumber block = ItemPointerGetBlockNumber(tid);
    OffsetNumber offset = ItemPointerGetOffsetNumber(tid);
    if (!deleted && (tuple == nullptr || tuple->t_data == nullptr || tuple->t_len == 0 ||
                        tuple->t_len > EPOCH_TIP_STAGING_MAX_TUPLE)) {
        return false;
    }

    uint32_t shard_idx = EpochTipShardIndex(relid, block, offset);
    EpochTipShardHeader* shard = EpochTipShardAt(shard_idx);
    EpochTipEntry* entries = EpochTipShardEntries(shard);
    uint32_t eps = g_epochTipStaging->header.entries_per_shard;

    /*
     * Brief retries after waking the flusher — never do inplace flush on the
     * apply hot path. Longer yields when full give the flusher time to drain.
     */
    static const long kRetrySleepUs[3] = {100L, 400L, 1000L};
    for (int attempt = 0; attempt < 3; ++attempt) {
        SpinLockAcquire(&shard->lock);
        bool was_used = false;
        int slot = EpochTipFindSlotLocked(entries, eps, relid, block, offset, &was_used);
        if (slot < 0) {
            SpinLockRelease(&shard->lock);
            EpochTipStagingRequestFlush();
            if (attempt + 1 < 3) {
                pg_usleep(kRetrySleepUs[attempt]);
                continue;
            }
            EpochTipLogShardFullThrottled(relid, block, offset);
            return false;
        }
        if (!EpochTipFillEntry(&entries[slot], relid, block, offset, tuple, csn, deleted)) {
            SpinLockRelease(&shard->lock);
            return false;
        }
        if (!was_used) {
            shard->used_count++;
            g_epochTipStaging->header.total_used.fetch_add(1, std::memory_order_relaxed);
        }
        SpinLockRelease(&shard->lock);
        EpochTipDirtyTouch(relid, block, offset);

        uint32_t used = EpochTipStagingTotalUsed();
        uint32_t high = (EPOCH_TIP_STAGING_NUM_ENTRIES * EPOCH_TIP_FLUSH_HIGH_PCT) / 100U;
        if (used >= high) {
            EpochTipStagingRequestFlush();
        }
        return true;
    }
    return false;
}

bool EpochTipStagingUpsert(Relation rel, ItemPointer tid, HeapTuple tuple, uint64_t csn)
{
    return EpochTipUpsertOrDelete(rel, tid, tuple, csn, false);
}

bool EpochTipStagingDelete(Relation rel, ItemPointer tid, uint64_t csn)
{
    return EpochTipUpsertOrDelete(rel, tid, nullptr, csn, true);
}

bool EpochTipStagingLookupCopy(Relation rel, ItemPointer tid, HeapTuple* out_copy, bool* is_deleted)
{
    if (out_copy != nullptr) {
        *out_copy = nullptr;
    }
    if (is_deleted != nullptr) {
        *is_deleted = false;
    }
    if (g_epochTipStaging == nullptr || rel == nullptr || tid == nullptr || !ItemPointerIsValid(tid)) {
        return false;
    }
    Oid relid = RelationGetRelid(rel);
    BlockNumber block = ItemPointerGetBlockNumber(tid);
    OffsetNumber offset = ItemPointerGetOffsetNumber(tid);
    uint32_t shard_idx = EpochTipShardIndex(relid, block, offset);
    EpochTipShardHeader* shard = EpochTipShardAt(shard_idx);
    EpochTipEntry* entries = EpochTipShardEntries(shard);
    uint32_t eps = g_epochTipStaging->header.entries_per_shard;

    char local_bytes[EPOCH_TIP_STAGING_MAX_TUPLE];
    uint16_t local_len = 0;
    bool local_deleted = false;
    bool found = false;

    SpinLockAcquire(&shard->lock);
    uint32_t start = EpochTipSlotStart(relid, block, offset, eps);
    for (uint32_t n = 0; n < eps; ++n) {
        uint32_t i = (start + n) % eps;
        if (!EpochTipKeyEqual(entries[i], relid, block, offset)) {
            continue;
        }
        found = true;
        local_deleted = entries[i].deleted != 0;
        local_len = entries[i].t_len;
        if (!local_deleted && local_len > 0 && local_len <= EPOCH_TIP_STAGING_MAX_TUPLE) {
            errno_t rc = memcpy_s(local_bytes, sizeof(local_bytes), entries[i].bytes, local_len);
            securec_check(rc, "\0", "\0");
        } else {
            local_len = 0;
        }
        break;
    }
    SpinLockRelease(&shard->lock);

    if (!found) {
        return false;
    }
    if (is_deleted != nullptr) {
        *is_deleted = local_deleted;
    }
    if (local_deleted || local_len == 0) {
        return true;
    }
    if (out_copy == nullptr) {
        return true;
    }
    HeapTupleData td;
    td.t_len = local_len;
    td.t_self = *tid;
    td.t_tableOid = relid;
    td.t_data = reinterpret_cast<HeapTupleHeader>(local_bytes);
    *out_copy = heap_copytuple(&td);
    return *out_copy != nullptr;
}

bool EpochTipStagingHasEntry(Relation rel, ItemPointer tid)
{
    HeapTuple copy = nullptr;
    bool deleted = false;
    if (!EpochTipStagingLookupCopy(rel, tid, &copy, &deleted)) {
        return false;
    }
    if (copy != nullptr) {
        heap_freetuple(copy);
    }
    return true;
}
