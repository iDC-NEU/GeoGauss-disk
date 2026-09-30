#ifndef OPENGAUSS_E_HEAP_VERSION_H
#define OPENGAUSS_E_HEAP_VERSION_H

#include "postgres.h"

#include "access/htup.h"
#include "utils/rel.h"

struct ItemPointerData;

/*
 * epoch CSN 版本链：索引/哨兵 TID 为链头，沿 t_ctid 正向前进直到 is_latest 或匹配 pinned CSN。
 * 读路径仅 BUFFER_LOCK_SHARE，不等待 apply 的 EXCLUSIVE 写新页。
 * m_status 存版本 commit logical epoch，供 read_frontier 过滤。
 */
#define EPOCH_HEAP_VERSION_CHAIN_MAX 128

extern bool EpochHeapVersionChainEnabled(Relation rel);

extern bool EpochHeapFetchPinnedCsnCopy(Relation rel, const ItemPointerData* root_tid, uint64_t pinned_csn,
    HeapTuple* out_copy);

extern bool EpochHeapFetchLatestCopy(Relation rel, const ItemPointerData* root_tid, HeapTuple* out_copy,
    int* out_chain_depth);

extern bool EpochHeapFetchFrontierCopy(Relation rel, const ItemPointerData* root_tid, uint64_t max_epoch,
    HeapTuple* out_copy);

extern bool EpochHeapReadPinnedCsn(Relation rel, const ItemPointerData* root_tid, uint64_t pinned_csn,
    uint64_t* out_csn, bool* out_found);

/* 非 latest 且 t_ctid 指向下游：已被 apply 新版本取代，seq scan 应跳过 */
extern bool EpochHeapIsSupersededVersion(Relation rel, const HeapTupleHeader hdr, ItemPointer self);

/*
 * 将 dest 升级为链上 read_frontier 可见的最新版本（索引/TID 读路径）。
 * dest_storage 存放 tuple body（如 scan->xs_ctbuf_hdr）；已是 latest 时 no-op 返回 true。
 */
extern bool EpochHeapMaterializeLatestRead(Relation rel, const ItemPointerData* chain_root, HeapTuple dest,
    HeapTupleHeader dest_storage);

/*
 * 在已持 EXCLUSIVE 的 buffer 上回收同页中间版本。
 * tip_tid 非空时只沿着指向 tip 的同页反向链回收（stock 热点路径）；否则扫全页但受 max_items 限制。
 * max_items<=0 表示使用默认 EPOCH_PRUNE_HOT_MAX_ITEMS。
 */
extern Size EpochHeapPrunePage(Relation rel, Buffer buffer, uint64_t f_safe);
extern Size EpochHeapPrunePageLimited(Relation rel, Buffer buffer, uint64_t f_safe,
    const ItemPointerData* tip_tid, int max_items);

/*
 * 在 EXCLUSIVE 锁下沿 t_ctid 追到 is_latest tip（可跨页，会换 buffer）。
 * *buffer_io 输入为 otid 所在页的已加锁 buffer；输出为 tip 所在页的已加锁 buffer。
 */
extern bool EpochHeapLockFollowToTip(Relation rel, ItemPointer otid, Buffer* buffer_io, HeapTupleData* oldtup);

#endif /* OPENGAUSS_E_HEAP_VERSION_H */
