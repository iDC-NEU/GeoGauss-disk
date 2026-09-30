#include "postgres.h"

#include "epoch/e_heap_version.h"
#include "epoch/e_apply_buflock.h"
#include "epoch/e_config.h"
#include "epoch/e_read_frontier.h"
#include "epoch/e_types.h"

#include "occ/occ_internal.h"
#include "occ/o_staging.h"

#include "access/heapam.h"
#include "catalog/catalog.h"
#include "storage/buf/bufmgr.h"
#include "storage/buf/bufpage.h"
#include "storage/freespace.h"
#include "storage/item/itemid.h"
#include "utils/rel.h"

#include "securec.h"
#include "securec_check.h"

/*
 * Phase 1 本地 tip staging：优先返回 staging 中以 root TID 为键的 tip。
 * 返回：true=已决断（命中 tip 或 tombstone）；false=未命中，继续追堆。
 * 命中 tip：*out_copy 非空；tombstone：*out_copy 为空且函数返回 true。
 */
static bool EpochHeapTryStagingLatestResolved(Relation rel, const ItemPointerData* root_tid, HeapTuple* out_copy,
    int* out_chain_depth)
{
    if (!g_epoch_local_tip_staging || rel == nullptr || root_tid == nullptr || out_copy == nullptr) {
        return false;
    }
    OccStagingInitEpochLocalTip();
    HeapTuple staged = nullptr;
    bool deleted = false;
    if (!OccStagingTryReadTuple(rel, const_cast<ItemPointer>(root_tid), &staged, &deleted)) {
        return false;
    }
    if (out_chain_depth != nullptr) {
        *out_chain_depth = 0;
    }
    if (deleted || staged == nullptr) {
        if (staged != nullptr) {
            heap_freetuple(staged);
        }
        *out_copy = nullptr;
        return true; /* 已决断：不可见 */
    }
    *out_copy = staged;
    return true;
}

bool EpochHeapVersionChainEnabled(Relation rel)
{
    if (rel == nullptr) {
        return false;
    }
    if (!g_enable_epoch_multimaster) {
        return false;
    }
    if (RelationIsUstoreFormat(rel)) {
        return false;
    }
    /*
     * 系统表 / toast 从未走 CSN 版本链；is_latest 默认为 0。
     * 若对它们启用链语义，seq/index 会把 pg_authid 等全部当 superseded，
     * 启动期出现 invalid role OID: 10 / could not open critical system index。
     */
    if (IsSystemRelation(rel) || IsCatalogRelation(rel) || IsToastRelation(rel)) {
        return false;
    }
    return O_HeapUseCsnEpochSemantics();
}

static bool EpochHeapCopyTupleOnBuffer(Relation rel, Buffer buffer, const HeapTupleData* tp, HeapTuple* out_copy)
{
    if (out_copy == nullptr || tp == nullptr || tp->t_data == nullptr) {
        return false;
    }
    HeapTuple copy = heap_copytuple(const_cast<HeapTuple>(tp));
    if (copy == nullptr) {
        return false;
    }
    copy->t_tableOid = RelationGetRelid(rel);
    copy->t_bucketId = RelationGetBktid(rel);
    *out_copy = copy;
    (void)buffer;
    return true;
}

/* 链断裂/触顶时：若已收集到 frontier 可见版本则返回，避免读路径 not found */
static bool EpochHeapReturnFrontierCopy(HeapTuple frontier_copy, HeapTuple* out_copy, uint64_t* out_csn, bool* out_found,
    int* out_chain_depth, int depth)
{
    if (frontier_copy == nullptr) {
        return false;
    }
    if (out_copy != nullptr) {
        *out_copy = frontier_copy;
    } else {
        heap_freetuple(frontier_copy);
    }
    if (out_csn != nullptr && out_copy != nullptr && *out_copy != nullptr) {
        *out_csn = (*out_copy)->t_data->m_rowCSN;
    }
    if (out_found != nullptr) {
        *out_found = true;
    }
    if (out_chain_depth != nullptr) {
        *out_chain_depth = depth;
    }
    return true;
}

static bool EpochHeapVersionVisibleAtEpoch(const HeapTupleHeader hdr, uint64_t max_epoch)
{
    if (hdr == nullptr) {
        return false;
    }
    if (max_epoch == 0) {
        return true;
    }
    /* m_status=0：升级前数据，视为可见 */
    if (hdr->m_status == 0) {
        return true;
    }
    return hdr->m_status <= max_epoch;
}

/*
 * 正向版本链：索引/哨兵 TID 为链头，沿 t_ctid 前进直到 is_latest 或匹配 pinned CSN。
 * 有 read_frontier 时收集链上 m_status≤frontier 的最新可见版本；
 * 一旦后继对 frontier 不可见则停链，避免继续 SHARE 追到 tip 与 apply 写互堵。
 */
static bool EpochHeapFollowChain(Relation rel, const ItemPointerData* root_tid, uint64_t pinned_csn, bool want_latest,
    uint64_t max_epoch, HeapTuple* out_copy, uint64_t* out_csn, bool* out_found, int* out_chain_depth)
{
    ItemPointerData ctid;
    int depth = 0;

    if (out_copy != nullptr) {
        *out_copy = nullptr;
    }
    if (out_csn != nullptr) {
        *out_csn = 0;
    }
    if (out_found != nullptr) {
        *out_found = false;
    }
    if (out_chain_depth != nullptr) {
        *out_chain_depth = 0;
    }
    if (rel == nullptr || root_tid == nullptr || !ItemPointerIsValid(root_tid)) {
        return false;
    }

    ctid = *root_tid;

    HeapTuple frontier_copy = nullptr;

    for (;;) {
        Buffer buffer;
        Page page;
        OffsetNumber offnum;
        ItemId lp;
        HeapTupleData tp;
        HeapTupleHeader hdr;

        if (depth++ > EPOCH_HEAP_VERSION_CHAIN_MAX) {
            if (out_chain_depth != nullptr) {
                *out_chain_depth = depth - 1;
            }
            /* 触顶时若已有 frontier 可见版本，仍返回，避免读路径变 not found */
            if (want_latest && max_epoch != 0 &&
                EpochHeapReturnFrontierCopy(frontier_copy, out_copy, out_csn, out_found, out_chain_depth, depth - 1)) {
                return true;
            }
            if (frontier_copy != nullptr) {
                heap_freetuple(frontier_copy);
            }
            return false;
        }

        if (ItemPointerGetBlockNumber(&ctid) >= RelationGetNumberOfBlocks(rel)) {
            if (want_latest && max_epoch != 0 &&
                EpochHeapReturnFrontierCopy(frontier_copy, out_copy, out_csn, out_found, out_chain_depth, depth)) {
                return true;
            }
            if (frontier_copy != nullptr) {
                heap_freetuple(frontier_copy);
            }
            return false;
        }

        buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(&ctid));
        EpochApplyBufLockShare(buffer);
        page = BufferGetPage(buffer);
        offnum = ItemPointerGetOffsetNumber(&ctid);
        if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page)) {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buffer);
            if (want_latest && max_epoch != 0 &&
                EpochHeapReturnFrontierCopy(frontier_copy, out_copy, out_csn, out_found, out_chain_depth, depth)) {
                return true;
            }
            if (frontier_copy != nullptr) {
                heap_freetuple(frontier_copy);
            }
            return false;
        }
        lp = PageGetItemId(page, offnum);
        if (!ItemIdIsNormal(lp)) {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buffer);
            /* 悬空 t_ctid（如跨页 prune 误删）：尽量退回 frontier 可见版本 */
            if (want_latest && max_epoch != 0 &&
                EpochHeapReturnFrontierCopy(frontier_copy, out_copy, out_csn, out_found, out_chain_depth, depth)) {
                return true;
            }
            if (frontier_copy != nullptr) {
                heap_freetuple(frontier_copy);
            }
            return false;
        }

        tp.t_self = ctid;
        tp.t_data = (HeapTupleHeader)PageGetItem(page, lp);
        tp.t_len = ItemIdGetLength(lp);
        tp.t_tableOid = RelationGetRelid(rel);
        tp.t_bucketId = RelationGetBktid(rel);
        HeapTupleCopyBaseFromPage(&tp, page);
        hdr = tp.t_data;

        if (max_epoch != 0 && EpochHeapVersionVisibleAtEpoch(hdr, max_epoch)) {
            if (frontier_copy != nullptr) {
                heap_freetuple(frontier_copy);
                frontier_copy = nullptr;
            }
            (void)EpochHeapCopyTupleOnBuffer(rel, buffer, &tp, &frontier_copy);
        }

        if (want_latest && hdr->is_latest) {
            bool ok = true;
            HeapTuple result = nullptr;
            if (max_epoch != 0) {
                if (frontier_copy != nullptr) {
                    result = frontier_copy;
                    frontier_copy = nullptr;
                } else if (EpochHeapVersionVisibleAtEpoch(hdr, max_epoch)) {
                    ok = EpochHeapCopyTupleOnBuffer(rel, buffer, &tp, &result);
                } else {
                    ok = false;
                }
            } else {
                ok = EpochHeapCopyTupleOnBuffer(rel, buffer, &tp, &result);
            }
            if (ok && out_copy != nullptr) {
                *out_copy = result;
                result = nullptr;
            } else if (result != nullptr) {
                heap_freetuple(result);
            }
            if (ok && out_csn != nullptr && out_copy != nullptr && *out_copy != nullptr) {
                *out_csn = (*out_copy)->t_data->m_rowCSN;
            }
            if (ok && out_found != nullptr) {
                *out_found = true;
            }
            if (frontier_copy != nullptr) {
                heap_freetuple(frontier_copy);
            }
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buffer);
            if (out_chain_depth != nullptr) {
                *out_chain_depth = depth;
            }
            return ok;
        }

        if (!want_latest && hdr->m_rowCSN == pinned_csn) {
            bool ok = true;
            if (out_copy != nullptr) {
                ok = EpochHeapCopyTupleOnBuffer(rel, buffer, &tp, out_copy);
            }
            if (ok && out_csn != nullptr) {
                *out_csn = hdr->m_rowCSN;
            }
            if (ok && out_found != nullptr) {
                *out_found = true;
            }
            if (frontier_copy != nullptr) {
                heap_freetuple(frontier_copy);
            }
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buffer);
            if (out_chain_depth != nullptr) {
                *out_chain_depth = depth;
            }
            return ok;
        }

        if (ItemPointerEquals(&tp.t_self, &hdr->t_ctid)) {
            bool ok = false;
            if (want_latest) {
                if (max_epoch != 0) {
                    if (frontier_copy != nullptr) {
                        ok = true;
                        if (out_copy != nullptr) {
                            *out_copy = frontier_copy;
                            frontier_copy = nullptr;
                        }
                        if (out_csn != nullptr && out_copy != nullptr && *out_copy != nullptr) {
                            *out_csn = (*out_copy)->t_data->m_rowCSN;
                        }
                    } else if (EpochHeapVersionVisibleAtEpoch(hdr, max_epoch)) {
                        ok = true;
                        if (out_copy != nullptr) {
                            ok = EpochHeapCopyTupleOnBuffer(rel, buffer, &tp, out_copy);
                        }
                        if (ok && out_csn != nullptr) {
                            *out_csn = hdr->m_rowCSN;
                        }
                    }
                } else {
                    ok = true;
                    if (out_copy != nullptr) {
                        ok = EpochHeapCopyTupleOnBuffer(rel, buffer, &tp, out_copy);
                    }
                    if (ok && out_csn != nullptr) {
                        *out_csn = hdr->m_rowCSN;
                    }
                }
                if (out_found != nullptr) {
                    *out_found = ok;
                }
            }
            if (frontier_copy != nullptr) {
                heap_freetuple(frontier_copy);
            }
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buffer);
            if (out_chain_depth != nullptr) {
                *out_chain_depth = depth;
            }
            return want_latest && ok;
        }

        if (hdr->is_latest) {
            if (frontier_copy != nullptr) {
                heap_freetuple(frontier_copy);
            }
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(buffer);
            return false;
        }

        /*
         * frontier 读：已持有可见副本时，若后继对 max_epoch 不可见则停。
         * 否则会继续 SHARE 锁 tip 页，与 apply 的 EXCLUSIVE 更新互堵，
         * 尽管逻辑上读者只需旧版本（MVCC 语义被页锁抵消）。
         */
        if (want_latest && max_epoch != 0 && frontier_copy != nullptr) {
            ItemPointerData next_tid = hdr->t_ctid;
            BlockNumber cur_blk = BufferGetBlockNumber(buffer);
            BlockNumber next_blk = ItemPointerGetBlockNumber(&next_tid);
            OffsetNumber next_off = ItemPointerGetOffsetNumber(&next_tid);
            bool stop_here = false;

            if (next_blk == cur_blk) {
                OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
                if (next_off < FirstOffsetNumber || next_off > maxoff) {
                    stop_here = true;
                } else {
                    ItemId nlp = PageGetItemId(page, next_off);
                    if (!ItemIdIsNormal(nlp)) {
                        stop_here = true;
                    } else {
                        HeapTupleHeader nhdr = (HeapTupleHeader)PageGetItem(page, nlp);
                        if (!EpochHeapVersionVisibleAtEpoch(nhdr, max_epoch)) {
                            stop_here = true;
                        }
                    }
                }
                if (stop_here) {
                    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                    ReleaseBuffer(buffer);
                    return EpochHeapReturnFrontierCopy(frontier_copy, out_copy, out_csn, out_found, out_chain_depth,
                        depth);
                }
            } else {
                /* 跨页：先释放当前页，再短窥后继；不可见则不再持 tip 锁返回 */
                LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                ReleaseBuffer(buffer);
                buffer = InvalidBuffer;

                if (next_blk >= RelationGetNumberOfBlocks(rel)) {
                    return EpochHeapReturnFrontierCopy(frontier_copy, out_copy, out_csn, out_found, out_chain_depth,
                        depth);
                }

                Buffer nbuf = ReadBuffer(rel, next_blk);
                EpochApplyBufLockShare(nbuf);
                Page npage = BufferGetPage(nbuf);
                OffsetNumber nmax = PageGetMaxOffsetNumber(npage);
                if (next_off < FirstOffsetNumber || next_off > nmax) {
                    stop_here = true;
                } else {
                    ItemId nlp = PageGetItemId(npage, next_off);
                    if (!ItemIdIsNormal(nlp)) {
                        stop_here = true;
                    } else {
                        HeapTupleHeader nhdr = (HeapTupleHeader)PageGetItem(npage, nlp);
                        if (!EpochHeapVersionVisibleAtEpoch(nhdr, max_epoch)) {
                            stop_here = true;
                        }
                    }
                }
                LockBuffer(nbuf, BUFFER_LOCK_UNLOCK);
                ReleaseBuffer(nbuf);

                if (stop_here) {
                    return EpochHeapReturnFrontierCopy(frontier_copy, out_copy, out_csn, out_found, out_chain_depth,
                        depth);
                }
                /* 后继仍可见：从后继继续，勿再锁回旧页 */
                ctid = next_tid;
                continue;
            }
        }

        ctid = hdr->t_ctid;
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
    }
}

bool EpochHeapFetchPinnedCsnCopy(Relation rel, const ItemPointerData* root_tid, uint64_t pinned_csn,
    HeapTuple* out_copy)
{
    if (!EpochHeapVersionChainEnabled(rel)) {
        return false;
    }
    return EpochHeapFollowChain(rel, root_tid, pinned_csn, false, 0, out_copy, nullptr, nullptr, nullptr);
}

bool EpochHeapFetchLatestCopy(Relation rel, const ItemPointerData* root_tid, HeapTuple* out_copy, int* out_chain_depth)
{
    if (!EpochHeapVersionChainEnabled(rel)) {
        return false;
    }
    if (EpochHeapTryStagingLatestResolved(rel, root_tid, out_copy, out_chain_depth)) {
        return out_copy != nullptr && *out_copy != nullptr;
    }
    uint64_t max_epoch = 0;
    if (EpochReadFrontierActive()) {
        max_epoch = EpochGetReadFrontier();
    }
    return EpochHeapFollowChain(rel, root_tid, 0, true, max_epoch, out_copy, nullptr, nullptr, out_chain_depth);
}

bool EpochHeapFetchFrontierCopy(Relation rel, const ItemPointerData* root_tid, uint64_t max_epoch, HeapTuple* out_copy)
{
    if (!EpochHeapVersionChainEnabled(rel)) {
        return false;
    }
    return EpochHeapFollowChain(rel, root_tid, 0, true, max_epoch, out_copy, nullptr, nullptr, nullptr);
}

bool EpochHeapReadPinnedCsn(Relation rel, const ItemPointerData* root_tid, uint64_t pinned_csn, uint64_t* out_csn,
    bool* out_found)
{
    if (!EpochHeapVersionChainEnabled(rel)) {
        return false;
    }
    return EpochHeapFollowChain(rel, root_tid, pinned_csn, false, 0, nullptr, out_csn, out_found, nullptr);
}

bool EpochHeapIsSupersededVersion(Relation rel, const HeapTupleHeader hdr, ItemPointer self)
{
    if (!EpochHeapVersionChainEnabled(rel) || hdr == nullptr || self == NULL) {
        return false;
    }
    if (hdr->is_latest) {
        return false;
    }
    /* 正向链：非 latest 且 t_ctid 指向下游 */
    return !ItemPointerEquals(self, &hdr->t_ctid);
}

bool EpochHeapMaterializeLatestRead(Relation rel, const ItemPointerData* chain_root, HeapTuple dest,
    HeapTupleHeader dest_storage)
{
    /* 未启用版本链：保持 dest 原样，返回成功（勿当未命中） */
    if (!EpochHeapVersionChainEnabled(rel)) {
        return true;
    }
    if (chain_root == nullptr || dest == nullptr || dest_storage == nullptr) {
        return false;
    }

    /*
     * staging tip 覆盖堆上内容（即使 dest 自带 is_latest）。
     * tombstone：返回 false 让上层跳过该行。
     * tip miss 时 Lookup 只扫一个 shard（~327），不经 dirty/flush。
     */
    if (g_epoch_local_tip_staging) {
        OccStagingInitEpochLocalTip();
        HeapTuple staged = nullptr;
        bool deleted = false;
        if (OccStagingTryReadTuple(rel, const_cast<ItemPointer>(chain_root), &staged, &deleted)) {
            if (deleted || staged == nullptr) {
                if (staged != nullptr) {
                    heap_freetuple(staged);
                }
                return false;
            }
            /* dest_storage must be large enough (scan rs_ctbuf_hdr / xs_ctbuf_hdr). */
            errno_t rc = memcpy_s((char*)dest_storage, MaxHeapTupleSize, (char*)staged->t_data, staged->t_len);
            securec_check(rc, "\0", "\0");
            dest->t_self = *chain_root;
            dest->t_len = staged->t_len;
            dest->t_data = dest_storage;
            dest->t_tableOid = RelationGetRelid(rel);
            dest->t_bucketId = RelationGetBktid(rel);
            heap_freetuple(staged);
            return true;
        }
    }

    /*
     * 已是 latest：无 frontier 时可直接用；有 frontier 时仍须按 m_status 过滤。
     */
    if (dest->t_data != nullptr && dest->t_data->is_latest && !EpochReadFrontierActive()) {
        return true;
    }
    if (dest->t_data != nullptr && !EpochHeapIsSupersededVersion(rel, dest->t_data, &dest->t_self) &&
        !EpochReadFrontierActive()) {
        return true;
    }

    HeapTuple latest = nullptr;
    if (!EpochHeapFetchLatestCopy(rel, chain_root, &latest, NULL) || latest == nullptr) {
        /*
         * 链断裂时：若 dest 本身在 frontier 下可见，仍返回 dest，避免 Warehouse not found。
         */
        if (dest->t_data != nullptr && EpochReadFrontierActive() &&
            EpochHeapVersionVisibleAtEpoch(dest->t_data, EpochGetReadFrontier())) {
            return true;
        }
        return false;
    }

    errno_t rc = memcpy_s((char*)dest_storage, latest->t_len, (char*)latest->t_data, latest->t_len);
    securec_check(rc, "\0", "\0");
    dest->t_self = latest->t_self;
    dest->t_len = latest->t_len;
    dest->t_data = dest_storage;
    dest->t_tableOid = RelationGetRelid(rel);
    dest->t_bucketId = RelationGetBktid(rel);
    heap_freetuple(latest);
    return true;
}

bool EpochHeapLockFollowToTip(Relation rel, ItemPointer otid, Buffer* buffer_io, HeapTupleData* oldtup)
{
    if (!EpochHeapVersionChainEnabled(rel) || otid == nullptr || buffer_io == nullptr || oldtup == nullptr ||
        !BufferIsValid(*buffer_io)) {
        return false;
    }

    int depth = 0;
    for (;;) {
        HeapTupleHeader hdr = oldtup->t_data;
        if (hdr == nullptr) {
            return false;
        }
        if (hdr->is_latest || ItemPointerEquals(&oldtup->t_self, &hdr->t_ctid)) {
            return true;
        }
        if (++depth > EPOCH_HEAP_VERSION_CHAIN_MAX) {
            return true;
        }

        ItemPointerData next = hdr->t_ctid;
        BlockNumber next_blk = ItemPointerGetBlockNumber(&next);
        OffsetNumber next_off = ItemPointerGetOffsetNumber(&next);
        BlockNumber cur_blk = BufferGetBlockNumber(*buffer_io);

        if (next_blk == cur_blk) {
            Page page = BufferGetPage(*buffer_io);
            if (next_off < FirstOffsetNumber || next_off > PageGetMaxOffsetNumber(page)) {
                return true;
            }
            ItemId lp = PageGetItemId(page, next_off);
            if (!ItemIdIsNormal(lp)) {
                return true;
            }
            oldtup->t_data = (HeapTupleHeader)PageGetItem(page, lp);
            oldtup->t_len = ItemIdGetLength(lp);
            oldtup->t_self = next;
            HeapTupleCopyBaseFromPage(oldtup, page);
            continue;
        }

        LockBuffer(*buffer_io, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(*buffer_io);
        *buffer_io = ReadBuffer(rel, next_blk);
        LockBuffer(*buffer_io, BUFFER_LOCK_EXCLUSIVE);
        Page page = BufferGetPage(*buffer_io);
        if (next_off < FirstOffsetNumber || next_off > PageGetMaxOffsetNumber(page)) {
            return false;
        }
        ItemId lp = PageGetItemId(page, next_off);
        if (!ItemIdIsNormal(lp)) {
            return false;
        }
        oldtup->t_data = (HeapTupleHeader)PageGetItem(page, lp);
        oldtup->t_len = ItemIdGetLength(lp);
        oldtup->t_self = next;
        oldtup->t_tableOid = RelationGetRelid(rel);
        oldtup->t_bucketId = RelationGetBktid(rel);
        HeapTupleCopyBaseFromPage(oldtup, page);
    }
}

Size EpochHeapPrunePage(Relation rel, Buffer buffer, uint64_t f_safe)
{
    return EpochHeapPrunePageLimited(rel, buffer, f_safe, nullptr, EPOCH_PRUNE_HOT_MAX_ITEMS);
}

Size EpochHeapPrunePageLimited(Relation rel, Buffer buffer, uint64_t f_safe, const ItemPointerData* tip_tid,
    int max_items)
{
    if (!EpochHeapVersionChainEnabled(rel) || !BufferIsValid(buffer) || f_safe == 0) {
        return 0;
    }

    Page page = BufferGetPage(buffer);
    BlockNumber blk = BufferGetBlockNumber(buffer);
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    if (maxoff < FirstOffsetNumber) {
        return 0;
    }
    if (max_items <= 0) {
        max_items = EPOCH_PRUNE_HOT_MAX_ITEMS;
    }

    /*
     * O(n) 建同页前驱表，再沿 tip 反向链（或全页候选）限量回收。
     * 旧实现双重循环 O(n²)×多趟，在 stock 满页上会长持 EXCLUSIVE，引发 find 秒级长尾。
     */
    OffsetNumber pred_of[MaxOffsetNumber + 1];
    bool has_pred[MaxOffsetNumber + 1];
    for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off)) {
        pred_of[off] = InvalidOffsetNumber;
        has_pred[off] = false;
    }

    for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off)) {
        ItemId lp = PageGetItemId(page, off);
        if (!ItemIdIsNormal(lp)) {
            continue;
        }
        HeapTupleHeader hdr = (HeapTupleHeader)PageGetItem(page, lp);
        ItemPointerData self;
        ItemPointerSet(&self, blk, off);
        if (hdr->is_latest || ItemPointerEquals(&self, &hdr->t_ctid)) {
            continue;
        }
        if (ItemPointerGetBlockNumber(&hdr->t_ctid) != blk) {
            continue;
        }
        OffsetNumber succ = ItemPointerGetOffsetNumber(&hdr->t_ctid);
        if (succ < FirstOffsetNumber || succ > maxoff) {
            continue;
        }
        /* 线性链：后者覆盖前者（正常每节点至多一个前驱） */
        pred_of[succ] = off;
        has_pred[succ] = true;
    }

    OffsetNumber work[MaxOffsetNumber];
    int nwork = 0;

    if (tip_tid != nullptr && ItemPointerIsValid(tip_tid) && ItemPointerGetBlockNumber(tip_tid) == blk) {
        OffsetNumber cur = ItemPointerGetOffsetNumber(tip_tid);
        int guard = 0;
        while (cur >= FirstOffsetNumber && cur <= maxoff && guard++ < MaxOffsetNumber) {
            if (has_pred[cur]) {
                OffsetNumber p = pred_of[cur];
                work[nwork++] = p;
                cur = p;
            } else {
                break;
            }
        }
    } else {
        /*
         * 无 tip 提示：回收「有同页前驱」的中间版本。
         * 后继可在异页（跨页 HOT 很常见）；旧逻辑跳过异页后继 → 中间版本永不回收 → 页只胀不缩。
         */
        for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off)) {
            ItemId lp = PageGetItemId(page, off);
            if (!ItemIdIsNormal(lp)) {
                continue;
            }
            HeapTupleHeader hdr = (HeapTupleHeader)PageGetItem(page, lp);
            ItemPointerData self;
            ItemPointerSet(&self, blk, off);
            if (hdr->is_latest || ItemPointerEquals(&self, &hdr->t_ctid)) {
                continue;
            }
            if (has_pred[off]) {
                work[nwork++] = off;
            }
        }
    }

    bool pruned_any = false;
    Size freed = 0;
    int pruned = 0;

    for (int i = 0; i < nwork && pruned < max_items; ++i) {
        OffsetNumber off = work[i];
        ItemId lp = PageGetItemId(page, off);
        if (!ItemIdIsNormal(lp)) {
            continue;
        }
        HeapTupleHeader hdr = (HeapTupleHeader)PageGetItem(page, lp);
        ItemPointerData self;
        ItemPointerSet(&self, blk, off);
        if (hdr->is_latest || ItemPointerEquals(&self, &hdr->t_ctid)) {
            continue;
        }
        if (!has_pred[off]) {
            continue;
        }

        BlockNumber succ_blk = ItemPointerGetBlockNumber(&hdr->t_ctid);
        OffsetNumber succ_off = ItemPointerGetOffsetNumber(&hdr->t_ctid);
        uint64_t succ_status = 0;
        bool succ_ok = false;

        if (succ_blk == blk) {
            if (succ_off < FirstOffsetNumber || succ_off > maxoff) {
                continue;
            }
            ItemId succ_lp = PageGetItemId(page, succ_off);
            if (!ItemIdIsNormal(succ_lp)) {
                continue;
            }
            HeapTupleHeader succ_hdr = (HeapTupleHeader)PageGetItem(page, succ_lp);
            succ_status = succ_hdr->m_status;
            succ_ok = true;
        } else {
            /*
             * 跨页后继：ConditionalLock 读 m_status，失败则跳过（绝不阻塞，避免与持异页锁者死锁）。
             */
            Buffer sbuf = ReadBuffer(rel, succ_blk);
            if (!ConditionalLockBuffer(sbuf)) {
                ReleaseBuffer(sbuf);
                continue;
            }
            Page spage = BufferGetPage(sbuf);
            OffsetNumber smax = PageGetMaxOffsetNumber(spage);
            if (succ_off >= FirstOffsetNumber && succ_off <= smax) {
                ItemId succ_lp = PageGetItemId(spage, succ_off);
                if (ItemIdIsNormal(succ_lp)) {
                    HeapTupleHeader succ_hdr = (HeapTupleHeader)PageGetItem(spage, succ_lp);
                    succ_status = succ_hdr->m_status;
                    succ_ok = true;
                }
            }
            LockBuffer(sbuf, BUFFER_LOCK_UNLOCK);
            ReleaseBuffer(sbuf);
        }
        if (!succ_ok || succ_status == 0 || succ_status > f_safe) {
            continue;
        }

        /* O(1) 改写同页前驱（线性链）；若不一致则回退扫描 */
        bool found_pred = false;
        OffsetNumber pred = pred_of[off];
        if (pred >= FirstOffsetNumber && pred <= maxoff && pred != off) {
            ItemId pred_lp = PageGetItemId(page, pred);
            if (ItemIdIsNormal(pred_lp)) {
                HeapTupleHeader pred_hdr = (HeapTupleHeader)PageGetItem(page, pred_lp);
                if (ItemPointerEquals(&pred_hdr->t_ctid, &self)) {
                    pred_hdr->t_ctid = hdr->t_ctid;
                    found_pred = true;
                    if (succ_blk == blk) {
                        pred_of[succ_off] = pred;
                        has_pred[succ_off] = true;
                    }
                    has_pred[off] = false;
                }
            }
        }
        if (!found_pred) {
            for (OffsetNumber p = FirstOffsetNumber; p <= maxoff; p = OffsetNumberNext(p)) {
                if (p == off) {
                    continue;
                }
                ItemId pred_lp = PageGetItemId(page, p);
                if (!ItemIdIsNormal(pred_lp)) {
                    continue;
                }
                HeapTupleHeader pred_hdr = (HeapTupleHeader)PageGetItem(page, pred_lp);
                if (ItemPointerEquals(&pred_hdr->t_ctid, &self)) {
                    pred_hdr->t_ctid = hdr->t_ctid;
                    found_pred = true;
                }
            }
        }
        if (!found_pred) {
            continue;
        }

        Size tup_len = ItemIdGetLength(lp);
        ItemIdSetUnused(lp);
        freed += tup_len;
        pruned_any = true;
        pruned++;
    }

    if (pruned_any) {
        PageRepairFragmentation(page);
        /*
         * 与 heap_page_prune 一致：清 PD_PAGE_FULL，并把实际空闲写回 FSM。
         * 否则换页后旧页即使腾出空间也永不被 RelationGetBufferForTuple 重用，
         * district/warehouse 会只扩不收（本轮 district 0.3MB→7MB）。
         */
        PageClearFull(page);
        MarkBufferDirty(buffer);
        RecordPageWithFreeSpace(rel, blk, PageGetHeapFreeSpace(page));
    }
    return freed;
}
