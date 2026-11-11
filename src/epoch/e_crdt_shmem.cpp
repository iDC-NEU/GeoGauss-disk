#include "postgres.h"

#include <atomic>
#include <cstring>
#include <functional>
#include <unordered_map>

#include "epoch/e_concurrent_map.h"
#include "epoch/e_config.h"
#include "epoch/e_crdt_shmem.h"
#include "epoch/e_txn_wire.h"
#include "epoch/e_pipeline.h"
#include "epoch/e_abort_csn.h"
#include "epoch/e_apply_stats.h"

/*
 * ============================ CRDT 行存储（进程内并发 map） ============================
 *
 * 【存什么】每个 epoch 里"被修改的 tuple"的赢家（每行只有一份，冲突已裁决）。
 *
 * 【怎么存】两级并发 map：epoch → (rowkey → EpochCrdtRow)
 *   - 外层 key = epoch：UnorderedMap 按 key 分片加锁 →
 *       同一 epoch 的 merge 串行（等价单线程 merge，语义正确），
 *       不同 epoch 的 merge 并行。
 *   - 内层 = 普通 unordered_map：只在持有外层分片锁时访问，无需再加锁。
 *
 * 【为什么进程内而不是共享内存】写入者是 merge 线程、读取者是 apply 线程，
 *   都在 postmaster 进程内 → 无需跨进程共享；进程内 map 可按需增长，
 *   避免共享内存"定长 + 预估容量"带来的浪费。
 *
 * 【为什么不需要按 wid 分片】同一 epoch 的 merge 由外层锁串行化，
 *   行冲突（同一 pk 两个 csn）在 Modify 的回调里按 csn 比较裁决即可。
 * ======================================================================================
 */

/* 行键：relid + 主键的哈希（同一行的不同事务映射到同一 key） */
static uint64_t EpochCrdtRowKey(uint32_t relid, const char* pk, size_t pk_len,
    uint16_t bi_hi, uint16_t bi_lo, uint16_t ip_posid)
{
    uint64_t h = 1469598103934665603ULL;   /* FNV-1a offset basis */
    for (size_t i = 0; i < pk_len; ++i) {
        h ^= static_cast<uint8_t>(pk[i]);
        h *= 1099511628211ULL;
    }
    /*
     * 主键未填充时必须用物理位置（ctid）区分行，否则同表所有行会撞成同一个 key
     * → 互相覆盖 → 误记 abort → apply 全跳过（实测 commit=0 的根因）。
     */
    if (pk_len == 0) {
        h ^= (static_cast<uint64_t>(bi_hi) << 32) ^ (static_cast<uint64_t>(bi_lo) << 16) ^
             static_cast<uint64_t>(ip_posid);
        h *= 1099511628211ULL;
    }
    return h ^ (static_cast<uint64_t>(relid) << 48);
}

using EpochCrdtRowMap = std::unordered_map<uint64_t, EpochCrdtRow>;

/* epoch → (rowkey → 赢家行) */
static EpochConcurrent::UnorderedMap<uint64_t, EpochCrdtRowMap> g_crdt_rows;
/* epoch → 因冲突被覆盖（abort）的事务 csn */
static EpochConcurrent::U64SetMap g_abort_csns;

/* 把一条 wire 行写入 CRDT 行 */
static void EpochCrdtFillRow(EpochCrdtRow* dst, const E_WireRow& src, const E_WireTxn& txn)
{
    dst->relid = src.relid;
    dst->bi_hi = src.bi_hi;
    dst->bi_lo = src.bi_lo;
    dst->ip_posid = src.ip_posid;
    size_t pk_len = src.pk.size();
    if (pk_len > EPOCH_CRDT_MAX_PK) {
        pk_len = EPOCH_CRDT_MAX_PK;
    }
    dst->pk_len = static_cast<uint16_t>(pk_len);
    if (dst->pk_len > 0) {
        memcpy(dst->pk, src.pk.data(), dst->pk_len);
    }
    size_t tuple_len = src.tuple_data.size();
    if (tuple_len > EPOCH_CRDT_MAX_TUPLE) {
        tuple_len = EPOCH_CRDT_MAX_TUPLE;
    }
    dst->tuple_len = static_cast<uint16_t>(tuple_len);
    if (dst->tuple_len > 0) {
        memcpy(dst->tuple_data, src.tuple_data.data(), dst->tuple_len);
    }
    dst->csn = txn.csn;
    dst->cen = txn.csn;       /* 本实现用 csn 作为 CRDT 版本 */
    dst->server_id = txn.server_id;
    dst->merged_ok = 1;
    dst->claim_only = 0;
}

/* PA 影子：只记冲突声明，不带行数据。 */
static void EpochCrdtFillClaim(EpochCrdtRow* dst, const E_WireRow& src, const E_WireTxn& txn)
{
    dst->relid = src.relid;
    dst->bi_hi = src.bi_hi;
    dst->bi_lo = src.bi_lo;
    dst->ip_posid = src.ip_posid;
    size_t pk_len = src.pk.size();
    if (pk_len > EPOCH_CRDT_MAX_PK) {
        pk_len = EPOCH_CRDT_MAX_PK;
    }
    dst->pk_len = static_cast<uint16_t>(pk_len);
    if (dst->pk_len > 0) {
        memcpy(dst->pk, src.pk.data(), dst->pk_len);
    }
    dst->tuple_len = 0;
    dst->csn = txn.csn;
    dst->cen = txn.csn;
    dst->server_id = txn.server_id;
    dst->merged_ok = 1;
    dst->claim_only = 1;
}

/* CRDT 已改为进程内结构，不再占用共享内存 */
Size EpochCrdtShmemSize(void)
{
    return 0;
}

void EpochCrdtShmemInit(void)
{
    /* 进程内 map，无需显式初始化 */
}
E_CrdtMergeOutcome EpochCrdtShmemMergeWireTxn(const E_WireTxn& txn)
{
    E_CrdtMergeOutcome outcome;
    outcome.txn_accepted = true;
    outcome.resource_failure = false;

    if (txn.rows.empty() || txn.epoch == 0) {
        return outcome;
    }

    /* PA 影子：按行重放冲突声明，不做整笔预检。 */
    if (txn.claim_only) {
        if (EpochAbortCsnInsert(txn.epoch, txn.csn, txn.server_id)) {
            EpochApplyStatsNoteAbortN(1);
        }
        g_crdt_rows.Modify(txn.epoch, [&](EpochCrdtRowMap& rows, bool /*is_new*/) {
            for (const E_WireRow& row : txn.rows) {
                if (row.op_type == EPOCH_OP_READ) {
                    continue;
                }
                const uint64_t key = EpochCrdtRowKey(row.relid, row.pk.data(), row.pk.size(),
                    row.bi_hi, row.bi_lo, row.ip_posid);
                auto it = rows.find(key);
                if (it != rows.end()) {
                    if (it->second.csn <= txn.csn) {
                        continue;   /* 该行已有更早的赢家，声明失效 */
                    }
                    /* 声明压过更晚的版本：该版本应判输，与直接路径一致 */
                    EpochAbortCsnInsert(txn.epoch, it->second.csn, it->second.server_id);
                    EpochCrdtFillClaim(&it->second, row, txn);
                } else {
                    EpochCrdtRow fresh;
                    EpochCrdtFillClaim(&fresh, row, txn);
                    rows.emplace(key, fresh);
                }
            }
        });
        outcome.txn_accepted = false;
        return outcome;
    }

    /*
     * 两阶段：先预检、后合并。
     *
     * 阶段①只读 CRDT，判断这笔事务的每一行是否还能赢；只要有一行已经被
     * 更新的 csn 占住，就整笔拒绝、CRDT 一行都不改。
     *
     * 这一步不能省。以前是单趟处理：某行判输后只置标记，循环继续把其余行
     * 写进 CRDT，然后整笔事务因 abort 集合被 apply 跳过 —— 于是 CRDT 里留下了
     * “堆表并不存在”的赢家版本（幽灵赢家）。后续任何 csn 更小的真实写者都会被
     * 它判输，导致这一行两方都不落地，最终 SELECT 查不到该行
     * （实测：TPCC 报 District/Warehouse not found）。
     */
    g_crdt_rows.Modify(txn.epoch, [&](EpochCrdtRowMap& rows, bool /*is_new*/) {
        /* ① 预检：任意一行已有 >= 本事务 csn 的版本 → 整笔拒绝 */
        for (const E_WireRow& row : txn.rows) {
            if (row.op_type == EPOCH_OP_READ) {
                continue;
            }
            const uint64_t key = EpochCrdtRowKey(row.relid, row.pk.data(), row.pk.size(),
                row.bi_hi, row.bi_lo, row.ip_posid);
            auto it = rows.find(key);
            if (it != rows.end() && txn.csn <= it->second.csn) {
                /*
                 * 合并阶段判输：这笔事务不会进 apply 队列，
                 * 所以必须在这里计入失败率，否则 abort tps 只统计到
                 * "先赢后被打回"的那一类，会明显偏低。
                 * Insert 自带去重，只有真正新插入时才计数。
                 */
                if (EpochAbortCsnInsert(txn.epoch, txn.csn, txn.server_id)) {
                    EpochApplyStatsNoteAbortN(1);
                }
                outcome.txn_accepted = false;
                return;
            }
        }

        /* ② 全部行都能赢，才逐行合并；被覆盖的旧版本记入 abort 集合 */
        for (const E_WireRow& row : txn.rows) {
            if (row.op_type == EPOCH_OP_READ) {
                continue;
            }
            const uint64_t key = EpochCrdtRowKey(row.relid, row.pk.data(), row.pk.size(),
                row.bi_hi, row.bi_lo, row.ip_posid);
            auto it = rows.find(key);
            if (it != rows.end()) {
                EpochAbortCsnInsert(txn.epoch, it->second.csn, it->second.server_id);
                EpochCrdtFillRow(&it->second, row, txn);
            } else {
                EpochCrdtRow fresh;
                EpochCrdtFillRow(&fresh, row, txn);
                rows.emplace(key, fresh);
            }
        }
    });

    /* 只有整笔被接受的事务才进 apply；判输的事务一行都不会落堆 */
    if (outcome.txn_accepted) {
        EpochPipelineEnqueueApplyWinner(txn);
    }
    return outcome;
}

void EpochCrdtShmemClearEpoch(uint64_t epoch)
{
    (void)g_crdt_rows.Erase(epoch);
    (void)g_abort_csns.Erase(epoch);
}

/*
 * 供 apply 遍历：对该 epoch 的所有"赢家行"逐行回调。
 * 回调内不要再调 Modify/Visit 同一 epoch（会重复加锁）。
 */
bool EpochCrdtShmemVisitRows(uint64_t epoch, const std::function<void(const EpochCrdtRow&)>& fn)
{
    return g_crdt_rows.Visit(epoch, [&](const EpochCrdtRowMap& rows) {
        for (const auto& entry : rows) {
            fn(entry.second);
        }
    });
}

bool EpochCrdtShmemIsEmpty(uint64_t epoch)
{
    bool empty = true;   /* key 不存在（Visit 返回 false）也视为空 */
    (void)g_crdt_rows.Visit(epoch, [&](const EpochCrdtRowMap& rows) { empty = rows.empty(); });
    return empty;
}
