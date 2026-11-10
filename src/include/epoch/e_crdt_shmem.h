#ifndef OPENGAUSS_E_CRDT_SHMEM_H
#define OPENGAUSS_E_CRDT_SHMEM_H

#include <cstdint>
#include <functional>

#include "epoch/e_concurrent_map.h"
#include "epoch/e_crdt.h"
#include "epoch/e_txn_wire.h"

/*
 * ============================ CRDT 行存储：字段长度为什么这么定 ============================
 *
 * 这里每个 "行槽"（RowEntry）保存的是【一个被修改的元组（tuple）】，不是一笔事务：
 *   一笔 TPCC NewOrder 会改 5 行（oorder/new_order/stock×3），就占 5 个行槽。
 *   之所以按 tuple 存而不是按事务存，是因为冲突裁决必须精确到行：
 *     “节点1 把 stock(i_id=42) 改成 90” 与 “节点2 改成 95” 要在同一行上比 CSN 决定谁赢，
 *     若按事务存就无法判断两条记录是否指同一行。
 *
 * 三个长度上限（定长而非变长，理由见下）：
 *   MAX_PK=128       主键最大长度。TPCC bmsql_stock 主键 (w_id,i_id) 等均 < 32B，128 留 4 倍余量。
 *   MAX_RELNAME=64   表名最大长度。最长 bmsql_new_order (17B)，64 足够。
 *   MAX_TUPLE=1024   单行元组数据最大长度。实测 bmsql_customer UPDATE 最大约 740B，1024 留余量。
 *
 * 【为什么用定长而不是变长】
 *   这些槽位于【共享内存】的固定偏移上，需要能按 index 直接算出地址（O(1) 随机访问）；
 *   变长就必须维护空闲链表/压缩，既慢又难保证崩溃后的一致性。定长让寻址变成纯算术，
 *   代价只是少量内部碎片（实际 740B 的行占 1024B）。
 *
 * 【为什么只有 1 个分片（不再按 997 分片）】
 *   merge worker 严格按 route_wid 亲和路由：同一个表/主键的所有写集永远落到【同一个 worker】，
 *   由它串行处理 → 同一行不存在并发写。不同 worker 并发写的是【不同的行】，
 *   而它们仍然写同一个 epoch 槽位 → 只需要【一把槽位锁】保护 append/查找，
 *   不需要 997 把分片锁。历史上对 997 分片 + 每片 256 的 skew 余量，
 *   会让每槽实际分配 997×257≈25.6 万行（内存放大近千倍），故收敛为单分片。
 * ======================================================================================
 */
static constexpr uint32_t EPOCH_CRDT_MAX_PK = 128;
static constexpr uint32_t EPOCH_CRDT_MAX_RELNAME = 64;
static constexpr uint32_t EPOCH_CRDT_MAX_TUPLE = 1024;

/* 行槽分片数：恒为 1（worker 按 wid 串行，槽位只需一把锁）。保留“分片”抽象是为了
 * 让上层寻址代码（ShardAt/ShardIndex）无需改动。 */
static constexpr uint32_t EPOCH_CRDT_NUM_SHARDS = 1;
/* 不再为 hash 偏斜预留额外余量：单分片下容量就是 rows_per_slot 本身。 */
static constexpr uint32_t EPOCH_CRDT_ROW_SKEW_SLACK = 0;
static constexpr uint32_t EPOCH_CRDT_INSERT_SKEW_SLACK = 0;


/*
 * ============================ CRDT 行存储（merge 合并结果） ============================
 *
 * 【存什么】每个 epoch 里"被修改的 tuple"的最终赢家（每行只有一份，冲突已裁决）。
 *          一笔事务改 5 行就产生 5 个 EpochCrdtRow；不是按事务存，因为裁决必须精确到行。
 *
 * 【三维结构】[epoch][merge线程][行]
 *   epoch   : 按 epoch % EPOCH_TXN_EPOCH_COUNT 复用（与发送队列同常量、同深度）
 *   merge线程: 分片键 = route_wid % merge线程数 —— 同一 warehouse 的行永远同一个线程处理
 *             → 每份单写者 → 无需锁
 *   行      : 该线程负责的、本 epoch 被改的 tuple
 *
 * 【为什么定长】共享内存中按 index 算术寻址；字段长度取 TPCC 实测最大值 × 余量：
 *   pk[128]        主键（TPCC < 32B）
 *   tuple_data[1024] 整行数据（实测 bmsql_customer UPDATE ≈ 740B）
 * ======================================================================================
 */


/* 一个被裁决的 tuple（赢家） */
struct EpochCrdtRow {
    uint32_t relid;                   /* 表 OID（本地） */
    uint16_t bi_hi;                   /* ctid 高位 */
    uint16_t bi_lo;                   /* ctid 块号 */
    uint16_t ip_posid;                /* ctid 行号 */
    uint16_t pk_len;
    uint8_t merged_ok;                /* merge 是否判定该版本为赢家 */
    uint8_t pad;
    uint32_t server_id;               /* 谁写的 */
    uint64_t csn;                     /* 所属事务的 csn（裁决依据 & 统计去重键） */
    uint64_t cen;                     /* CRDT 版本（冲突比较用） */
    uint16_t tuple_len;
    char pk[EPOCH_CRDT_MAX_PK];       /* 主键 */
    char tuple_data[EPOCH_CRDT_MAX_TUPLE];   /* 改后的整行数据 */
};




struct EpochCrdtShmem;


extern Size EpochCrdtShmemSize(void);
extern void EpochCrdtShmemInit(void);
extern void EpochCrdtShmemClearEpoch(uint64_t epoch);
/* 供 apply 遍历该 epoch 的全部赢家行（进程内 map） */
extern bool EpochCrdtShmemVisitRows(uint64_t epoch, const std::function<void(const EpochCrdtRow&)>& fn);
extern E_CrdtMergeOutcome EpochCrdtShmemMergeWireTxn(const E_WireTxn& txn);
/* merge 关窗后从 CRDT shmem 组装 apply 载荷；前置条件：txn 不在 abort_csn 且有写行 */
extern bool EpochCrdtShmemIsEmpty(uint64_t epoch);

#endif /* OPENGAUSS_E_CRDT_SHMEM_H */
