#ifndef OPENGAUSS_E_TXN_WIRE_H
#define OPENGAUSS_E_TXN_WIRE_H

#include <cstdint>
#include <string>
#include <vector>

#include "epoch/e_types.h"

static constexpr uint64_t EPOCH_WIRE_MAGIC = 0xE0C0CAFEULL;
/* v8：事务头增加 claim_only（PA 影子）。控制消息沿用同一报文，不另立格式。 */
static constexpr uint32_t EPOCH_WIRE_VERSION = 8;
static constexpr uint64_t EPOCH_MERGE_TASK_MAGIC = 0xE0C0CAF2ULL;
static constexpr uint32_t EPOCH_MERGE_TASK_VERSION = 2;
/*
 * 单笔事务序列化后的最大字节数（发送队列 / merge 任务 / apply 任务三处定长缓冲共用）。
 * TPCC 实测最大写集：Delivery 改 10 行 × 740B ≈ 8.1KB → 12KB 留 1.5 倍余量。
 * 之所以定长：这些缓冲在【共享内存】里，需要 O(1) 算术寻址；变长需要分配器且易碎片。
 */
static constexpr uint32_t EPOCH_MAX_WIRE_TXN_BYTES = 12 * 1024;
static constexpr uint16_t EPOCH_WIRE_MAX_PK = 128;
/* 空包 + 该 CSN 表示请求对端重发某 epoch 的缓存事务 */
static constexpr uint64_t EPOCH_WIRE_TXN_REQUEST_CSN = UINT64_MAX;
/*
 * PA 会话状态（on/off 两个 csn 值区分）。同一条控制报文携带：
 *   epoch 字段        = 会话的首个未应用 epoch（e_f）
 *   server_id 字段    = 慢节点 id
 *   route_wid 字段    = 选定的 proxy id
 *   origin_start_us   = session id
 * 三个字段本来就在 v6/v7 报文头里，控制消息不必新增格式。
 */
static constexpr uint64_t EPOCH_WIRE_PA_STATE_ON_CSN = UINT64_MAX - 5;
static constexpr uint64_t EPOCH_WIRE_PA_STATE_OFF_CSN = UINT64_MAX - 6;
/* PA：代理对某一片的转发决策（epoch 字段带片号，csn 区分 skip/direct） */
static constexpr uint64_t EPOCH_WIRE_PA_EPOCH_SKIP_CSN = UINT64_MAX - 7;
static constexpr uint64_t EPOCH_WIRE_PA_EPOCH_DIRECT_CSN = UINT64_MAX - 8;
/* 拉模式：本端缺某 peer 的 epoch-end 时，请对方重发该 epoch 的 EOF（EOF 可能被丢弃） */
static constexpr uint64_t EPOCH_WIRE_EOF_REQUEST_CSN = UINT64_MAX - 2;
/* 快节点把自己的 physical epoch 通告给慢节点（内容放在 txn.epoch 里） */
static constexpr uint64_t EPOCH_WIRE_PHYS_NOTIFY_CSN = UINT64_MAX - 3;
/* EOF 确认：携带"我连续收到的你的 EOF 到第几片"（内容在 txn.epoch 里） */
static constexpr uint64_t EPOCH_WIRE_EOF_ACK_CSN = UINT64_MAX - 9;
/* PA：请代理对某一片立即给出路由决策（epoch 字段带片号），避免等它自己走到该片 */
static constexpr uint64_t EPOCH_WIRE_PA_DECIDE_REQ_CSN = UINT64_MAX - 10;

struct E_WireRow {
    EpochOpType op_type{EPOCH_OP_READ};
    uint32_t relid{0};
    uint16_t bi_hi{0};
    uint16_t bi_lo{0};
    uint16_t ip_posid{0};
    /* backend 执行时 write set 记录的读版本 CSN，merge/apply 与 heap.m_rowCSN 对比 */
    uint64_t expected_csn{0};
    std::vector<char> tuple_data;
    /* 发送端 relid 仅作参考；对端用 relname 解析本地 OID 后 heap_open */
    std::string relname;
    /* pack 时预计算 PK（v5）；merge 侧直接用于 CRDT 分片，无需 heap_open */
    uint16_t pk_len{0};
    std::string pk;
};

static inline bool E_WireRowHasPackedPk(const E_WireRow& row)
{
    return row.pk_len > 0 && row.pk_len <= EPOCH_WIRE_MAX_PK && row.pk.size() == row.pk_len;
}

struct E_WireTxn {
    uint64_t csn{0};
    uint64_t epoch{0};
    uint32_t server_id{0};
    /*
     * 发起节点的事务起点（微秒，gettimeofday 口径，v7 起随 wire 传输）。
     * 仅用于发起节点本机统计：事务开始 → 本节点 apply 写回完成。
     */
    uint64_t origin_start_us{0};
    EpochTxnState txn_state{EPOCH_TXN_COMMIT};
    /* TPCC w_id；0=未知，路由 fallback 到 wire_key % N */
    uint16_t route_wid{0};
    /*
     * PA 影子（v8）：该事务在代理处已判输，转发时去掉行数据，只保留冲突声明。
     * 影子参与冲突裁决（阻挡 CSN 更小的本地事务），但永远不会被 apply。
     */
    bool claim_only{false};
    std::vector<E_WireRow> rows;
};

/* apply/merge worker 亲和：同 wid 固定落到同一 worker；未设 wid 用 wire_key */
static inline uint32_t E_WireTxnRouteWorker(uint16_t route_wid, uint64_t wire_key, uint32_t n_workers)
{
    if (n_workers == 0) {
        return 0;
    }
    if (route_wid == 0) {
        return static_cast<uint32_t>(wire_key % n_workers);
    }
    return static_cast<uint32_t>(route_wid % n_workers);
}

static inline bool E_WireTxnIsHello(const E_WireTxn& txn)
{
    return txn.rows.empty() && txn.epoch == 0 && txn.csn != EPOCH_WIRE_TXN_REQUEST_CSN;
}

/* 拉取式 EOF 重传：某节点请求重发指定片的收尾包 */
/* EOF 确认消息 */
static inline bool E_WireTxnIsEofAck(const E_WireTxn& txn)
{
    return txn.csn == EPOCH_WIRE_EOF_ACK_CSN;
}

/* 收尾包：某源在本片的输入到此为止（不含任何计数，declared 由接收端冻结） */
static inline bool E_WireTxnIsEpochEnd(const E_WireTxn& txn)
{
    /* csn == 0 把“空写集”与“收尾包”区分开（收尾包不带事务号） */
    return txn.rows.empty() && txn.epoch > 0 && txn.csn == 0 && !txn.claim_only;
}

/* PA 状态广播：返回 true 时 *congested 给出状态 */
static inline bool E_WireTxnIsPaState(const E_WireTxn& txn, bool* congested)
{
    if (txn.csn == EPOCH_WIRE_PA_STATE_ON_CSN) {
        if (congested != nullptr) { *congested = true; }
        return true;
    }
    if (txn.csn == EPOCH_WIRE_PA_STATE_OFF_CSN) {
        if (congested != nullptr) { *congested = false; }
        return true;
    }
    return false;
}

/* PA 每片决策：返回 true 时 *skip 给出结论，txn.epoch 即片号 */
static inline bool E_WireTxnIsPaEpochDecision(const E_WireTxn& txn, bool* skip)
{
    if (txn.csn == EPOCH_WIRE_PA_EPOCH_SKIP_CSN) {
        if (skip != nullptr) { *skip = true; }
        return true;
    }
    if (txn.csn == EPOCH_WIRE_PA_EPOCH_DIRECT_CSN) {
        if (skip != nullptr) { *skip = false; }
        return true;
    }
    return false;
}

/* PA 影子：只带冲突声明，不带行数据 */
static inline bool E_WireTxnIsShadow(const E_WireTxn& txn)
{
    return txn.claim_only;
}

/* PA 决策请求：epoch 字段即所问的片 */
static inline bool E_WireTxnIsPaDecideRequest(const E_WireTxn& txn)
{
    return txn.csn == EPOCH_WIRE_PA_DECIDE_REQ_CSN && txn.epoch > 0;
}

static inline bool E_WireTxnIsTxnRequest(const E_WireTxn& txn)
{
    return txn.rows.empty() && txn.epoch > 0 && txn.csn == EPOCH_WIRE_TXN_REQUEST_CSN;
}

bool E_WireSerializeTxn(const E_WireTxn& txn, std::string* out);
bool E_WireDeserializeTxn(const char* data, size_t len, E_WireTxn* out);
/* 轻量窥视已序列化 apply payload 的 route_wid（失败则 *out=0） */
bool E_WirePeekRouteWid(const char* data, size_t len, uint16_t* out);
/* merge worker 队列专用：无双重 wire 编解码，直接传递 txn 载荷 */
bool E_MergeTaskSerialize(const E_WireTxn& txn, std::string* out);
bool E_MergeTaskDeserialize(const char* data, size_t len, E_WireTxn* out);

#endif /* OPENGAUSS_E_TXN_WIRE_H */
