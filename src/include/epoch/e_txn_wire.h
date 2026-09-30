#ifndef OPENGAUSS_E_TXN_WIRE_H
#define OPENGAUSS_E_TXN_WIRE_H

#include <cstdint>
#include <string>
#include <vector>

#include "epoch/e_types.h"

static constexpr uint64_t EPOCH_WIRE_MAGIC = 0xE0C0CAFEULL;
static constexpr uint32_t EPOCH_WIRE_VERSION = 6;
static constexpr uint64_t EPOCH_MERGE_TASK_MAGIC = 0xE0C0CAF2ULL;
static constexpr uint32_t EPOCH_MERGE_TASK_VERSION = 2;
static constexpr uint32_t EPOCH_MAX_WIRE_TXN_BYTES = 256 * 1024;
static constexpr uint16_t EPOCH_WIRE_MAX_PK = 128;
/* 空包 + 该 CSN 表示请求对端重发某 epoch 的缓存事务 */
static constexpr uint64_t EPOCH_WIRE_TXN_REQUEST_CSN = UINT64_MAX;
/* PA 激活：带一行，expected_csn = 协助区间终点，relid = session */
static constexpr uint64_t EPOCH_WIRE_PA_ACTIVATE_CSN = UINT64_MAX - 1;

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
    EpochTxnState txn_state{EPOCH_TXN_COMMIT};
    /* TPCC w_id；0=未知，路由 fallback 到 wire_key % N */
    uint16_t route_wid{0};
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

static inline bool E_WireTxnIsEpochEnd(const E_WireTxn& txn)
{
    return txn.rows.empty() && txn.epoch > 0 && txn.csn != EPOCH_WIRE_TXN_REQUEST_CSN &&
           txn.csn != EPOCH_WIRE_PA_ACTIVATE_CSN;
}

static inline bool E_WireTxnIsPaActivate(const E_WireTxn& txn)
{
    return txn.csn == EPOCH_WIRE_PA_ACTIVATE_CSN && txn.epoch > 0;
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
