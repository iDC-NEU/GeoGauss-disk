#ifndef OPENGAUSS_E_TXN_WIRE_H
#define OPENGAUSS_E_TXN_WIRE_H

#include <cstdint>
#include <string>
#include <vector>

#include "epoch/e_types.h"

static constexpr uint64_t EPOCH_WIRE_MAGIC = 0xE0C0CAFEULL;
static constexpr uint32_t EPOCH_WIRE_VERSION = 2;
static constexpr uint32_t EPOCH_MAX_WIRE_TXN_BYTES = 256 * 1024;
/* 空包 + 该 CSN 表示请求对端重发某 epoch 的缓存事务 */
static constexpr uint64_t EPOCH_WIRE_TXN_REQUEST_CSN = UINT64_MAX;

struct E_WireRow {
    EpochOpType op_type{EPOCH_OP_READ};
    uint32_t relid{0};
    uint16_t bi_hi{0};
    uint16_t bi_lo{0};
    uint16_t ip_posid{0};
    std::vector<char> tuple_data;
    /* 发送端 relid 仅作参考；对端用 relname 解析本地 OID 后 heap_open */
    std::string relname;
};

struct E_WireTxn {
    uint64_t csn{0};
    uint64_t start_epoch{0};
    uint64_t commit_epoch{0};
    uint32_t server_id{0};
    EpochTxnState txn_state{EPOCH_TXN_COMMIT};
    std::vector<E_WireRow> rows;
};

bool E_WireSerializeTxn(const E_WireTxn& txn, std::string* out);
bool E_WireDeserializeTxn(const char* data, size_t len, E_WireTxn* out);

#endif /* OPENGAUSS_E_TXN_WIRE_H */
