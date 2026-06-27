//
// Wire-level types for heterogeneous network replication (dynamic-net).
// Plain C++ structs — no protobuf dependency; mirrors dynamic-main demo messages.
//

#ifndef DYNAMIC_NET_DN_TYPES_H
#define DYNAMIC_NET_DN_TYPES_H

#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

enum class DnEntryType : uint8_t {
    WRITE = 0,
    EPOCH_STATUS = 1,
    NOOP = 2
};

enum class DnMsgKind : uint8_t {
    APPEND_ENTRIES,
    APPEND_RESPONSE,
    COMMIT_NOTIFY,
    FAST_HASH,
    PARTIAL_SNAPSHOT,
    SNAPSHOT_ACK,
    WIRE_PAYLOAD /* epoch E_WireTxn blob */
};

enum class DnStrategyAction : uint8_t {
    NORMAL,
    JUMP
};

struct DnRow {
    std::string key;
    std::string value;
    bool is_delete;

    DnRow() : is_delete(false) {}
};

struct DnTransaction {
    uint64_t client_txn_id;
    uint32_t origin_node_id;
    std::string client_key;
    std::vector<DnRow> rows;

    DnTransaction() : client_txn_id(0), origin_node_id(0) {}
};

struct DnRaftEntry {
    int32_t term;
    int32_t index;
    int32_t epoch;
    DnEntryType type;
    std::vector<DnTransaction> txns;

    DnRaftEntry() : term(0), index(0), epoch(0), type(DnEntryType::NOOP) {}
};

struct DnAppendEntriesRequest {
    int32_t term;
    int32_t channel_id;
    int32_t channel_leader_id;
    int32_t prev_log_index;
    int32_t prev_log_term;
    int32_t leader_commit;
    int32_t epoch;
    std::vector<DnRaftEntry> entries;

    /* Metadata-only path (FN → SN): entries empty, hash/size carry payload digest */
    uint64_t payload_hash;
    int32_t payload_size;

    DnAppendEntriesRequest()
        : term(0),
          channel_id(0),
          channel_leader_id(0),
          prev_log_index(0),
          prev_log_term(0),
          leader_commit(0),
          epoch(0),
          payload_hash(0),
          payload_size(0)
    {
    }

    bool IsMetadataOnly() const
    {
        return entries.empty();
    }
};

struct DnAppendEntriesResponse {
    int32_t term;
    int32_t node_id;
    bool success;
    int32_t last_log_index;
    int32_t match_index;

    DnAppendEntriesResponse()
        : term(0), node_id(0), success(false), last_log_index(0), match_index(0)
    {
    }
};

struct DnCommitNotify {
    int32_t channel_id;
    int32_t term;
    int32_t commit_index;

    DnCommitNotify() : channel_id(0), term(0), commit_index(0) {}
};

struct DnFastHashMessage {
    int32_t epoch;
    uint64_t hash;

    DnFastHashMessage() : epoch(0), hash(0) {}
};

struct DnNodeContribution {
    int32_t origin_node_id;
    std::vector<DnTransaction> committed_txns;
    std::vector<uint64_t> dropped_hashes;
    std::string reconstruction_bitmap;

    DnNodeContribution() : origin_node_id(0) {}
};

struct DnPartialSnapshotMessage {
    int32_t epoch;
    int32_t proxy_node_id;
    std::vector<DnNodeContribution> contributions;
    uint64_t checksum;

    DnPartialSnapshotMessage() : epoch(0), proxy_node_id(0), checksum(0) {}
};

struct DnSnapshotAckMessage {
    int32_t epoch;
    int32_t node_id;
    bool success;
    uint64_t state_hash;

    DnSnapshotAckMessage() : epoch(0), node_id(0), success(false), state_hash(0) {}
};

struct DnNetworkPacket {
    DnMsgKind kind;
    int32_t channel_id;
    int32_t from_node_id;
    DnAppendEntriesRequest append_req;
    DnAppendEntriesResponse append_resp;
    DnCommitNotify commit_notify;
    DnFastHashMessage fast_hash;
    DnPartialSnapshotMessage partial_snapshot;
    DnSnapshotAckMessage snapshot_ack;
    std::string wire_payload;

    DnNetworkPacket() : kind(DnMsgKind::APPEND_ENTRIES), channel_id(0), from_node_id(0) {}
};

/* Slow-node metadata table entry */
struct DnEpochMetadata {
    int32_t epoch;
    bool received;
    int32_t term;
    uint64_t payload_hash;
    int32_t payload_size;

    DnEpochMetadata() : epoch(0), received(false), term(0), payload_hash(0), payload_size(0) {}
};

struct DnProposalStrategy {
    DnStrategyAction action;
    bool should_propose;
    int32_t target_epoch;
    int32_t jump_delta;
    int32_t lag;

    DnProposalStrategy()
        : action(DnStrategyAction::NORMAL),
          should_propose(true),
          target_epoch(0),
          jump_delta(0),
          lag(0)
    {
    }
};

struct DnEpochBatch {
    uint64_t epoch_id;
    std::vector<DnTransaction> transactions;

    DnEpochBatch() : epoch_id(0) {}
};

struct DnNetworkStats {
    uint64_t tx_bytes;
    uint64_t rx_bytes;
    uint64_t tx_count;
    uint64_t rx_count;
    uint64_t rx_append_full;
    uint64_t rx_append_meta;
    uint64_t rx_snapshot;
    uint64_t dropped_tx;
    uint64_t dropped_rx;
    int32_t max_observed_epoch;
    int32_t last_lag;
    size_t tx_queue_size;
    size_t rx_queue_size;

    DnNetworkStats()
        : tx_bytes(0),
          rx_bytes(0),
          tx_count(0),
          rx_count(0),
          rx_append_full(0),
          rx_append_meta(0),
          rx_snapshot(0),
          dropped_tx(0),
          dropped_rx(0),
          max_observed_epoch(0),
          last_lag(0),
          tx_queue_size(0),
          rx_queue_size(0)
    {
    }
};

#endif /* DYNAMIC_NET_DN_TYPES_H */
