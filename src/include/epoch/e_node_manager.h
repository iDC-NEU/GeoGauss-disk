#ifndef OPENGAUSS_E_NODE_MANAGER_H
#define OPENGAUSS_E_NODE_MANAGER_H

#include <cstdint>

class E_NodeManager {
public:
    static uint64_t GetPhysicalEpoch(void);
    static uint64_t AddPhysicalEpoch(void);
    static void SetPhysicalEpoch(uint64_t value);

    static uint64_t GetLogicalEpoch(void);
    static uint64_t AddLogicalEpoch(void);
    static void SetLogicalEpoch(uint64_t value);

    static void SetStoreShouldReceivedPackCounters(uint64_t epochId, uint64_t nodeId, uint64_t value);
    static void SetStoreReceivedPackCounters(uint64_t epochId, uint64_t nodeId, uint64_t value);
    static void SetLocalPackTxnCounters(uint64_t epochId, uint64_t nodeId, uint64_t value);

    static uint64_t IncChangeSetTxnCounters(uint64_t epochId, uint64_t nodeId);
    static uint64_t DecChangeSetTxnCounters(uint64_t epochId, uint64_t nodeId);
    static uint64_t GetChangeSetTxnCounters(uint64_t epochId, uint64_t nodeId);

    static uint64_t IncExecutingTxnCounters(uint64_t epochId, uint64_t nodeId);
    /* 成功 dec 返回 true；slot 非当前或计数已为 0 返回 false（不调用 EnsureEpochSlot）。 */
    static bool DecExecutingTxnCounters(uint64_t epochId, uint64_t nodeId);
    static uint64_t GetExecutingTxnCounters(uint64_t epochId, uint64_t nodeId);

    static uint64_t IncLocalPackTxnCounters(uint64_t epochId, uint64_t nodeId);
    static uint64_t DecLocalPackTxnCounters(uint64_t epochId, uint64_t nodeId);
    static uint64_t GetLocalPackTxnCounters(uint64_t epochId, uint64_t nodeId);
    static uint64_t GetSealedPackTxnCounters(uint64_t epochId, uint64_t nodeId);
    static void SetSealedPackTxnCounters(uint64_t epochId, uint64_t nodeId, uint64_t value);

    static uint64_t IncAbortTxnCounters(uint64_t epochId, uint64_t nodeId);
    static uint64_t GetAbortTxnCounters(uint64_t epochId, uint64_t nodeId);

    static uint64_t IncWriteAbortAfterSendTxnCounters(uint64_t epochId, uint64_t nodeId);
    static uint64_t GetWriteAbortAfterSendTxnCounters(uint64_t epochId, uint64_t nodeId);

    static uint64_t UpdateStoreShouldReceivedPackCounters(uint64_t epochId, uint64_t nodeId, uint64_t increment);
    static uint64_t GetStoreShouldReceivedPackCounters(uint64_t epochId, uint64_t nodeId);

    static uint64_t UpdateStoreReceivedPackCounters(uint64_t epochId, uint64_t nodeId, uint64_t increment);
    static uint64_t GetStoreReceivedPackCounters(uint64_t epochId, uint64_t nodeId);

    static void SetNodeSendCompleted(uint64_t epochId, uint64_t nodeId, bool completed);
    static bool IsNodeSendCompleted(uint64_t epochId, uint64_t nodeId);

    static void SetNodeReceiveCompleted(uint64_t epochId, uint64_t nodeId, bool completed);
    static bool IsNodeReceiveCompleted(uint64_t epochId, uint64_t nodeId);

    static uint64_t IncWireMergeDoneCounters(uint64_t epochId, uint64_t nodeId);
    static uint64_t GetWireMergeDoneCounters(uint64_t epochId, uint64_t nodeId);

    static uint64_t IncMergedTxnCounters(uint64_t epochId, uint64_t nodeId);
    static uint64_t DecMergedTxnCounters(uint64_t epochId, uint64_t nodeId);
    static uint64_t GetMergedTxnCounters(uint64_t epochId, uint64_t nodeId);
    /* merge 关窗后一次性写入 CRDT 净赢家数（替代流式 Inc/Dec） */
    static void SetMergedTxnCounters(uint64_t epochId, uint64_t nodeId, uint64_t value);

    static uint64_t IncCommittedTxnCounters(uint64_t epochId, uint64_t nodeId);
    static uint64_t GetCommittedTxnCounters(uint64_t epochId, uint64_t nodeId);

    static void ResetEpochCounters(uint64_t epochId);
    static void ClearEpochSlot(uint64_t epochId);
    static void EnsureEpochSlot(uint64_t epochId);

private:
    static size_t GetIndex(uint64_t epochId, uint64_t nodeId);
    static uint32_t GetSlot(uint64_t epochId);
};

#endif /* OPENGAUSS_E_NODE_MANAGER_H */
