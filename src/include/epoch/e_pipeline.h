#ifndef OPENGAUSS_E_PIPELINE_H
#define OPENGAUSS_E_PIPELINE_H

#include <cstdint>
#include <string>

extern void EpochPipelineInit(void);
extern void EpochPipelineShutdown(void);
extern void EpochPipelinePoll(void);
extern void EpochPipelineRebroadcastCachedTxns(uint64_t epoch);
extern void EpochPipelineResyncPeerReceived(uint64_t epoch, uint32_t from_node);
extern bool EpochPipelineHasWireCache(uint64_t epoch);
extern uint64_t EpochPipelineCachedPackCount(uint64_t epoch);
/* self seal 后、AddLogicalEpoch 前：将 merged 落盘并推进 committed 计数 */
extern bool EpochPipelineTryApplyMergedEpoch(uint64_t epoch);

struct E_WireTxn;
extern void EpochPipelineInjectRemoteWireTxn(const E_WireTxn& txn);
extern bool EpochPipelinePublishWire(const std::string& wire, const E_WireTxn& txn);

#endif /* OPENGAUSS_E_PIPELINE_H */
