#ifndef OPENGAUSS_E_PIPELINE_H
#define OPENGAUSS_E_PIPELINE_H

#include <cstdint>
#include <string>

extern void EpochPipelineInit(void);
extern void EpochPipelineShutdown(void);
/*
 * Pack 模式：由 epochcoord 调用一次做管理侧 Init（启 recv/logic/phys + pool）。
 * 数据面 seal/drain/send/merge/apply 不在此路径。幂等。
 */
extern void EpochPipelineBootstrapOwner(void);
extern bool EpochPipelineIsBootstrapped(void);
extern void EpochPipelinePoll(void);
/* 流水线：Recv / logical / physical 独立线程；merge+apply+seal 在主 worker */
extern void EpochPipelineStartRecvThread(void);
extern void EpochPipelineStopRecvThread(void);
extern void EpochPipelineStartMergeThread(void);
extern void EpochPipelineStopMergeThread(void);
extern void EpochPipelineStartApplyThread(void);
extern void EpochPipelineStopApplyThread(void);
extern void EpochPipelineStartLogicalAdvanceThread(void);
extern void EpochPipelineStopLogicalAdvanceThread(void);
extern void EpochPipelineStartPhysicalAdvanceThread(void);
extern void EpochPipelineStopPhysicalAdvanceThread(void);
extern bool EpochPipelineDrainRemoteMergeQueue(void);
extern bool EpochPipelinePollSendSeal(void);
/*
 * GeoGauss 式：merge 工人 Claim 优先对着 merge_epoch；
 * PullTick 仅 Drain/关窗/Feed 当前 merge_epoch 并在 complete 后 ++（须 < physical）。
 */
extern bool EpochPipelineMergeWorkerPullTick(uint32_t worker_id, uint32_t n_workers);
/*
 * Pack×N：自拉 TxnQueue → RemoteMergeEnqueue + send 队列；Pack#0 兼 seal。
 * Send×1：自拉 send 队列 → ZMQ（原 PublishWire）。
 */
extern bool EpochPipelinePackWorkerTick(uint32_t pack_id, uint32_t n_pack);
extern bool EpochPipelineSendWorkerTick(void);
extern bool EpochPipelinePackSendActive(void);
/* apply claim 门闩：仅 merge_complete(E) 后 E 才可被 apply */
extern bool EpochPipelineEpochMergeComplete(uint64_t epoch);
struct E_WireTxn;
/*
 * merge 当场判定赢家后：把原始 wire 入 apply 环（不 Build）。
 * payload 可为 merge task 缓冲；NULL 则现场 serialize txn。
 * 队列满返回 false（调用方下轮续投）。
 */
extern bool EpochPipelineTryEnqueueMergeWinnerOrig(const E_WireTxn& txn,
    uint64_t wire_key,
    const char* payload,
    uint32_t payload_len);
/* 无 Pack 旧路径：EpochWorker 做 seal+drain；Pack 活跃时不应再跑 */
extern bool EpochPipelineAdvanceMainWorkerStages(void);
extern bool EpochPipelineAllStagesReady(uint64_t epoch);
extern void EpochPipelineClearStageFlags(uint64_t epoch);
extern void EpochPipelineNoteLogicalAdvanceStall(uint64_t epoch);
extern void EpochPipelineResyncPeerReceived(uint64_t epoch, uint32_t from_node);
extern void EpochPipelineNotePeerEpochEndReceived(uint64_t epoch, uint32_t from_node);
extern void EpochPipelineFinishPeerRecvAfterEpochEnd(uint64_t epoch, uint32_t from_node);
extern void EpochPipelineNotePeerTxnWireProgress(uint64_t epoch, uint32_t from_node);
extern bool EpochPipelinePeerEpochEndReceived(uint64_t epoch, uint32_t peer);
/* self seal 后、AddLogicalEpoch 前：将 merged 落盘并推进 committed 计数 */
extern bool EpochPipelineTryApplyMergedEpoch(uint64_t epoch);
/* merger 中仍有待 apply 的对端写行（sync 关窗硬条件） */
extern bool EpochPipelineMergerHasPendingRemoteApply(uint64_t epoch);
extern uint32_t EpochPipelineCountUnappliedWireTxns(uint64_t epoch);
extern void EpochPipelineDiagLogFirstUnapplied(uint64_t epoch);
/* merge 诊断：expected=local_pack+Σpeer_should；wire_done=已完成 merge 的 wire txn 数 */
extern uint64_t EpochPipelineExpectedMergeTxnCount(uint64_t epoch);
extern uint64_t EpochPipelineSeenMergeTxnCount(uint64_t epoch);
/* merge 关窗后 abort_csn 补集：有写行且未 abort 的 wire 数（= apply heap 目标数） */
extern uint64_t EpochPipelineExpectedApplyWinnerCount(uint64_t epoch);
extern bool EpochPipelineEpochHasMergeInflight(uint64_t epoch);
extern bool EpochPipelineIsMergePhaseComplete(uint64_t epoch);
/* logical advance 诊断：仅读 stage atomic，不碰 bookkeeping 分片 map */
extern void EpochPipelineReadStageFlagsForDiag(uint64_t epoch, bool* physical_ready, bool* local_commit_ready,
    bool* self_send_sent, bool* merge_complete, bool* apply_complete);

extern void EpochPipelineInjectRemoteWireTxn(const E_WireTxn& txn);
/* 兼容旧名：实际由 Send worker 调用 ZMQ */
extern bool EpochPipelinePublishWire(const std::string& wire, const E_WireTxn& txn);
extern bool EpochPipelineSendWire(const std::string& wire, const E_WireTxn& txn);

#endif /* OPENGAUSS_E_PIPELINE_H */
