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
/* 独立的状态打印线程（只打日志，不参与业务锁，用于区分卡住 vs 崩溃） */
extern void EpochPipelineStartStateLogThread(void);
extern void EpochPipelineStopStateLogThread(void);
extern void EpochPipelineStopPhysicalAdvanceThread(void);
extern bool EpochPipelineDrainRemoteMergeQueue(void);
/*
 * GeoGauss 式：merge 工人 Claim 优先对着 merge_epoch；
 * PullTick 仅 Drain/关窗/Feed 当前 merge_epoch 并在 complete 后 ++（须 < physical）。
 */
extern bool EpochPipelineMergeWorkerPullTick(uint32_t worker_id, uint32_t n_workers);
/*
 * Pack×N：自拉 TxnQueue → RemoteMergeEnqueue + send 队列；Pack#0 兼 seal。
 * Send×1：自拉 send 队列 → ZMQ（原 PublishWire）。
 */
extern bool EpochPipelineSendWorkerTick(uint32_t send_id, uint32_t n_send);

/* 诊断：merge 停滞时打印账本细节 */
extern void EpochPipelineLogMergeStall(void);

/* 为每个对端起一条发送线程（send worker 启动时调用一次） */
extern void EpochPipelineStartPeerSendThreads(void);
extern bool EpochPipelinePackSendActive(void);
/* apply claim 门闩：仅 merge_complete(E) 后 E 才可被 apply */
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
extern bool EpochPipelineAllStagesReady(uint64_t epoch);
extern void EpochPipelineClearStageFlags(uint64_t epoch);

/* merge 线程合并完一笔后的收尾动作（原 OnAsyncMergeTaskDone 内联到 worker，这里只导出零件） */
extern void EpochPipelineRevokeAcceptedWire(uint64_t epoch, uint64_t wire_key);
extern bool EpochPipelineWireTxnHasWritableRows(const E_WireTxn& txn);
extern void EpochPipelineMarkWireTxnApplied(uint64_t epoch, uint64_t wire_key);
extern void EpochPipelineSkipWireTxnForApply(uint64_t epoch, uint64_t wire_key, bool is_merge_abort);
extern void EpochPipelineEnqueueApplyWinner(const E_WireTxn& txn);

/* merge 线程每合并完一笔调用：merged++ 并把赢家推进 apply 队列 */
extern void EpochPipelineNoteLogicalAdvanceStall(uint64_t epoch);
extern void EpochPipelineFinishPeerRecvAfterEpochEnd(uint64_t epoch, uint32_t from_node);
/* self seal 后、AddLogicalEpoch 前：将 merged 落盘并推进 committed 计数 */
extern bool EpochPipelineTryApplyMergedEpoch(uint64_t epoch);
/* merger 中仍有待 apply 的对端写行（sync 关窗硬条件） */
extern bool EpochPipelineMergerHasPendingRemoteApply(uint64_t epoch);
extern uint32_t EpochPipelineCountUnappliedWireTxns(uint64_t epoch);
extern void EpochPipelineDiagLogFirstUnapplied(uint64_t epoch);
/* merge 诊断：expected=local_pack+Σpeer_should；wire_done=已完成 merge 的 wire txn 数 */
extern uint64_t EpochPipelineExpectedMergeTxnCount(uint64_t epoch);
extern uint64_t EpochPipelineMergedTxnCount(uint64_t epoch);
/* merge 关窗后 abort_csn 补集：有写行且未 abort 的 wire 数（= apply heap 目标数） */
extern uint64_t EpochPipelineExpectedApplyWinnerCount(uint64_t epoch);
extern bool EpochPipelineEpochHasMergeInflight(uint64_t epoch);
extern bool EpochPipelineIsMergePhaseComplete(uint64_t epoch);
/* logical advance 诊断：仅读 stage atomic，不碰 bookkeeping 分片 map */
extern void EpochPipelineReadStageFlagsForDiag(uint64_t epoch, bool* physical_ready, bool* local_commit_ready,
    bool* self_send_sent, bool* merge_complete, bool* apply_complete);

extern void EpochPipelineInjectRemoteWireTxn(const E_WireTxn& txn);
/* DBR 放行：本地 merge + 发送队列。wire 里的 epoch 已是最终提交 epoch。 */
/* 返回 false 表示 merge 环这格已满，调用方保留写集下个 tick 再试。 */
extern bool EpochPipelineAdmitLocalWire(const E_WireTxn& txn, const std::string& wire);
/* 兼容旧名：实际由 Send worker 调用 ZMQ */

#endif /* OPENGAUSS_E_PIPELINE_H */
