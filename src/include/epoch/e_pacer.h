#ifndef OPENGAUSS_E_PACER_H
#define OPENGAUSS_E_PACER_H

#include <cstdint>
#include <string>

struct E_WireTxn;

/*
 * Pacer 同步层。
 *
 * DBR（epoch_dyn_net_catchup）：只在发送线程里生效——
 *   一条写集要发给某个 peer 时，如果该 peer 的发送通道当前不可写（拥塞），
 *   就丢掉这一条（不入 ZMQ）；收尾包永远发送。
 *   丢弃是安全的：对端的 declared 在收到 EOF 时冻结为"实际收到的条数"。
 *
 * PA（epoch_dyn_net_proxy）：三个及以上副本时，慢节点选定一个健康 proxy，
 *   其他健康节点不再把写集直接发给慢节点，改由 proxy 归并后只转发获胜行。
 */

bool EpochPacerDbrEnabled(void);

/* 本节点是否在"慢节点"名单里 */
bool EpochPacerIsSlowSelf(void);

/* PA 开关 */
bool EpochPacerPaEnabled(void);

/* DBR：现在能不能发。true = 上行已拥塞，先不发 */
bool EpochPacerShouldDefer(void);

/*
 * 每条写集的统一决策：返回 1 = 发送、0 = 丢弃。
 * CAS 保证同一笔（epoch, index）只决策一次，多个 peer 线程读到同一结论。
 */
int EpochPacerDecideItem(uint64_t epoch, uint64_t csn, uint32_t server_id, bool is_eof);

/* 清理过期的决策记录（避免 map 无限增长） */
void EpochPacerSweepDecisions(uint64_t up_to_epoch);

/* 延后目标片：>= epoch+4 且 > physical+1 */
uint64_t EpochPacerDeferTarget(uint64_t epoch);

/* 该笔已发出/已放弃：清掉延后计数 */
void EpochPacerDeferDone(uint64_t csn, uint32_t server_id);

/*
 * 自环入队的固定负责人（最小的对端 id）。
 * 必须由单一线程入队，否则同片的写入顺序会被打乱：EOF 可能先于写集进入
 * 接收队列，declared 提前冻结，后续写集到达后 received > declared，
 * 该片永远判定不完成（实测 recv 卡在 70、snap_to 涨到 500+）。
 */
uint32_t EpochPacerSelfLoopPeer(void);

/* DBR 丢弃计数（诊断） */
void EpochPacerNoteDropped(void);

/* merge 入队失败重试（背压），正常应为 0 */
void EpochPacerNoteMergeRetry(void);
uint64_t EpochPacerMergeRetryCount(void);
uint64_t EpochPacerDroppedCount(void);

/* 发送线程每 tick 调用：慢节点按需发出 PA 会话 */
void EpochPacerPackTick(void);

/* proxy 节点 id（第一台不在慢名单里的节点） */
uint32_t EpochPacerPaProxyNodeId(void);

/* ---------- PA：慢节点侧 ---------- */
/* 采样下行速率，状态翻转时把 on/off 广播给 proxy（由 PackTick 调用） */
void EpochPacerPaSlowSideTick(void);

/* ---------- PA：proxy 侧 ---------- */
/* 收到慢节点广播的状态（在收包线程里调用） */
void EpochPacerPaNoteSlowState(bool congested);

/*
 * 发送端：这一片要不要跳过慢节点。
 * 第一次遇到该片时按当前拥塞状态决定并记录，后续同片沿用同一结论。
 */
bool EpochPacerPaShouldSkipPeer(uint32_t peer, uint64_t epoch);

/* merge 端：每合并完一笔调用；被跳过片的赢家会进入转发缓冲 */
void EpochPacerPaMaybeBufferMerged(const E_WireTxn& txn);

/* merge 端：该片 merge 完成时调用，把缓冲的赢家 + EOF 重发给慢节点 */
void EpochPacerPaResendEpoch(uint64_t epoch);

/* 非代理节点：收到代理广播的"该片是否跳过"决策 */
void EpochPacerPaNoteEpochDecision(uint64_t epoch, bool skip);

#endif /* OPENGAUSS_E_PACER_H */
