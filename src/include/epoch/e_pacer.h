#ifndef OPENGAUSS_E_PACER_H
#define OPENGAUSS_E_PACER_H

#include <cstdint>
#include <string>

struct E_WireTxn;

/*
 * Pacer 同步层。两张开关默认关，关着时这些函数都是空操作（Wait）。
 * DBR：epoch_dyn_net_catchup。上行持续堵住时，还没进发送队列的本地写集改到后面的 epoch，
 * 当前 epoch 不再计入它们，并照常发 EOF。
 * PA：epoch_dyn_net_proxy。三个及以上副本时，慢节点选定一个健康 proxy。
 * 其他健康节点不再把写集直接发到慢节点；proxy 把这些原始写集转到慢节点。
 * 慢节点下行上只剩 proxy 这一路。两个副本时没有第二路可合并，PA 不改发送。
 */

bool EpochPacerDbrEnabled(void);
bool EpochPacerPaEnabled(void);

/* 分配位置下界。0 表示不抬。 */
uint64_t EpochPacerAssignmentFloor(void);

/*
 * 上行持续堵住时把本地写集留在 Local。返回 true 表示调用方不要再入 merge/send。
 */
bool EpochPacerHoldLocal(const E_WireTxn& txn, const std::string& wire);

/*
 * 慢节点上，写集已经记在 seal 窗口外的目的 epoch。
 * 先放着，关窗时再后移或发送。健康节点返回 false，调用方照常 merge/send。
 */
bool EpochPacerParkLocal(uint64_t epoch, const std::string& wire);

/*
 * 关窗前调用。堵住则把本 epoch 上还没发送的写集改记到后面的 epoch，
 * 本 epoch 的 pack 计数不再包含它们。
 */
void EpochPacerBeforeSeal(uint64_t epoch);

/* 改记没有完成、写集仍挂在这个 epoch 上时，先不要发 EOF。 */
bool EpochPacerBlocksSeal(uint64_t epoch);

/* Pack#0：低水位放行，高水位做一次 DBR；慢节点按需发出 PA 会话。 */
void EpochPacerPackTick(void);

/*
 * PA 会话内，健康节点改为按副本单播，不把本 epoch 的写集直接发给慢节点。
 * 返回 true 表示已经发送，调用方不要再广播。
 */
bool EpochPacerRouteSend(const std::string& wire, const E_WireTxn& txn);

/* 控制消息返回 true（调用方不要再当普通事务处理）。proxy 顺带把健康来源转给慢节点。 */
bool EpochPacerOnIncoming(const E_WireTxn& txn, const std::string& wire);

#endif /* OPENGAUSS_E_PACER_H */
