#ifndef OPENGAUSS_E_RECV_LEDGER_H
#define OPENGAUSS_E_RECV_LEDGER_H

#include <cstdint>

#include "epoch/e_types.h"

/*
 * 接收账本：每个时间片一行，每行每个节点一条。
 *
 *   received  —— 该节点在这一片收了多少条写集
 *   declared  —— 收到它的收尾包时冻结，等于那一刻的 received
 *   eof       —— 该节点的收尾包到了没
 *
 * receive_epoch 是所有节点的公共游标：所有对端的 eof 都为 true 时 +1。
 */

/* 收到一条该节点的写集：received + 1 */
void EpochRecvNoteWire(uint64_t epoch, uint32_t node);

/* 收到该节点的收尾包：eof = true，declared = 当前 received */
void EpochRecvNoteEof(uint64_t epoch, uint32_t node);

/* 该节点的收尾包到了没 */
bool EpochRecvHasEof(uint64_t epoch, uint32_t node);

uint64_t EpochRecvDeclaredCount(uint64_t epoch, uint32_t node);
uint64_t EpochRecvReceivedCount(uint64_t epoch, uint32_t node);

/* 所有对端（不含本端）的 eof 都为 true */
bool EpochRecvAllEof(uint64_t epoch);

/* 迟到消息计数（epoch 与当前接收片不一致而被丢弃） */
void EpochRecvNoteStale(void);
uint64_t EpochRecvStaleCount(void);

/* 从某节点"连续收到 EOF"到第几片（用于给发送端回执） */
uint64_t EpochRecvAckUpto(uint32_t node);

uint64_t EpochRecvGetEpoch(void);
bool EpochRecvTryAdvanceEpoch(uint64_t epoch);

void EpochRecvLedgerInit(void);

/* 记账出错时直接崩，并打印明细 */

#endif /* OPENGAUSS_E_RECV_LEDGER_H */
