#ifndef OPENGAUSS_E_STATISTICIAN_H
#define OPENGAUSS_E_STATISTICIAN_H

#include <cstdint>

/* 对端已封口到哪个 epoch（pacer 读它做调度参考） */
uint64_t EpochGetPeerMaxSealedEpoch(uint32_t peer_node);

/* 记录对端封口进度（收到对端收尾包时更新） */
void EpochNotePeerSealedEpoch(uint32_t peer_node, uint64_t epoch);

/* 多主同步模式下 backend 跳过 WAL（持久化交给 apply） */
bool EpochShouldSkipBackendWal(void);

#endif /* OPENGAUSS_E_STATISTICIAN_H */
