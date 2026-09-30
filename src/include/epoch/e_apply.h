#ifndef OPENGAUSS_E_APPLY_H
#define OPENGAUSS_E_APPLY_H

#include "epoch/e_txn_wire.h"

extern bool E_ApplyWireTxn(const E_WireTxn& txn, bool is_local);
/* worker 路径：调用方已 EpochApplyProfileBeginWireTxn 并记录 queue_wait/deserialize */
extern bool E_ApplyWireTxnCore(const E_WireTxn& txn, bool is_local);
/* 允许 server_id==本节点（sync 后台 commit 路径） */
extern bool E_ApplyLocalWireTxn(const E_WireTxn& txn);

/* coordinator / apply worker 启动后预加载 FK 图，避免 apply 热路径首次扫 catalog */
extern void E_ApplyWarmup(void);

#endif /* OPENGAUSS_E_APPLY_H */
