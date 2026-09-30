#ifndef OPENGAUSS_E_APPLY_PROFILE_H
#define OPENGAUSS_E_APPLY_PROFILE_H

#include <cstdint>

#include "epoch/e_types.h"

enum EpochApplyProfilePhase {
    EAP_QUEUE_WAIT = 0,
    EAP_DESERIALIZE,
    EAP_SORT,
    EAP_TXN_BEGIN,
    EAP_TXN_COMMIT,
    EAP_HEAP_OPEN,
    EAP_TUPLE_DECODE,
    EAP_ESTATE_NEW,
    EAP_FIND_TUPLE,
    EAP_FIND_INDEX_SCAN,
    EAP_FIND_CHAIN_WALK,
    EAP_HEAP_INSERT,
    EAP_HEAP_UPDATE,
    EAP_HEAP_DELETE,
    EAP_CLEANUP,
    EAP_PHASE_COUNT
};

extern void EpochApplyProfileBeginWireTxn(void);
extern void EpochApplyProfileAddUs(EpochApplyProfilePhase phase, uint64_t us);
extern void EpochApplyProfileSetRowCount(uint64_t rows);
extern void EpochApplyProfileEndWireTxn(uint64_t epoch, uint64_t csn, uint32_t server_id, bool ok);

#define EPOCH_APPLY_CHAIN_DEPTH_LOG_THRESHOLD 16

extern void EpochApplyProfileRecordFind(Oid relid, uint64_t index_scan_us, uint64_t chain_walk_us, int chain_depth);

/*
 * apply 定位/落盘失败：计入 abort 且调用方将本行视为已处理（不 defer），保证 logical 推进。
 * is_local：相对本机 server_id 的 wire（tid 快路径）vs 远端（PK Find）。
 */
extern void EpochApplyProfileNoteApplyAbort(bool is_local, Oid relid, EpochOpType op);

/*
 * INSERT 分路计数（定位「冲突 skip」到底走 staging / Find / 真插入 / UNIQUE CATCH）。
 * is_local：相对本机 server_id 的 wire。
 */
enum EpochApplyInsertPath {
    EAP_INSERT_STAGING_HIT = 0, /* PK staging 命中 → skip */
    EAP_INSERT_FIND_HIT,        /* 索引 Find 命中 → skip */
    EAP_INSERT_DO,              /* 真正 heap_insert 成功 */
    EAP_INSERT_UNIQUE_CATCH,    /* heap_insert 后 UNIQUE 冲突 CATCH → skip */
    EAP_INSERT_PATH_COUNT
};

extern void EpochApplyProfileNoteInsertPath(EpochApplyInsertPath path, bool is_local, Oid relid);

/*
 * 兼容旧名：等价于 EAP_INSERT_UNIQUE_CATCH（勿再用于 staging/Find 命中）。
 */
extern void EpochApplyProfileNoteInsertPkConflict(bool is_local, Oid relid);

#endif /* OPENGAUSS_E_APPLY_PROFILE_H */
