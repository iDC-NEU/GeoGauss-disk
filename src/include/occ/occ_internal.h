#ifndef OPENGAUSS_OCC_INTERNAL_H
#define OPENGAUSS_OCC_INTERNAL_H

#include "occ/o_txn.h"
#include "occ/o_access.h"
#include "knl/knl_session.h"
#include "occ/occ_mem.h"
#include "storage/item/itemptr.h"
#include "utils/rel.h"

#define abortParentTransactionParamsNoDetail(error, msg, ...) \
    ereport(ERROR, (errmodule(MOD_MOT), errcode(error), errmsg(msg, __VA_ARGS__)));

inline void OccLogMemoryAllocationFailure(const char *function, const char *file, int line, size_t size)
{
    // This function is designed for debugging purposes
    // You can set a breakpoint here to catch memory allocation failures
    ereport(WARNING, (errmodule(MOD_MOT),
                      errmsg("Memory allocation failure in %s at %s:%d, size: %zu bytes"),
                      function, file, line, size));
}

inline O_TxnManager *GetTxnManager()
{
    if (!u_sess->occ_cxt.txn_manager)
    {
        O_TxnManager *txnManager = nullptr;
        /* 使用 occ_malloc 分配在事务上下文之外，避免 Portal/Query 上下文回收后悬空 */
        void *buffer = occ_session_malloc(sizeof(O_TxnManager));

        if (!buffer)
        {
            OccLogMemoryAllocationFailure(__FUNCTION__, __FILE__, __LINE__, sizeof(O_TxnManager));
            return nullptr;
        }

        txnManager = new (buffer) O_TxnManager();

        if (!txnManager->Init())
        {
            txnManager->~O_TxnManager();
            occ_session_free(buffer);
            return nullptr;
        }

        u_sess->occ_cxt.txn_manager = txnManager;
    }
    return u_sess->occ_cxt.txn_manager;
}

/* 开关：true = commit 写 heap 后镜像 delta，读路径优先查 delta 分片表 */
extern bool g_enable_occ_delta;

/* occ_enable_stats_log GUC：true 时每次 commit/abort 写 OCC_STATS；false 时每 1024 次采样 */
extern bool g_occ_enable_stats_log;

/* 简单 C 接口，方便测试或后续挂接到 GUC */
inline void O_SetDeltaMode(bool enable_delta)
{
    g_enable_occ_delta = enable_delta;
}

/* 从 sentinel 安全解析 relid，避免使用失效 relation 指针或 arena 残留垃圾值 */
static inline Oid OccResolveRelid(const occ_sentinel &sent)
{
    if (OidIsValid(sent.relid)) {
        return sent.relid;
    }
    if (sent.relation != nullptr) {
        return RelationGetRelid(sent.relation);
    }
    return InvalidOid;
}

/* 写集校验/写回时解析表 OID：优先 access，再回退写集 key 与本地 tuple */
static inline Oid OccResolveAccessRelid(O_Access *access, const occ_sentinel *key = nullptr)
{
    if (access == nullptr) {
        return InvalidOid;
    }

    Oid relid = OccResolveRelid(access->m_origSentinel);
    if (!OidIsValid(relid) && key != nullptr) {
        relid = OccResolveRelid(*key);
    }
    if (!OidIsValid(relid) && access->m_globalRow != nullptr &&
        OidIsValid(access->m_globalRow->t_tableOid)) {
        relid = access->m_globalRow->t_tableOid;
    }
    if (!OidIsValid(relid) && access->m_localInsertRow != nullptr &&
        OidIsValid(access->m_localInsertRow->t_tableOid)) {
        relid = access->m_localInsertRow->t_tableOid;
    }
    if (!OidIsValid(relid) && access->m_localRow != nullptr &&
        OidIsValid(access->m_localRow->t_tableOid)) {
        relid = access->m_localRow->t_tableOid;
    }
    return relid;
}

/* 块号粗校验：拒绝 arena 残留垃圾 TID，避免 ReadBuffer 抛 ERROR */
static inline bool OccTidIsPlausible(const ItemPointerData *tid, Relation rel = nullptr)
{
    if (tid == nullptr || !ItemPointerIsValid(tid)) {
        return false;
    }
    BlockNumber blk = ItemPointerGetBlockNumber(tid);
    if (blk == InvalidBlockNumber) {
        return false;
    }
    if (rel != nullptr) {
        return blk < RelationGetNumberOfBlocks(rel);
    }
    return blk < 10000000;
}

/* 从 access / 写集 key / 本地 tuple 解析 TID（按访问类型区分，避免 WR 误用 localRow 的新 TID） */
static inline ItemPointerData OccResolveAccessTid(O_Access *access, const occ_sentinel *key = nullptr)
{
    ItemPointerData tid;
    ItemPointerSetInvalid(&tid);

    if (access == nullptr) {
        return tid;
    }

    if (access->m_type == INS) {
        if (access->m_localInsertRow != nullptr &&
            OccTidIsPlausible(&access->m_localInsertRow->t_self)) {
            return access->m_localInsertRow->t_self;
        }
        if (key != nullptr && OccTidIsPlausible(&key->tuple_id)) {
            return key->tuple_id;
        }
        if (OccTidIsPlausible(&access->m_origSentinel.tuple_id)) {
            return access->m_origSentinel.tuple_id;
        }
        return tid;
    }

    if (access->m_type == WR || access->m_type == DEL) {
        if (key != nullptr && OccTidIsPlausible(&key->tuple_id)) {
            return key->tuple_id;
        }
        if (OccTidIsPlausible(&access->m_origSentinel.tuple_id)) {
            return access->m_origSentinel.tuple_id;
        }
        if (access->m_globalRow != nullptr &&
            OccTidIsPlausible(&access->m_globalRow->t_self)) {
            return access->m_globalRow->t_self;
        }
        return tid;
    }

    if (key != nullptr && OccTidIsPlausible(&key->tuple_id)) {
        return key->tuple_id;
    }
    if (OccTidIsPlausible(&access->m_origSentinel.tuple_id)) {
        return access->m_origSentinel.tuple_id;
    }
    if (access->m_globalRow != nullptr && OccTidIsPlausible(&access->m_globalRow->t_self)) {
        return access->m_globalRow->t_self;
    }
    if (access->m_localRow != nullptr && OccTidIsPlausible(&access->m_localRow->t_self)) {
        return access->m_localRow->t_self;
    }
    if (access->m_localInsertRow != nullptr &&
        OccTidIsPlausible(&access->m_localInsertRow->t_self)) {
        return access->m_localInsertRow->t_self;
    }
    return tid;
}

/* 写回/校验前统一解析 relid + TID 并写回 access */
static inline bool OccPrepareAccessTarget(O_Access *access, const occ_sentinel *key,
                                          Oid *relid, ItemPointerData *tid)
{
    if (access == nullptr || relid == nullptr || tid == nullptr) {
        return false;
    }
    *relid = OccResolveAccessRelid(access, key);
    *tid = OccResolveAccessTid(access, key);
    if (!OidIsValid(*relid) || !OccTidIsPlausible(tid)) {
        return false;
    }
    access->m_origSentinel.relid = *relid;
    access->m_origSentinel.tuple_id = *tid;
    return true;
}

/* 当前会话是否存在 OCC 写集（用于跳过只读路径上的 OCC 扫描开销） */
inline bool O_TxnHasActiveWriteSet()
{
    O_TxnManager *txn = u_sess->occ_cxt.txn_manager;
    if (txn == nullptr || txn->m_accessMgr == nullptr) {
        return false;
    }
    return txn->m_accessMgr->m_Size() > 0;
}

#endif // OPENGAUSS_OCC_INTERNAL_H