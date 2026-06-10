#ifndef OPENGAUSS_OCC_INTERNAL_H
#define OPENGAUSS_OCC_INTERNAL_H

#include "occ/o_txn.h"
#include "knl/knl_session.h"
#include "occ/occ_mem.h"

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
//        void *buffer = occ_malloc(sizeof(O_TxnManager));
        void *buffer = palloc(sizeof(O_TxnManager));

        if (!buffer)
        {
            OccLogMemoryAllocationFailure(__FUNCTION__, __FILE__, __LINE__, sizeof(O_TxnManager));
            return nullptr;
        }

        txnManager = new (buffer) O_TxnManager();

        if (!txnManager->Init())
        {
            txnManager->~O_TxnManager();
            pfree(buffer);
            return nullptr;
        }

        u_sess->occ_cxt.txn_manager = txnManager;
    }
    return u_sess->occ_cxt.txn_manager;
}

/* 开关：是否启用 OCC delta 覆盖表（true = 使用 delta，不直接写 heap） */
extern bool g_enable_occ_delta;

/* 简单 C 接口，方便测试或后续挂接到 GUC */
inline void O_SetDeltaMode(bool enable_delta)
{
    g_enable_occ_delta = enable_delta;
}

#endif // OPENGAUSS_OCC_INTERNAL_H