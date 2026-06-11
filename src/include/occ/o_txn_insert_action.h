//
// Created by hcj2003 on 10/7/25.
//

#ifndef OPENGAUSS_O_TXN_INSERT_ACTION_H
#define OPENGAUSS_O_TXN_INSERT_ACTION_H

#include "utils/relcache.h"
#include "occ/o_sentinel.h"

class O_TxnManager;

class InsItem {

};

class O_TxnInsertAction {
public:
    /** @brief Constructor. */
    O_TxnInsertAction()
    {}

    /** @brief Destructor. */
    ~O_TxnInsertAction();

    bool Init(O_TxnManager* _manager);

    void ExecuteOptimisticInsert(HeapTuple heap_tuple, occ_sentinel sentinel);

    inline void ClearSet()
    {
        m_insertSetSize = 0;
    };

private:
    O_TxnManager* m_manager = NULL;

    uint32_t m_insertSetSize = 0;

    uint32_t m_insertArraySize = 0;

    static const uint64_t INSERT_ARRAY_DEFAULT_SIZE = 64;

    InsItem* m_insertSet = NULL;

    void AddInsertToLocalAccess(HeapTuple heap_tuple, occ_sentinel pIndexInsertResult);
};

#endif  // OPENGAUSS_O_TXN_INSERT_ACTION_H
