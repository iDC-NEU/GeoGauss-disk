#ifndef OPENGAUSS_E_HEAP_MERGE_H
#define OPENGAUSS_E_HEAP_MERGE_H

#include "epoch/e_txn_wire.h"

/* bootstrap merge：读 heap m_rowCSN 与 wire expected_csn 对比 */
extern bool EpochHeapFindRowCsn(const E_WireRow& row, uint64_t* out_csn, bool* out_found);

#endif /* OPENGAUSS_E_HEAP_MERGE_H */
