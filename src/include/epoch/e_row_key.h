#ifndef OPENGAUSS_E_ROW_KEY_H
#define OPENGAUSS_E_ROW_KEY_H

#include <string>

#include "epoch/e_txn_wire.h"

struct HeapTupleData;
typedef HeapTupleData* HeapTuple;
struct RelationData;
typedef RelationData* Relation;

static constexpr uint16_t E_ROW_PK_MAX_LEN = 128;

bool E_HeapTupleFromWireRow(const E_WireRow& row, HeapTuple* out);
void E_FreeWireHeapTuple(HeapTuple tup);

bool E_BuildPkKey(Relation rel, HeapTuple tup, std::string* out);
bool E_BuildPkKey(Relation rel, HeapTuple tup, char* pk_buf, uint16_t pk_buf_cap, uint16_t* pk_len);
bool E_BuildPkKeyFromWireRow(uint32_t relid, const E_WireRow& row, std::string* out);
bool E_BuildPkKeyFromWireRow(uint32_t relid, const E_WireRow& row, char* pk_buf, uint16_t pk_buf_cap,
    uint16_t* pk_len);
Oid E_ResolveLocalRelid(const E_WireRow& row);

#endif /* OPENGAUSS_E_ROW_KEY_H */
