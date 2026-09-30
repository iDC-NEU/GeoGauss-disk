#include "postgres.h"

#include "epoch/e_row_key.h"

#include "epoch/e_merge_profile.h"

#include "access/heapam.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "catalog/index.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "portability/instr_time.h"

struct E_PkKeyWriter {
    char* buf;
    uint16_t cap;
    uint16_t len;

    bool HasRoom(uint16_t need) const
    {
        return len + need <= cap;
    }

    bool AppendByte(char c)
    {
        if (!HasRoom(1)) {
            return false;
        }
        buf[len++] = c;
        return true;
    }

    bool AppendBytes(const void* data, uint16_t size)
    {
        if (!HasRoom(size)) {
            return false;
        }
        errno_t rc = memcpy_s(buf + len, cap - len, data, size);
        securec_check(rc, "\0", "\0");
        len = static_cast<uint16_t>(len + size);
        return true;
    }
};

static void E_SetItemPointer(ItemPointer tid, const E_WireRow& row)
{
    tid->ip_blkid.bi_hi = row.bi_hi;
    tid->ip_blkid.bi_lo = row.bi_lo;
    tid->ip_posid = row.ip_posid;
}

static bool E_AppendDatumToPkKey(Datum value, bool isnull, Oid typeOid, E_PkKeyWriter* out)
{
    if (out == nullptr) {
        return false;
    }
    if (isnull) {
        return out->AppendByte('\0');
    }
    if (!out->AppendByte('\1')) {
        return false;
    }
    switch (typeOid) {
        case INT2OID: {
            int16 v = DatumGetInt16(value);
            return out->AppendBytes(&v, sizeof(v));
        }
        case INT4OID: {
            int32 v = DatumGetInt32(value);
            return out->AppendBytes(&v, sizeof(v));
        }
        case INT8OID: {
            int64 v = DatumGetInt64(value);
            return out->AppendBytes(&v, sizeof(v));
        }
        case BOOLOID: {
            char v = DatumGetBool(value) ? 1 : 0;
            return out->AppendByte(v);
        }
        case TEXTOID:
        case VARCHAROID: {
            text* t = DatumGetTextPP(value);
            int len = VARSIZE_ANY_EXHDR(t);
            const char* data = VARDATA_ANY(t);
            uint32_t ulen = static_cast<uint32_t>(len);
            if (!out->AppendBytes(&ulen, sizeof(ulen))) {
                return false;
            }
            return out->AppendBytes(data, static_cast<uint16_t>(len));
        }
        default: {
            int16 typlen = 0;
            bool typbyval = false;
            get_typlenbyval(typeOid, &typlen, &typbyval);
            if (typbyval) {
                return out->AppendBytes(&value, sizeof(Datum));
            }
            if (typlen == -1) {
                struct varlena* vl = DatumGetByteaPP(value);
                int len = VARSIZE_ANY(vl);
                return out->AppendBytes(vl, static_cast<uint16_t>(len));
            }
            if (typlen > 0) {
                return out->AppendBytes(DatumGetPointer(value), static_cast<uint16_t>(typlen));
            }
            return false;
        }
    }
}

static bool E_AppendTidToPkKey(const E_WireRow& row, E_PkKeyWriter* out)
{
    if (out == nullptr) {
        return false;
    }
    if (!out->AppendByte('T')) {
        return false;
    }
    if (!out->AppendBytes(&row.bi_hi, sizeof(row.bi_hi))) {
        return false;
    }
    if (!out->AppendBytes(&row.bi_lo, sizeof(row.bi_lo))) {
        return false;
    }
    return out->AppendBytes(&row.ip_posid, sizeof(row.ip_posid));
}

bool E_HeapTupleFromWireRow(const E_WireRow& row, HeapTuple* out)
{
    if (out == nullptr || row.tuple_data.empty()) {
        return false;
    }
    Size allocSize = HEAPTUPLESIZE + row.tuple_data.size();
    HeapTuple tuple = (HeapTuple)palloc(allocSize);
    tuple->tupTableType = HEAP_TUPLE;
    tuple->t_len = row.tuple_data.size();
    E_SetItemPointer(&tuple->t_self, row);
    tuple->t_tableOid = row.relid;
    tuple->t_bucketId = InvalidBktId;
    HeapTupleSetZeroBase(tuple);
    tuple->t_data = (HeapTupleHeader)((char*)tuple + HEAPTUPLESIZE);
    errno_t rc = memcpy_s(tuple->t_data, row.tuple_data.size(), row.tuple_data.data(), row.tuple_data.size());
    securec_check(rc, "\0", "\0");
    *out = tuple;
    return true;
}

void E_FreeWireHeapTuple(HeapTuple tup)
{
    if (tup != nullptr) {
        pfree(tup);
    }
}

bool E_BuildPkKey(Relation rel, HeapTuple tup, char* pk_buf, uint16_t pk_buf_cap, uint16_t* pk_len)
{
    if (rel == nullptr || tup == nullptr || pk_buf == nullptr || pk_len == nullptr || pk_buf_cap == 0) {
        return false;
    }
    instr_time index_start;
    INSTR_TIME_SET_CURRENT(index_start);
    E_PkKeyWriter writer = {pk_buf, pk_buf_cap, 0};
    Oid pkOid = RelationGetPrimaryKeyIndex(rel);
    if (!OidIsValid(pkOid)) {
        return false;
    }

    Relation idxRel = index_open(pkOid, AccessShareLock);
    TupleDesc heapDesc = RelationGetDescr(rel);
    int nkeys = IndexRelationGetNumberOfKeyAttributes(idxRel);
    for (int i = 0; i < nkeys; ++i) {
        AttrNumber attno = idxRel->rd_index->indkey.values[i];
        bool isnull = false;
        Datum value = heap_getattr(tup, attno, heapDesc, &isnull);
        Oid typeOid = heapDesc->attrs[attno - 1].atttypid;
        if (!E_AppendDatumToPkKey(value, isnull, typeOid, &writer)) {
            index_close(idxRel, AccessShareLock);
            return false;
        }
    }
    index_close(idxRel, AccessShareLock);
    instr_time index_end;
    INSTR_TIME_SET_CURRENT(index_end);
    INSTR_TIME_SUBTRACT(index_end, index_start);
    EpochMergeProfileAddUs(EMP_PK_INDEX_ATTRS, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(index_end)));
    *pk_len = writer.len;
    return true;
}

bool E_BuildPkKey(Relation rel, HeapTuple tup, std::string* out)
{
    if (out == nullptr) {
        return false;
    }
    char buf[E_ROW_PK_MAX_LEN];
    uint16_t len = 0;
    if (!E_BuildPkKey(rel, tup, buf, sizeof(buf), &len)) {
        return false;
    }
    out->assign(buf, len);
    return true;
}

Oid E_ResolveLocalRelid(const E_WireRow& row)
{
    if (!row.relname.empty()) {
        Oid local = get_relname_relid(row.relname.c_str(), PG_PUBLIC_NAMESPACE);
        if (OidIsValid(local)) {
            return local;
        }
    }
    return row.relid;
}

bool E_BuildPkKeyFromWireRow(uint32_t relid, const E_WireRow& row, char* pk_buf, uint16_t pk_buf_cap,
    uint16_t* pk_len)
{
    if (pk_buf == nullptr || pk_len == nullptr || pk_buf_cap == 0) {
        return false;
    }
    Oid localRelid = E_ResolveLocalRelid(row);
    instr_time heap_start;
    INSTR_TIME_SET_CURRENT(heap_start);
    Relation rel = heap_open(localRelid, AccessShareLock);
    instr_time heap_end;
    INSTR_TIME_SET_CURRENT(heap_end);
    INSTR_TIME_SUBTRACT(heap_end, heap_start);
    EpochMergeProfileAddUs(EMP_PK_HEAP_OPEN, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(heap_end)));
    if (rel == nullptr) {
        return false;
    }
    HeapTuple tuple = nullptr;
    bool ok = false;
    instr_time tuple_start;
    INSTR_TIME_SET_CURRENT(tuple_start);
    if (E_HeapTupleFromWireRow(row, &tuple)) {
        instr_time tuple_end;
        INSTR_TIME_SET_CURRENT(tuple_end);
        INSTR_TIME_SUBTRACT(tuple_end, tuple_start);
        EpochMergeProfileAddUs(EMP_PK_TUPLE_DECODE, static_cast<uint64_t>(INSTR_TIME_GET_MICROSEC(tuple_end)));
        ok = E_BuildPkKey(rel, tuple, pk_buf, pk_buf_cap, pk_len);
        if (!ok) {
            E_PkKeyWriter writer = {pk_buf, pk_buf_cap, 0};
            ok = E_AppendTidToPkKey(row, &writer);
            if (ok) {
                *pk_len = writer.len;
            }
        }
        E_FreeWireHeapTuple(tuple);
    }
    heap_close(rel, AccessShareLock);
    return ok;
}

bool E_BuildPkKeyFromWireRow(uint32_t relid, const E_WireRow& row, std::string* out)
{
    if (out == nullptr) {
        return false;
    }
    char buf[E_ROW_PK_MAX_LEN];
    uint16_t len = 0;
    if (!E_BuildPkKeyFromWireRow(relid, row, buf, sizeof(buf), &len)) {
        return false;
    }
    out->assign(buf, len);
    return true;
}
