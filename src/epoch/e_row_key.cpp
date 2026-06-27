#include "postgres.h"

#include "epoch/e_row_key.h"

#include "access/heapam.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

static void E_SetItemPointer(ItemPointer tid, const E_WireRow& row)
{
    tid->ip_blkid.bi_hi = row.bi_hi;
    tid->ip_blkid.bi_lo = row.bi_lo;
    tid->ip_posid = row.ip_posid;
}

static void E_AppendDatumToPkKey(Datum value, bool isnull, Oid typeOid, std::string* out)
{
    if (isnull) {
        out->push_back('\0');
        return;
    }
    out->push_back('\1');
    switch (typeOid) {
        case INT2OID: {
            int16 v = DatumGetInt16(value);
            out->append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case INT4OID: {
            int32 v = DatumGetInt32(value);
            out->append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case INT8OID: {
            int64 v = DatumGetInt64(value);
            out->append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case BOOLOID: {
            char v = DatumGetBool(value) ? 1 : 0;
            out->push_back(v);
            break;
        }
        case TEXTOID:
        case VARCHAROID: {
            text* t = DatumGetTextPP(value);
            int len = VARSIZE_ANY_EXHDR(t);
            const char* data = VARDATA_ANY(t);
            uint32_t ulen = static_cast<uint32_t>(len);
            out->append(reinterpret_cast<const char*>(&ulen), sizeof(ulen));
            out->append(data, len);
            break;
        }
        default: {
            int16 typlen = 0;
            bool typbyval = false;
            get_typlenbyval(typeOid, &typlen, &typbyval);
            if (typbyval) {
                out->append(reinterpret_cast<const char*>(&value), sizeof(Datum));
            } else if (typlen == -1) {
                struct varlena* vl = DatumGetByteaPP(value);
                int len = VARSIZE_ANY(vl);
                out->append(reinterpret_cast<const char*>(vl), len);
            } else if (typlen > 0) {
                out->append(DatumGetPointer(value), typlen);
            }
            break;
        }
    }
}

static void E_AppendTidToPkKey(const E_WireRow& row, std::string* out)
{
    out->push_back('T');
    out->append(reinterpret_cast<const char*>(&row.bi_hi), sizeof(row.bi_hi));
    out->append(reinterpret_cast<const char*>(&row.bi_lo), sizeof(row.bi_lo));
    out->append(reinterpret_cast<const char*>(&row.ip_posid), sizeof(row.ip_posid));
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

bool E_BuildPkKey(Relation rel, HeapTuple tup, std::string* out)
{
    if (rel == nullptr || tup == nullptr || out == nullptr) {
        return false;
    }
    out->clear();
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
        E_AppendDatumToPkKey(value, isnull, typeOid, out);
    }
    index_close(idxRel, AccessShareLock);
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

bool E_BuildPkKeyFromWireRow(uint32_t relid, const E_WireRow& row, std::string* out)
{
    if (out == nullptr) {
        return false;
    }
    Oid localRelid = E_ResolveLocalRelid(row);
    Relation rel = heap_open(localRelid, AccessShareLock);
    if (rel == nullptr) {
        return false;
    }
    HeapTuple tuple = nullptr;
    bool ok = false;
    if (E_HeapTupleFromWireRow(row, &tuple)) {
        ok = E_BuildPkKey(rel, tuple, out);
        if (!ok) {
            E_AppendTidToPkKey(row, out);
            ok = true;
        }
        E_FreeWireHeapTuple(tuple);
    }
    heap_close(rel, AccessShareLock);
    return ok;
}
