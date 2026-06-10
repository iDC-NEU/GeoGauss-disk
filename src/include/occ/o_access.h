//
// Created by hcj2003 on 10/7/25.
//

#ifndef OPENGAUSS_O_ACCESS_H
#define OPENGAUSS_O_ACCESS_H

#include "occ/o_global.h"
#include "occ/o_sentinel.h"

#include "nodes/execnodes.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "occ/occ_mem.h"

static IndexInfo *O_makeIndexInfo(int numattrs, List *expressions, List *predicates, bool unique, bool isready, bool concurrent)
{
    IndexInfo *n = (IndexInfo *)occ_malloc(sizeof(IndexInfo));

    n->ii_NumIndexAttrs = numattrs;
    n->ii_Unique = unique;
    n->ii_ReadyForInserts = isready;
    n->ii_Concurrent = concurrent;

    /* expressions */
    n->ii_Expressions = expressions;
    n->ii_ExpressionsState = NIL;

    /* predicates  */
    n->ii_Predicate = predicates;
    n->ii_PredicateState = NULL;

    /* exclusion constraints */
    n->ii_ExclusionOps = NULL;
    n->ii_ExclusionProcs = NULL;
    n->ii_ExclusionStrats = NULL;

    /* initialize index-build state to default */
    n->ii_BrokenHotChain = false;
    n->ii_PgClassAttrId = 0;
    n->ii_ParallelWorkers = 0;

    return n;
}

static TupleDesc O_CreateTemplateTupleDesc(int natts, bool hasoid, const TableAmRoutine *tam_ops)
{
    TupleDesc desc;

    /*
     * sanity checks
     */
    AssertArg(natts >= 0);

    /*
     * Allocate enough memory for the tuple descriptor, including the
     * attribute rows.
     */
    desc = (TupleDesc)occ_malloc(offsetof(struct tupleDesc, attrs) +
                                 natts * sizeof(FormData_pg_attribute));

    /*
     * Initialize other fields of the tupdesc.
     */
    desc->natts = natts;
    desc->constr = NULL;
    desc->tdtypeid = RECORDOID;
    desc->tdtypmod = -1;
    desc->tdhasoid = hasoid;
    desc->tdrefcount = -1;    /* assume not reference-counted */
    desc->initdefvals = NULL; /* initialize the attrinitdefvals */
    desc->tdhasuids = false;
    desc->tdisredistable = false;
    desc->td_tam_ops = tam_ops;

    return desc;
}

static TupleDesc O_CreateTupleDescCopy(TupleDesc tupdesc)
{
    TupleDesc desc;
    int i;
    errno_t rc;

    desc = O_CreateTemplateTupleDesc(tupdesc->natts, tupdesc->tdhasoid, tupdesc->td_tam_ops);

    for (i = 0; i < desc->natts; i++)
    {
        Form_pg_attribute att = TupleDescAttr(desc, i);

        rc = memcpy_s(att, ATTRIBUTE_FIXED_PART_SIZE, &tupdesc->attrs[i], ATTRIBUTE_FIXED_PART_SIZE);
        securec_check(rc, "\0", "\0");
        att->attnotnull = false;
        att->atthasdef = false;
    }

    desc->tdtypeid = tupdesc->tdtypeid;
    desc->tdtypmod = tupdesc->tdtypmod;
    desc->tdisredistable = tupdesc->tdisredistable;
    desc->tdhasuids = tupdesc->tdhasuids;

    /* copy the attinitdefval */
    if (tupdesc->initdefvals)
    {
        desc->initdefvals = tupInitDefValCopy(tupdesc->initdefvals, tupdesc->natts);
    }

    return desc;
}

static void O_ExecSetSlotDescriptor(TupleTableSlot *slot, /* slot to change */
                                    TupleDesc tup_desc)   /* new tuple descriptor */
{
    /* For safety, make sure slot is empty before changing it */
    (void)ExecClearTuple(slot);

    /*
     * Release any old descriptor.  Also release old Datum/isnull arrays if
     * present (we don't bother to check if they could be re-used).
     */
    if (slot->tts_tupleDescriptor != NULL)
    {
        if (slot->tts_tupleDescriptor->initdefvals)
        {
            free(slot->tts_tupleDescriptor->initdefvals);
        }
        free(slot->tts_tupleDescriptor);
    }
#ifdef PGXC
    /* XXX there in no routine to release AttInMetadata instance */
    if (slot->tts_attinmeta != NULL)
    {
        slot->tts_attinmeta = NULL;
    }
#endif

    if (slot->tts_values != NULL)
    {
        free(slot->tts_values);
    }
    if (slot->tts_isnull != NULL)
    {
        free(slot->tts_isnull);
    }
    if (slot->tts_lobPointers != NULL)
    {
        free(slot->tts_lobPointers);
    }
    /*
     * Install the new descriptor
     */

    slot->tts_tupleDescriptor = O_CreateTupleDescCopy(tup_desc);

    /*
     * Allocate Datum/isnull arrays of the appropriate size.  These must have
     * the same lifetime as the slot, so allocate in the slot's own context.
     */
    slot->tts_values = (Datum *)occ_malloc(tup_desc->natts * sizeof(Datum));
    slot->tts_isnull = (bool *)occ_malloc(tup_desc->natts * sizeof(bool));
    slot->tts_lobPointers = (Datum *)occ_malloc(tup_desc->natts * sizeof(Datum));
}

static IndexInfo *O_BuildIndexInfo(Relation index)
{
    Form_pg_index indexStruct = index->rd_index;
    int i;
    int numAtts;

    /* check the number of keys, and copy attr numbers into the IndexInfo */
    numAtts = indexStruct->indnatts;
    if (numAtts < 1 || numAtts > INDEX_MAX_KEYS)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("invalid indnatts %d for index %u", numAtts, RelationGetRelid(index))));

    IndexInfo *ii = O_makeIndexInfo(indexStruct->indnatts,
                                    RelationGetIndexExpressions(index),
                                    RelationGetIndexPredicate(index),
                                    indexStruct->indisunique,
                                    IndexIsReady(indexStruct),
                                    false);

    ii->ii_NumIndexAttrs = numAtts;
    ii->ii_NumIndexKeyAttrs = IndexRelationGetNumberOfKeyAttributes(index);
    Assert(ii->ii_NumIndexKeyAttrs != 0);
    Assert(ii->ii_NumIndexKeyAttrs <= ii->ii_NumIndexAttrs);
    /* fill in attribute numbers */
    for (i = 0; i < numAtts; i++)
    {
        ii->ii_KeyAttrNumbers[i] = indexStruct->indkey.values[i];
    }

    /* fetch exclusion constraint info if any */
    if (indexStruct->indisexclusion)
    {
        RelationGetExclusionInfo(index, &ii->ii_ExclusionOps, &ii->ii_ExclusionProcs, &ii->ii_ExclusionStrats);
    }

    /* not doing speculative insertion here */
    ii->ii_UniqueOps = NULL;
    ii->ii_UniqueProcs = NULL;
    ii->ii_UniqueStrats = NULL;

    return ii;
}

static TupleTableSlot *O_MakeTupleTableSlot(bool has_tuple_mcxt, const TableAmRoutine *tam_ops)
{
    TupleTableSlot *slot = (TupleTableSlot *)occ_malloc(sizeof(TupleTableSlot));
    errno_t rc = memset_s(slot, sizeof(TupleTableSlot), 0, sizeof(TupleTableSlot));
    securec_check(rc, "\0", "\0");
    Assert(tam_ops == TableAmHeap || tam_ops == TableAmUstore);

    slot->tts_flags |= TTS_FLAG_EMPTY;
    slot->tts_tuple = NULL;
    slot->tts_tupleDescriptor = NULL;

    slot->tts_mcxt = CurrentMemoryContext;
    slot->tts_buffer = InvalidBuffer;
    slot->tts_nvalid = 0;
    slot->tts_values = NULL;
    slot->tts_isnull = NULL;
    slot->tts_mintuple = NULL;

    slot->tts_tam_ops = tam_ops;

    return slot;
}

static TupleTableSlot *O_MakeSingleTupleTableSlot(TupleDesc tup_desc, bool allocSlotCxt = false, const TableAmRoutine *tam_ops = TableAmHeap)
{
    TupleTableSlot *slot = O_MakeTupleTableSlot(allocSlotCxt, tam_ops);
    O_ExecSetSlotDescriptor(slot, tup_desc);
    return slot;
}

static FORCE_INLINE
    HeapTuple
    O_heap_copytuple(HeapTuple tuple)
{
    HeapTuple newTuple;
    errno_t rc = EOK;

    if (!HeapTupleIsValid(tuple) || tuple->t_data == NULL)
    {
        return NULL;
    }

    Assert(!HEAP_TUPLE_IS_COMPRESSED(tuple->t_data));

    newTuple = (HeapTuple)occ_malloc(HEAPTUPLESIZE + tuple->t_len);
    newTuple->tupTableType = HEAP_TUPLE;
    newTuple->t_len = tuple->t_len;
    newTuple->t_self = tuple->t_self;
    newTuple->t_tableOid = tuple->t_tableOid;
    newTuple->t_bucketId = tuple->t_bucketId;
    HeapTupleCopyBase(newTuple, tuple);

    newTuple->t_data = (HeapTupleHeader)((char *)newTuple + HEAPTUPLESIZE);
    rc = memcpy_s((char *)newTuple->t_data, tuple->t_len, (char *)tuple->t_data, tuple->t_len);
    securec_check(rc, "\0", "\0");
    return newTuple;
}

static TupleTableSlot *O_ExecCopySlot(TupleTableSlot *dst_slot, TupleTableSlot *src_slot)
{
    HeapTuple new_tuple;
    MemoryContext old_context;

    /*
     * There might be ways to optimize this when the source is virtual, but
     * for now just always build a physical copy.  Make sure it is in the
     * right context.
     */
    //    old_context = MemoryContextSwitchTo(dst_slot->tts_mcxt);
    new_tuple = O_heap_copytuple((HeapTuple)src_slot->tts_tuple);
    //    MemoryContextSwitchTo(old_context);

    return ExecStoreTuple(new_tuple, dst_slot, InvalidBuffer, true);
}

class O_Access
{
public:
    explicit O_Access()
    {
    }

    ~O_Access()
    {
        if (m_localRow != nullptr)
        {
            //        occ_free(access->m_localRow);
            m_localRow = nullptr;
        }

        if (m_localInsertRow != nullptr)
        {
            //        occ_free(access->m_localInsertRow);
            m_localInsertRow = nullptr;
        }

        if (m_globalRow != nullptr)
        {
            //        occ_free(access->m_globalRow);
            m_globalRow = nullptr;
        }

        if (m_ownsEstate && m_estate)
        {
            if (m_estate->es_result_relation_info)
            {
                if (m_estate->es_result_relation_info->ri_IndexRelationDescs)
                {
                    occ_free(m_estate->es_result_relation_info->ri_IndexRelationDescs);
                }
                if (m_estate->es_result_relation_info->ri_IndexRelationInfo)
                {
                    occ_free(m_estate->es_result_relation_info->ri_IndexRelationInfo);
                }
                occ_free(m_estate->es_result_relation_info);
            }
            occ_free(m_estate);
            m_estate = nullptr;
        }
        else
        {
            m_estate = nullptr;
        }

        if (m_ownsSlot && m_slot)
        {
            if (m_slot->tts_tupleDescriptor)
            {
                if (m_slot->tts_tupleDescriptor->initdefvals)
                {
                    occ_free(m_slot->tts_tupleDescriptor->initdefvals);
                }
                occ_free(m_slot->tts_tupleDescriptor);
            }
            if (m_slot->tts_values)
            {
                occ_free(m_slot->tts_values);
            }
            if (m_slot->tts_isnull)
            {
                occ_free(m_slot->tts_isnull);
            }
            if (m_slot->tts_lobPointers)
            {
                occ_free(m_slot->tts_lobPointers);
            }
            occ_free(m_slot);
            m_slot = nullptr;
        }
        else
        {
            m_slot = nullptr;
        }

        m_type = AccessType::INV;
        update_index = false;
        update_ins = false;
        old_version = false;
    }

    void WriteGlobalChanges(uint64_t csn);

    uint32_t GetStmtCount() const
    {
        return m_stmtCount;
    }

    inline HeapTuple GetTxnRow() const
    {
        if (m_type == RD or m_type == RD_FOR_UPDATE)
        {
            return m_globalRow;
        }
        if (m_type != INS)
        {
            return m_localRow;
        }
        else
        {
            return m_localInsertRow;
        }
    }

    inline HeapTuple GetGlobalVersion() const
    {
        return m_globalRow;
    }

    inline HeapTuple GetLocalVersion()
    {
        if (m_type != INS)
        {
            return m_localRow;
        }
        else
        {
            return m_localInsertRow;
        }
    }

    inline EState *GetEstate()
    {
        return m_estate;
    }

    inline void SetEstate(EState *estate)
    {
        /* 借用执行器 EState，避免每次 UPDATE 打开全部索引 */
        m_estate = estate;
        m_ownsEstate = false;
    }

    inline TupleTableSlot *GetSlot()
    {
        return m_slot;
    }

    inline void SetSlot(TupleTableSlot *slot, bool deepCopy = false)
    {
        if (deepCopy) {
            m_slot = O_MakeSingleTupleTableSlot(slot->tts_tupleDescriptor);
            m_slot = O_ExecCopySlot(m_slot, slot);
            m_ownsSlot = true;
        } else {
            m_slot = slot;
            m_ownsSlot = false;
        }
    }

    occ_sentinel m_origSentinel;

    /** @var The global visible version   */
    HeapTuple m_globalRow = nullptr;

    /** @var The inserted row for the INS operation */
    HeapTuple m_localInsertRow = nullptr;

    /** @var The modified draft row. */
    HeapTuple m_localRow = nullptr;

    EState *m_estate = nullptr;

    TupleTableSlot *m_slot = nullptr;

    bool m_ownsEstate = false;

    bool m_ownsSlot = false;

    uint32_t m_stmtCount = 0;

    uint64_t m_csn = 0;

    AccessType m_type = AccessType::INV;

    bool update_index = false;

    bool update_ins = false;

    bool old_version = false;
};
#endif // OPENGAUSS_O_ACCESS_H
