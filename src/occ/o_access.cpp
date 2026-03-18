//
// Created by hcj2003 on 10/8/25.
//

#include "occ/o_txn.h"
#include "occ/o_heapam.h"
#include "occ/o_executor.h"
#include "occ/o_delta_table.h"
#include "occ/occ_internal.h"

void O_Access::WriteGlobalChanges(uint64_t csn)
{
    if (m_type == RD)
    {
        return;
    }

    if (g_enable_occ_delta)
    {
        /* delta 覆盖模式：只写 delta 表，不直接触碰 heap page */
        O_DeltaTable &delta = O_DeltaTable::Instance();

        switch (m_type)
        {
        case WR:
        case INV:
        {
            HeapTuple newVersion = m_localRow;
            if (newVersion && newVersion->t_data)
            {
                newVersion->t_data->m_rowCSN = csn;
                newVersion->t_data->is_latest = true;
                delta.Upsert(m_origSentinel.relation, &m_origSentinel.tuple_id, newVersion, csn);
            }
            break;
        }
        case DEL:
            delta.MarkDeleted(m_origSentinel.relation, &m_origSentinel.tuple_id, csn);
            break;
        case INS:
        {
            HeapTuple insTuple = m_localInsertRow ? m_localInsertRow : m_localRow;
            if (insTuple && insTuple->t_data)
            {
                insTuple->t_data->m_rowCSN = csn;
                insTuple->t_data->is_latest = true;
                delta.Upsert(m_origSentinel.relation, &m_origSentinel.tuple_id, insTuple, csn);
            }
            break;
        }
        default:
            break;
        }
    }
    else
    {
        /* 原始 heap 写回路径：保持你之前的实现逻辑 */
        HeapTuple newVersion = nullptr;
        switch (m_type)
        {
        case WR:
        case INV:
            newVersion = m_localRow;
            if (update_index)
            {
                O_heap_update2(m_origSentinel.relation, &m_origSentinel.tuple_id, newVersion,
                               m_estate->es_output_cid, csn);
            }
            else
            {
                O_heap_update(m_origSentinel.relation, &m_origSentinel.tuple_id, newVersion,
                              m_estate->es_output_cid, csn, m_slot, m_estate);
            }
            break;
        case DEL:
            O_heap_delete(m_origSentinel.relation, &m_origSentinel.tuple_id, m_estate->es_output_cid, csn);
            break;
        case INS:
            if (update_ins)
            {
                if (update_index)
                {
                    O_heap_update2(m_origSentinel.relation, &m_origSentinel.tuple_id, m_localInsertRow,
                                   m_estate->es_output_cid, csn);
                }
                else
                {
                    O_heap_update(m_origSentinel.relation, &m_origSentinel.tuple_id, m_localInsertRow,
                                  m_estate->es_output_cid, csn, m_slot, m_estate);
                }
            }
            else
            {
                m_localInsertRow->t_data->m_rowCSN = csn;
            }
            break;
        default:
            break;
        }
    }
}
