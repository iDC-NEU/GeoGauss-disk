#ifndef OCC_MEM_H
#define OCC_MEM_H

#include <stdlib.h>
#include <new>

#include "knl/knl_session.h"

static inline size_t O_OccAlignSize(size_t size)
{
    return (size + 7) & ~(size_t)7;
}

static inline void O_InitOccTxnArena(OccTxnArena *arena)
{
    if (arena == nullptr) {
        return;
    }
    arena->num_chunks = 0;
    arena->active_chunk = 0;
    arena->offset = 0;
}

static inline void O_EnsureOccTxnArenaChunk(OccTxnArena *arena)
{
    if (arena->num_chunks > 0) {
        return;
    }
    arena->chunks[0] = (char *)malloc(OCC_ARENA_CHUNK_SIZE);
    if (arena->chunks[0] == nullptr) {
        return;
    }
    arena->chunk_sizes[0] = OCC_ARENA_CHUNK_SIZE;
    arena->num_chunks = 1;
    arena->active_chunk = 0;
    arena->offset = 0;
}

static inline void *O_OccArenaAlloc(OccTxnArena *arena, size_t size)
{
    size = O_OccAlignSize(size);
    O_EnsureOccTxnArenaChunk(arena);
    if (arena->num_chunks == 0) {
        return malloc(size);
    }

    if (arena->offset + size <= arena->chunk_sizes[arena->active_chunk]) {
        void *ptr = arena->chunks[arena->active_chunk] + arena->offset;
        arena->offset += size;
        return ptr;
    }

    if (arena->num_chunks < OCC_ARENA_MAX_CHUNKS) {
        uint8_t idx = arena->num_chunks++;
        arena->chunks[idx] = (char *)malloc(OCC_ARENA_CHUNK_SIZE);
        if (arena->chunks[idx] == nullptr) {
            return malloc(size);
        }
        arena->chunk_sizes[idx] = OCC_ARENA_CHUNK_SIZE;
        arena->active_chunk = idx;
        arena->offset = size;
        return arena->chunks[idx];
    }

    return malloc(size);
}

/*
 * Bump-pointer arena: occ_malloc is O(1); O_ResetOccTxnMemory resets the bump
 * offset without per-object free.  occ_free is a no-op — never route OCC-owned
 * pointers through pfree/heap_freetuple (see O_ExecCopySlot should_free=false).
 */
static inline void O_ResetOccTxnMemory(void)
{
    if (u_sess == nullptr) {
        return;
    }
    u_sess->occ_cxt.txn_arena.active_chunk = 0;
    u_sess->occ_cxt.txn_arena.offset = 0;
}

static inline void *occ_session_malloc(size_t size)
{
    return malloc(size);
}

static inline void occ_session_free(void *ptr)
{
    if (ptr != nullptr) {
        free(ptr);
    }
}

static inline void *occ_malloc(size_t size)
{
    if (u_sess == nullptr) {
        return malloc(size);
    }
    return O_OccArenaAlloc(&u_sess->occ_cxt.txn_arena, size);
}

static inline void occ_free(void *ptr)
{
    (void)ptr;
}

template <typename T, typename... Args>
T *occ_new(Args &&...args)
{
    void *mem = occ_malloc(sizeof(T));
    return new (mem) T(std::forward<Args>(args)...);
}

template <typename T>
void occ_delete(T *ptr)
{
    if (ptr)
    {
        ptr->~T();
        occ_free(ptr);
    }
}

#endif // OCC_MEM_H
