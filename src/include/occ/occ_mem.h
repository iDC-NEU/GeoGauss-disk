#ifndef OCC_MEM_H
#define OCC_MEM_H

#include <stdlib.h>
#include <new>

static inline void *occ_malloc(size_t size) { return malloc(size); }
static inline void occ_free(void *ptr) { free(ptr); }

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
