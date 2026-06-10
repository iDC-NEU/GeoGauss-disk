//
// Created by hcj2003 on 10/7/25.
//

#ifndef OPENGAUSS_O_SENTINEL_H
#define OPENGAUSS_O_SENTINEL_H

#include "utils/relcache.h"
#include "storage/item/itemptr.h"
#include "access/htup.h"
#include "utils/snapshot.h"
#include "utils/rel.h"

#include <atomic>
#include <vector>
#include <cstring>
#include <thread>
#include <functional>   // std::hash 定义在这里
//#include "occ/o_atomic_ops.h"

enum SentinelFlags : uint64_t {
    S_LOCK_BIT = 1UL << 62,   // Lock bit
    S_WRITE_BIT = 1UL << 61,  // Write bit
    S_LOCK_WRITE_BITS = (S_LOCK_BIT | S_WRITE_BIT),
};

struct alignas(64) LockSlot {
    std::atomic<uint64_t> v{0};
};

static inline uint64_t tid_hash(const ItemPointerData *tid)
{
    /* 原始 48 位信息 */
    uint64_t hi = uint64_t(tid->ip_blkid.bi_hi) << 32;
    uint64_t lo = uint64_t(tid->ip_blkid.bi_lo) << 16;
    uint64_t pos = tid->ip_posid;

    uint64_t x = hi | lo | pos;          // 48 位打包
    /* 64→64 雪崩混合（Murillo 常量） */
    x ^= x >> 32;
    x *= 0x4CD6944C5CC20B6DULL;
    x ^= x >> 32;
    x *= 0x4CD6944C5CC20B6DULL;
    x ^= x >> 32;
    return x;
}

static inline uint64_t sentinel_hash(Oid relid, const ItemPointerData* tid)
{
    uint64_t t = tid_hash(tid);
    uint64_t h = (uint64_t)relid;
    h ^= h >> 32;
    h *= 0x100000001b3ULL;
    uint64_t x = h ^ t;
    x ^= x >> 32;
    x ^= x >> 16;
    x ^= x >> 8;
    return x & 0xFFFFF;
}

static constexpr uint64_t LOCK_BIT = 1;
static constexpr uint64_t HOLDER_SHIFT = 1;           // bit1..20 持有者
static constexpr uint64_t HOLDER_BITS   = 20;
static constexpr uint64_t HOLDER_MASK  = 0xFFFFFULL << HOLDER_SHIFT; // 20 bit

/* 简单线程 id 哈希，这里用 std::hash 混合一下，防止平台差异 */
static inline uint64_t thread_hash()
{
    static thread_local uint64_t cached = 0;
    if (cached == 0) {
        std::hash<std::thread::id> h;
        cached = (h(std::this_thread::get_id()) & 0xFFFFF) + 1; // 保证非 0
    }
    return cached;
}

class LockTable {
public:
    explicit LockTable(unsigned bits)
        : mask_((1ull << bits) - 1), slots_(1ull << bits) {}
    LockSlot& slot(uint64_t hash) { return slots_[hash & mask_]; }
private:
    size_t mask_;
    std::vector<LockSlot> slots_;      // 对象数组，天然支持 .load()
};

inline LockTable& global_table() {
    static LockTable instance(20);   // 1 M 槽
    return instance;
}

class occ_sentinel {
public:
    occ_sentinel()
        : relation(nullptr), tuple_id(), snapshot(nullptr), relid(InvalidOid)
    {
    }

    occ_sentinel(Relation rel, ItemPointerData tid, Snapshot snap = nullptr)
        : relation(rel), tuple_id(tid), snapshot(snap),
          relid(rel != nullptr ? RelationGetRelid(rel) : InvalidOid)
    {
    }

    HeapTuple GetData(HeapTuple heap_tuple);
    HeapTuple GetData(HeapTuple heap_tuple, Buffer buffer);

    uint64_t GetStatus(HeapTuple heap_tuple)
    {
        return GetData(heap_tuple)->t_data->m_status;
//        return MOT_ATOMIC_LOAD(GetData()->t_data->m_status);
    }

    bool TryLock(uint64_t /*thdid*/) const
    {
        if (!OidIsValid(relid)) {
            return false;
        }
        uint64_t    h       = sentinel_hash(relid, &tuple_id);
        LockSlot&   s       = global_table().slot(h);

        uint64_t self_id = thread_hash();
        uint64_t old = s.v.load(std::memory_order_relaxed);

        if ((old & LOCK_BIT) == 0) {
            uint64_t newv = LOCK_BIT | (self_id << HOLDER_SHIFT);
            return s.v.compare_exchange_strong(old, newv,
                                               std::memory_order_acquire,
                                               std::memory_order_relaxed);
        }

        uint64_t holder = (old & HOLDER_MASK) >> HOLDER_SHIFT;
        return holder == self_id;
    }

    void Lock(uint64_t thdId) const
    {
        for (;;) {
            if (TryLock(thdId)) {
                return;
            }
            for (int i = 0; i < 64; ++i) {
                __builtin_ia32_pause();
            }
        }
    }

    inline void Unlock() const
    {
        if (!OidIsValid(relid)) {
            return;
        }
        uint64_t    h       = sentinel_hash(relid, &tuple_id);
        LockSlot&   s       = global_table().slot(h);

        uint64_t self_id = thread_hash();
        uint64_t old = s.v.load(std::memory_order_relaxed);

        /* 必须是自己加的锁 */
        uint64_t holder = (old & HOLDER_MASK) >> HOLDER_SHIFT;
        if ((old & LOCK_BIT) == 0 || holder != self_id) {
            /* 这里可以抛异常或者打日志，说明有 bug */
            return;
        }

        /* 清零即可：锁位 + 持有者一起清 */
        s.v.store(0, std::memory_order_release);
    }

    Relation relation;
    ItemPointerData tuple_id;
    Snapshot snapshot;
    Oid relid;

    bool operator<(const occ_sentinel& other) const {
        if (relid != other.relid) {
            return relid < other.relid;
        }
        if (tuple_id.ip_blkid.bi_hi != other.tuple_id.ip_blkid.bi_hi) {
            return tuple_id.ip_blkid.bi_hi < other.tuple_id.ip_blkid.bi_hi;
        }
        if (tuple_id.ip_blkid.bi_lo != other.tuple_id.ip_blkid.bi_lo) {
            return tuple_id.ip_blkid.bi_lo < other.tuple_id.ip_blkid.bi_lo;
        }
        return tuple_id.ip_posid < other.tuple_id.ip_posid;
    }

};

#endif  // OPENGAUSS_O_SENTINEL_H
