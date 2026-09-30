#include "postgres.h"

#include "epoch/e_pk_staging_shmem.h"

#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/spin.h"

#include "securec.h"
#include "securec_check.h"

#include <atomic>
#include <cstring>
#include <functional>

struct EpochPkEntry {
    uint8_t used;
    uint8_t deleted;
    uint16_t pk_len;
    Oid relid;
    BlockNumber block;
    OffsetNumber offset;
    char pk[EPOCH_PK_STAGING_MAX_PK];
};

struct EpochPkShardHeader {
    slock_t lock;
    uint32_t used_count;
};

struct EpochPkStagingHeader {
    uint32_t num_shards;
    uint32_t entries_per_shard;
    std::atomic<uint32_t> total_used;
};

struct EpochPkStagingShmem {
    EpochPkStagingHeader header;
    char blob[1];
};

static constexpr const char* EPOCH_PK_STAGING_SHMEM_NAME = "Epoch PK Staging Store";

static EpochPkStagingShmem* g_epochPkStaging = nullptr;

static uint32_t EpochPkEntriesPerShard(void)
{
    return (EPOCH_PK_STAGING_NUM_ENTRIES + EPOCH_PK_STAGING_NUM_SHARDS - 1U) / EPOCH_PK_STAGING_NUM_SHARDS +
           EPOCH_PK_STAGING_SKEW_SLACK;
}

static size_t EpochPkShardStride(uint32_t entries_per_shard)
{
    return sizeof(EpochPkShardHeader) + static_cast<size_t>(entries_per_shard) * sizeof(EpochPkEntry);
}

Size EpochPkStagingShmemSize(void)
{
    uint32_t eps = EpochPkEntriesPerShard();
    size_t shards = static_cast<size_t>(EPOCH_PK_STAGING_NUM_SHARDS) * EpochPkShardStride(eps);
    return sizeof(EpochPkStagingHeader) + shards;
}

static EpochPkShardHeader* EpochPkShardAt(uint32_t shard_idx)
{
    uint32_t eps = g_epochPkStaging->header.entries_per_shard;
    char* base = g_epochPkStaging->blob;
    return reinterpret_cast<EpochPkShardHeader*>(base + static_cast<size_t>(shard_idx) * EpochPkShardStride(eps));
}

static EpochPkEntry* EpochPkShardEntries(EpochPkShardHeader* shard)
{
    return reinterpret_cast<EpochPkEntry*>(shard + 1);
}

static size_t EpochPkKeyHash(Oid relid, const char* pk, uint16_t pk_len)
{
    size_t h = std::hash<Oid>{}(relid);
    for (uint16_t i = 0; i < pk_len; ++i) {
        h ^= static_cast<size_t>(static_cast<unsigned char>(pk[i])) + 0x9e3779b9 + (h << 6) + (h >> 2);
    }
    return h;
}

static uint32_t EpochPkShardIndex(Oid relid, const char* pk, uint16_t pk_len)
{
    return static_cast<uint32_t>(EpochPkKeyHash(relid, pk, pk_len) % g_epochPkStaging->header.num_shards);
}

static uint32_t EpochPkSlotStart(Oid relid, const char* pk, uint16_t pk_len, uint32_t eps)
{
    return static_cast<uint32_t>(EpochPkKeyHash(relid, pk, pk_len) % eps);
}

static bool EpochPkKeyEqual(const EpochPkEntry& e, Oid relid, const char* pk, uint16_t pk_len)
{
    return e.used != 0 && e.relid == relid && e.pk_len == pk_len &&
           (pk_len == 0 || memcmp(e.pk, pk, pk_len) == 0);
}

void EpochPkStagingShmemInit(void)
{
    bool found = false;
    g_epochPkStaging = static_cast<EpochPkStagingShmem*>(
        ShmemInitStruct(EPOCH_PK_STAGING_SHMEM_NAME, EpochPkStagingShmemSize(), &found));
    if (g_epochPkStaging == nullptr) {
        return;
    }
    if (found) {
        return;
    }

    uint32_t eps = EpochPkEntriesPerShard();
    g_epochPkStaging->header.num_shards = EPOCH_PK_STAGING_NUM_SHARDS;
    g_epochPkStaging->header.entries_per_shard = eps;
    g_epochPkStaging->header.total_used.store(0, std::memory_order_relaxed);

    for (uint32_t s = 0; s < g_epochPkStaging->header.num_shards; ++s) {
        EpochPkShardHeader* shard = EpochPkShardAt(s);
        SpinLockInit(&shard->lock);
        shard->used_count = 0;
        EpochPkEntry* entries = EpochPkShardEntries(shard);
        for (uint32_t i = 0; i < eps; ++i) {
            entries[i].used = 0;
            entries[i].deleted = 0;
            entries[i].pk_len = 0;
            entries[i].relid = InvalidOid;
            entries[i].block = 0;
            entries[i].offset = InvalidOffsetNumber;
        }
    }
}

static int EpochPkFindSlotLocked(EpochPkEntry* entries, uint32_t eps, Oid relid, const char* pk, uint16_t pk_len,
    bool* out_was_used)
{
    int empty = -1;
    uint32_t start = EpochPkSlotStart(relid, pk, pk_len, eps);
    for (uint32_t n = 0; n < eps; ++n) {
        uint32_t i = (start + n) % eps;
        /* 开放寻址：空槽之后不可能有本 key。未命中不要扫完整 shard。 */
        if (entries[i].used == 0) {
            empty = static_cast<int>(i);
            break;
        }
        if (EpochPkKeyEqual(entries[i], relid, pk, pk_len)) {
            if (out_was_used != nullptr) {
                *out_was_used = true;
            }
            return static_cast<int>(i);
        }
    }
    if (out_was_used != nullptr) {
        *out_was_used = false;
    }
    return empty;
}

bool EpochPkStagingUpsert(Oid relid, const char* pk, uint16_t pk_len, ItemPointer tid)
{
    if (g_epochPkStaging == nullptr || !OidIsValid(relid) || pk == nullptr || pk_len == 0 ||
        pk_len > EPOCH_PK_STAGING_MAX_PK || tid == nullptr || !ItemPointerIsValid(tid)) {
        return false;
    }

    uint32_t shard_idx = EpochPkShardIndex(relid, pk, pk_len);
    EpochPkShardHeader* shard = EpochPkShardAt(shard_idx);
    EpochPkEntry* entries = EpochPkShardEntries(shard);
    uint32_t eps = g_epochPkStaging->header.entries_per_shard;

    SpinLockAcquire(&shard->lock);
    bool was_used = false;
    int slot = EpochPkFindSlotLocked(entries, eps, relid, pk, pk_len, &was_used);
    if (slot < 0) {
        /* shard full: overwrite oldest probe start (best-effort eviction) */
        slot = static_cast<int>(EpochPkSlotStart(relid, pk, pk_len, eps));
        was_used = entries[slot].used != 0;
        if (was_used && shard->used_count > 0) {
            /* keep used_count; replace in place */
        } else if (!was_used) {
            /* empty after wrap weirdness */
        }
    }
    entries[slot].used = 1;
    entries[slot].deleted = 0;
    entries[slot].pk_len = pk_len;
    entries[slot].relid = relid;
    entries[slot].block = ItemPointerGetBlockNumber(tid);
    entries[slot].offset = ItemPointerGetOffsetNumber(tid);
    errno_t rc = memcpy_s(entries[slot].pk, EPOCH_PK_STAGING_MAX_PK, pk, pk_len);
    securec_check(rc, "\0", "\0");
    if (!was_used) {
        shard->used_count++;
        g_epochPkStaging->header.total_used.fetch_add(1, std::memory_order_relaxed);
    }
    SpinLockRelease(&shard->lock);
    return true;
}

bool EpochPkStagingMarkDeleted(Oid relid, const char* pk, uint16_t pk_len)
{
    if (g_epochPkStaging == nullptr || !OidIsValid(relid) || pk == nullptr || pk_len == 0 ||
        pk_len > EPOCH_PK_STAGING_MAX_PK) {
        return false;
    }
    uint32_t shard_idx = EpochPkShardIndex(relid, pk, pk_len);
    EpochPkShardHeader* shard = EpochPkShardAt(shard_idx);
    EpochPkEntry* entries = EpochPkShardEntries(shard);
    uint32_t eps = g_epochPkStaging->header.entries_per_shard;

    SpinLockAcquire(&shard->lock);
    bool was_used = false;
    int slot = EpochPkFindSlotLocked(entries, eps, relid, pk, pk_len, &was_used);
    if (slot < 0 || !was_used) {
        SpinLockRelease(&shard->lock);
        return false;
    }
    entries[slot].deleted = 1;
    SpinLockRelease(&shard->lock);
    return true;
}

bool EpochPkStagingLookup(Oid relid, const char* pk, uint16_t pk_len, ItemPointerData* out_tid, bool* deleted)
{
    if (out_tid != nullptr) {
        ItemPointerSetInvalid(out_tid);
    }
    if (deleted != nullptr) {
        *deleted = false;
    }
    if (g_epochPkStaging == nullptr || !OidIsValid(relid) || pk == nullptr || pk_len == 0 ||
        pk_len > EPOCH_PK_STAGING_MAX_PK) {
        return false;
    }

    uint32_t shard_idx = EpochPkShardIndex(relid, pk, pk_len);
    EpochPkShardHeader* shard = EpochPkShardAt(shard_idx);
    EpochPkEntry* entries = EpochPkShardEntries(shard);
    uint32_t eps = g_epochPkStaging->header.entries_per_shard;

    SpinLockAcquire(&shard->lock);
    uint32_t start = EpochPkSlotStart(relid, pk, pk_len, eps);
    for (uint32_t n = 0; n < eps; ++n) {
        uint32_t i = (start + n) % eps;
        if (entries[i].used == 0) {
            break;
        }
        if (!EpochPkKeyEqual(entries[i], relid, pk, pk_len)) {
            continue;
        }
        if (deleted != nullptr) {
            *deleted = entries[i].deleted != 0;
        }
        if (out_tid != nullptr) {
            ItemPointerSet(out_tid, entries[i].block, entries[i].offset);
        }
        SpinLockRelease(&shard->lock);
        return true;
    }
    SpinLockRelease(&shard->lock);
    return false;
}
