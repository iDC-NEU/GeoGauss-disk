#ifndef E_CONCURRENT_MAP_H
#define E_CONCURRENT_MAP_H

/*
 * HCJ ADDBY: 参考 GeoGauss mot_internal.h 中 aum::concurrent_unordered_map
 * （997 分片 + 分片 mutex），为 epoch 模块提供无 MOT 依赖的并发 unordered_map。
 * 源路径: GeoGauss/src/gausskernel/storage/mot/fdw_adapter/src/mot_internal.h
 */

#include <cstdint>
#include <functional>
#include <mutex>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace EpochConcurrent {

template<typename Key, typename Value>
class UnorderedMap {
public:
    static constexpr uint64_t NUM_SHARDS = 997;

    void Clear(void)
    {
        for (uint64_t i = 0; i < NUM_SHARDS; ++i) {
            std::lock_guard<std::mutex> lock(shards_[i].mutex);
            shards_[i].map.clear();
        }
    }

    bool Find(const Key& key, Value* out) const
    {
        const Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.map.find(key);
        if (it == shard.map.end()) {
            return false;
        }
        if (out != nullptr) {
            *out = it->second;
        }
        return true;
    }

    Value Get(const Key& key, const Value& default_value = Value()) const
    {
        Value out = default_value;
        (void)Find(key, &out);
        return out;
    }

    void Insert(const Key& key, Value value)
    {
        Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        shard.map[key] = std::move(value);
    }

    template<typename Fn>
    void Modify(const Key& key, Fn&& fn)
    {
        Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.map.find(key);
        const bool is_new = (it == shard.map.end());
        Value& slot = is_new ? shard.map[key] : it->second;
        fn(slot, is_new);
    }

    Value& Ref(const Key& key)
    {
        Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        return shard.map[key];
    }

    bool Extract(const Key& key, Value* out)
    {
        Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.map.find(key);
        if (it == shard.map.end()) {
            return false;
        }
        if (out != nullptr) {
            *out = std::move(it->second);
        }
        shard.map.erase(it);
        return true;
    }

    bool Erase(const Key& key)
    {
        Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        return shard.map.erase(key) > 0;
    }

    /* 返回 map 内元素指针；锁释放后指针仍有效，直至 Erase(key)。 */
    Value* FindOrInsertPtr(const Key& key)
    {
        Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.map.find(key);
        if (it == shard.map.end()) {
            it = shard.map
                     .emplace(std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple())
                     .first;
        }
        return &it->second;
    }

    /* 不插入；锁释放后指针仍有效，直至 Erase(key)。 */
    Value* FindPtr(const Key& key)
    {
        Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.map.find(key);
        if (it == shard.map.end()) {
            return nullptr;
        }
        return &it->second;
    }

    const Value* FindPtr(const Key& key) const
    {
        const Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.map.find(key);
        if (it == shard.map.end()) {
            return nullptr;
        }
        return &it->second;
    }

    /* 持分片锁访问，不存在则返回 false（不插入）。 */
    template<typename Fn>
    bool Visit(const Key& key, Fn&& fn)
    {
        Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.map.find(key);
        if (it == shard.map.end()) {
            return false;
        }
        fn(it->second);
        return true;
    }

    template<typename Fn>
    bool Visit(const Key& key, Fn&& fn) const
    {
        const Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.map.find(key);
        if (it == shard.map.end()) {
            return false;
        }
        fn(it->second);
        return true;
    }

    template<typename Fn>
    void ForEach(Fn&& fn) const
    {
        for (uint64_t i = 0; i < NUM_SHARDS; ++i) {
            std::lock_guard<std::mutex> lock(shards_[i].mutex);
            for (const auto& kv : shards_[i].map) {
                fn(kv.first, kv.second);
            }
        }
    }

private:
    struct Shard {
        mutable std::mutex mutex;
        std::unordered_map<Key, Value> map;
    };

    Shard& ShardFor(const Key& key)
    {
        return shards_[ShardIndex(key)];
    }

    const Shard& ShardFor(const Key& key) const
    {
        return shards_[ShardIndex(key)];
    }

    uint64_t ShardIndex(const Key& key) const
    {
        return hasher_(key) % NUM_SHARDS;
    }

    std::hash<Key> hasher_;
    Shard shards_[NUM_SHARDS];
};

using U64Map = UnorderedMap<uint64_t, uint64_t>;
using U64SetMap = UnorderedMap<uint64_t, std::unordered_set<uint64_t>>;

inline uint64_t U64FetchAdd(U64Map& map, uint64_t key, uint64_t delta)
{
    uint64_t total = 0;
    map.Modify(key, [&](uint64_t& value, bool /*is_new*/) {
        value += delta;
        total = value;
    });
    return total;
}

inline uint64_t U64Get(const U64Map& map, uint64_t key, uint64_t default_value = 0)
{
    return map.Get(key, default_value);
}

inline bool U64SetInsert(U64SetMap& map, uint64_t epoch, uint64_t wire_key)
{
    bool inserted = false;
    map.Modify(epoch, [&](std::unordered_set<uint64_t>& keys, bool /*is_new*/) {
        inserted = keys.insert(wire_key).second;
    });
    return inserted;
}

inline bool U64SetContains(const U64SetMap& map, uint64_t epoch, uint64_t wire_key)
{
    bool found = false;
    map.Visit(epoch, [&](const std::unordered_set<uint64_t>& keys) {
        found = keys.count(wire_key) > 0;
    });
    return found;
}

inline bool U64SetEmpty(const U64SetMap& map, uint64_t epoch)
{
    bool empty = true;
    if (!map.Visit(epoch, [&](const std::unordered_set<uint64_t>& keys) {
            empty = keys.empty();
        })) {
        return true;
    }
    return empty;
}

inline void U64SetErase(U64SetMap& map, uint64_t epoch, uint64_t wire_key)
{
    bool erase_epoch = false;
    map.Modify(epoch, [&](std::unordered_set<uint64_t>& keys, bool is_new) {
        if (is_new) {
            return;
        }
        keys.erase(wire_key);
        erase_epoch = keys.empty();
    });
    if (erase_epoch) {
        map.Erase(epoch);
    }
}

} /* namespace EpochConcurrent */

#endif /* E_CONCURRENT_MAP_H */
