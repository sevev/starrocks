// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <ostream>
#include <string>

#include "tenann/index/index_cache.h"
#include "util/dynamic_cache.h"

namespace starrocks {

class MemTracker;

// Value type held in the DynamicCache. Owns its own per-entry mutex so
// Lookup / Insert / GetOrCreate can serialize ref access (loads, reads, and
// overwrites) without an external striped-lock table.
//
// Contract: every read or write of `_ref` happens under guard(). After a
// successful load the entry may still be overwritten by a later Insert
// (tenann's Insert allows replacement); readers must therefore re-copy `_ref`
// each Lookup under the guard rather than cache the shared_ptr locally.
class VectorIndexCacheEntry {
public:
    std::unique_lock<std::mutex> guard() { return std::unique_lock<std::mutex>(_mu); }

    // All of the below must be called under guard().
    bool has_ref() const { return _ref != nullptr; }
    const tenann::IndexRef& ref() const { return _ref; }
    void set_ref(tenann::IndexRef ref) { _ref = std::move(ref); }
    size_t memory_usage() const { return _ref ? _ref->EstimateMemoryUsage() : 0; }

private:
    std::mutex _mu;
    tenann::IndexRef _ref;
};

// Minimal streaming operator required by DynamicCache's internal error logs.
// Intentionally opaque — those log paths fire only on ref-count bugs that
// should never trigger in correct use.
inline std::ostream& operator<<(std::ostream& os, const VectorIndexCacheEntry&) {
    return os << "VectorIndexCacheEntry";
}

// SR-owned implementation of tenann::IndexCache backed by DynamicCache — the
// standard CD primitive for ref-counted, mutable, pinned storage-path caches
// (see UpdateManager::prepare_primary_index).
//
// MemTracker is attached once via DynamicCache::set_mem_tracker; consume and
// release happen automatically on insert, update_object_size and evict. The
// tracker must outlive this cache; ExecEnv destructs the cache before tearing
// down the mem tracker hierarchy.
class VectorIndexCache final : public tenann::IndexCache {
public:
    using Cache = DynamicCache<std::string, VectorIndexCacheEntry>;
    using Entry = Cache::Entry;

    VectorIndexCache(size_t capacity, MemTracker* tracker);
    ~VectorIndexCache() override = default;

    VectorIndexCache(const VectorIndexCache&) = delete;
    VectorIndexCache& operator=(const VectorIndexCache&) = delete;

    // tenann::IndexCache — thin adapters around DynamicCache primitives.
    [[nodiscard]] bool Lookup(const tenann::CacheKey& key, tenann::IndexCacheHandle* handle) override;
    void Insert(const tenann::CacheKey& key, tenann::IndexRef ref, tenann::IndexCacheHandle* handle) override;
    [[nodiscard]] bool GetOrCreate(const tenann::CacheKey& key, const IndexLoader& loader,
                                   tenann::IndexCacheHandle* handle) override;

    // Management surface.
    void SetCapacity(size_t new_capacity) { _cache.set_capacity(new_capacity); }
    size_t capacity() const { return _cache.capacity(); }
    size_t memory_usage() const { return _cache.size(); }

    uint64_t lookup_count() const { return _lookup_count.load(std::memory_order_relaxed); }
    uint64_t hit_count() const { return _hit_count.load(std::memory_order_relaxed); }
    uint64_t loader_run_count() const { return _loader_run_count.load(std::memory_order_relaxed); }

private:
    // Wrap a pinned DynamicCache Entry* into a tenann::IndexCacheHandle. The
    // shared_ptr<void> releaser decrements the entry's refcount when the
    // handle is destroyed, matching tenann's RAII pin model.
    tenann::IndexCacheHandle _wrap(Entry* entry, tenann::IndexRef ref);

    Cache _cache;

    std::atomic<uint64_t> _lookup_count{0};
    std::atomic<uint64_t> _hit_count{0};
    std::atomic<uint64_t> _loader_run_count{0};
};

} // namespace starrocks
