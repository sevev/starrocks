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

#include "storage/index/vector/vector_index_cache.h"

#include "common/logging.h"
#include "runtime/mem_tracker.h"

namespace starrocks {

VectorIndexCache::VectorIndexCache(size_t capacity, MemTracker* tracker) : _cache(capacity) {
    _cache.set_mem_tracker(tracker);
}

bool VectorIndexCache::Lookup(const tenann::CacheKey& key, tenann::IndexCacheHandle* handle) {
    _lookup_count.fetch_add(1, std::memory_order_relaxed);
    Entry* entry = _cache.get(key.to_string());
    if (entry == nullptr) return false;

    tenann::IndexRef ref;
    {
        auto g = entry->value().guard();
        if (!entry->value().has_ref()) {
            // Entry exists but is still being populated by another caller;
            // release the ref we just bumped and report miss.
            g.unlock();
            _cache.release(entry);
            return false;
        }
        ref = entry->value().ref();
    }
    _hit_count.fetch_add(1, std::memory_order_relaxed);
    *handle = _wrap(entry, std::move(ref));
    return true;
}

void VectorIndexCache::Insert(const tenann::CacheKey& key, tenann::IndexRef ref,
                              tenann::IndexCacheHandle* handle) {
    Entry* entry = _cache.get_or_create(key.to_string());
    tenann::IndexRef stored;
    {
        auto g = entry->value().guard();
        // Overwrite is allowed; the previous ref (if any) is kept alive by
        // outstanding IndexCacheHandles holding their own shared_ptr copies.
        entry->value().set_ref(ref);
        _cache.update_object_size(entry, entry->value().memory_usage());
        stored = entry->value().ref();
    }
    *handle = _wrap(entry, std::move(stored));
}

bool VectorIndexCache::GetOrCreate(const tenann::CacheKey& key, const IndexLoader& loader,
                                   tenann::IndexCacheHandle* handle) {
    // Follow the PrimaryIndex::prepare_primary_index pattern: get_or_create →
    // per-entry guard → load if needed → update_object_size. The guard holds
    // across loader() so concurrent callers on the same key single-flight.
    Entry* entry = _cache.get_or_create(key.to_string());
    bool ran = false;
    tenann::IndexRef ref;
    {
        auto g = entry->value().guard();
        if (!entry->value().has_ref()) {
            tenann::IndexRef loaded;
            try {
                loaded = loader();
            } catch (...) {
                g.unlock();
                _cache.release(entry);
                throw;
            }
            if (loaded == nullptr) {
                g.unlock();
                _cache.release(entry);
                LOG(ERROR) << "VectorIndexCache loader returned null IndexRef for key " << key.to_string();
                return false;
            }
            _loader_run_count.fetch_add(1, std::memory_order_relaxed);
            entry->value().set_ref(std::move(loaded));
            _cache.update_object_size(entry, entry->value().memory_usage());
            ran = true;
        }
        ref = entry->value().ref();
    }
    _lookup_count.fetch_add(1, std::memory_order_relaxed);
    if (!ran) _hit_count.fetch_add(1, std::memory_order_relaxed);
    *handle = _wrap(entry, std::move(ref));
    return !ran;
}

tenann::IndexCacheHandle VectorIndexCache::_wrap(Entry* entry, tenann::IndexRef ref) {
    Cache* cache = &_cache;
    return tenann::IndexCacheHandle(
            std::move(ref),
            std::shared_ptr<void>(entry, [cache](void* p) { cache->release(static_cast<Entry*>(p)); }));
}

} // namespace starrocks
