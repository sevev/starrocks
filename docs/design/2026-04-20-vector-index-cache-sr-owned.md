# Vector Index Cache — SR-Owned Design

- **Date**: 2026-04-20
- **Status**: Design approved, pending implementation plan
- **Scope**: Replace tenann's internal `IndexCache` with a StarRocks-owned cache (`VectorIndexCache`) that owns capacity, eviction, and memory accounting. Reduce tenann to a pure algorithm library with zero runtime cache state.
- **Relationship to current branch**: `async_vector_index_build_v5_dev` holds the in-progress Morsel Dedup implementation (`VectorIndexLoader` + shared path). That code has **not** been merged. This design **replaces** it: `VectorIndexLoader` is never merged to main; PR-B directly implements the SR-owned cache path.
- **Related docs**: `docs/design/vector_index_morsel_dedup.md`

## 1. Background

The current Morsel Dedup PR introduces `VectorIndexLoader` as a thin wrapper over `tenann::IndexCache::GetOrCreate`, letting concurrent morsels share a loaded `.vi` file instead of each re-deserialising. It works, but leaves three problems with the memory accounting model:

1. **Cross-thread eviction free is mis-attributed.** tenann's `IndexCache` runs eviction deleters on whichever thread happens to trigger `Insert`. tcmalloc's hook reads `tls_mem_tracker` on that thread, so the released bytes get subtracted from an unrelated query's tracker, and `vector_index_mem_tracker` only grows — never shrinks.
2. **`SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER` only wraps the loader body.** Allocations that happen after load (faiss search-time scratch) and after eviction are not attributed.
3. **Two sources of truth for capacity / limit.** tenann's `IndexCache` holds one capacity; SR has a deprecated alias `vector_query_cache_capacity` plus the new `vector_index_cache_limit`. Logic to reconcile them is duplicated in `TenANNReader::init_searcher_shared` (per-query, hot path) and `UpdateConfigAction`.

Additionally, with tenann fully owned and statically linked into BE, the split "tenann owns a cache, SR queries it" is the wrong factoring: there is no scenario where tenann runs without SR.

## 2. Goals and Non-Goals

### Goals

- **G1** Accurate `vector_index_mem_tracker`: value reflects real bytes held by cached `IndexRef`s at all times; tracks correctly through eviction regardless of which thread triggers it.
- **G2** Unified cache management plane: one capacity config, one set of metrics, one admin API — all in SR.
- **G3** tenann becomes stateless w.r.t. caching: exposes an abstract `IndexCacheInterface`, holds no LRU / shard / capacity of its own.
- **G4** No changes to on-disk `.vi` format. v0.5.0 production indexes are readable by the new BE without any migration.

### Non-Goals

- Putting `.vi` raw bytes on datacache / disk tier. That is a separate read-through optimisation; this design operates only on the deserialised-object layer.
- Splitting the cache into two (HNSW whole-index vs IVF-PQ block): see §4 for why a single cache is correct.
- Hard memory limits / OOM enforcement driven from `vector_index_mem_tracker`. Current design reports; limiting is a follow-up.

## 3. Architecture

```
┌─── tenann (static lib, fully owned) ─────────────────────┐
│  tenann/index/index_cache_interface.h                    │
│    class IndexCacheInterface {                           │
│      virtual bool     Lookup(key, handle*)           = 0;│
│      virtual void     Insert(key, ref, handle*)      = 0;│
│      virtual bool     GetOrCreate(key, loader, h*)   = 0;│
│    };                                                    │
│    class IndexCacheHandle {                              │
│      IndexRef index_ref() const;                         │
│      // internally: IndexRef + shared_ptr<void> releaser │
│    };                                                    │
│    extern void                   SetGlobalIndexCache(…); │
│    extern IndexCacheInterface*   GetGlobalIndexCache();  │
│                                                          │
│  tenann internals (index_reader.cc, index_ivfpq_reader)  │
│    use GetGlobalIndexCache() — T_CHECK(cache != nullptr) │
│                                                          │
│  tenann has NO default production implementation.        │
│  test/util/fake_index_cache.h provides a minimal impl    │
│  used by tenann's own UT.                                │
└──────────────────────────────────────────────────────────┘
                              ▲  injected once at BE boot
┌─── StarRocks BE ─────────────────────────────────────────┐
│  be/src/storage/index/vector/vector_index_cache.{h,cpp}  │
│    class VectorIndexCache : tenann::IndexCacheInterface {│
│      // wraps starrocks::ShardedLRUCache                 │
│      // Insert/Evict callback → tracker Consume/Release  │
│      // (via SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER)     │
│      void   SetCapacity(size_t);                         │
│      size_t capacity() const;                            │
│      size_t memory_usage() const;                        │
│      uint64_t lookup_count / hit_count / loader_runs;    │
│    };                                                    │
│                                                          │
│  ExecEnv::init():                                        │
│    _vector_index_cache = std::make_unique<…>(cap, tkr);  │
│    tenann::SetGlobalIndexCache(_vector_index_cache.get())│
│                                                          │
│  TenANNReader::init_searcher(meta, path, fs):            │
│    cache = GetGlobalIndexCache();                        │
│    cache->GetOrCreate(path, loader, &_cache_handle);     │
│    _searcher = CreateSearcherFromMeta(meta);             │
│    _searcher->AttachIndex(_cache_handle.index_ref());    │
└──────────────────────────────────────────────────────────┘
```

## 4. Cache Entries: HNSW whole-index vs IVF-PQ block

Both entry types live in the same `VectorIndexCache` instance. A single LRU over a heterogeneous entry set is deliberate: it lets the LRU rebalance memory between the two entry types based on actual access pattern.

|                           | HNSW whole-index                                                                  | IVF-PQ block_cache_list                                                          |
|---------------------------|-----------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| Entries per segment       | 1                                                                                 | N (= nlist, typically 256–4096)                                                  |
| Entry size                | GB scale; full HNSW graph + raw vectors                                           | MB scale; per-list PQ codes + ids                                                |
| Cache key                 | `.vi` file path (`{rowset_dir}/{rowset_id}_{seg_id}_{index_id}.vi`) — path already encodes rowset+segment+index uniquely, so no suffix needed | `hash(filename) + "_" + mtime + "_" + list_id` (tenann internal) |
| Value (via `IndexRef`)    | `tenann::Index(faiss::Index*, kFaissHnsw, deleter=delete)`                        | `tenann::Index(void* bytes, kFaissIvfPqOneInvertedList, deleter=free, size=N)`   |
| Charge (bytes)            | `Index::EstimateMemoryUsage()` — walks faiss structures                            | `Index::EstimateMemoryUsage()` — returns `explicit_bytes` set at construction    |
| Inserted by               | `GetOrCreate` loader body (HNSW `ReadIndex` on cache miss)                         | `BlockCacheInvertedLists::get_ptr` on list probe                                 |
| Pin holder                | `TenANNReader::_cache_handle` (reader lifetime)                                   | `BlockCacheInvertedLists::cache_handles[list_no]` (per-list, short-lived)        |
| Eviction probability      | Low (pinned by reader)                                                            | High (short pin, naturally LRU-eligible)                                         |
| Single eviction cost      | Tens to hundreds of ms (GB free)                                                   | µs                                                                                |

### Unified insert/deleter path

Both entry types go through the same `VectorIndexCache::Insert` code:

```cpp
struct Holder { tenann::IndexRef ref; };

cache_->insert(
    key,
    new Holder{ref},
    /*charge=*/ref->EstimateMemoryUsage(),
    /*deleter=*/[tracker = tracker_](void* v, size_t) {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(tracker);
        delete static_cast<Holder*>(v);  // → ~Index() → type-specific raw-data deleter
    });
```

The LRU deleter is identical for both types. Actual resource release happens inside `~tenann::Index()` using the deleter the `Index` was constructed with (`delete` for HNSW faiss objects, `free` for IVF-PQ block buffers). `VectorIndexCache` itself is agnostic about what it's caching.

### Why a single cache

Alternatives considered:

- **Two separate caches with separate capacities.** Rejected: operators would need to predict the HNSW/IVF split ratio, and a single LRU naturally allocates capacity between types based on access frequency. Doubles admin surface (configs, metrics, endpoints) for no gain.
- **Tiered cache (memory for deserialised objects, disk for raw bytes).** Rejected: raw `.vi` bytes and deserialised faiss objects are not 1:1 (serialised form is compact, in-memory form expands by M·dim·8). Raw-byte caching is a separate read-through concern.

## 5. Memory Accounting Model

`vector_index_mem_tracker` is a **child of `process_mem_tracker`** (standard hierarchical tracker), `limit = -1`.

### Load path (SCOPED setter in loader body)

```cpp
auto loader = [&, tracker]() -> tenann::IndexRef {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(tracker);  // vector_index_mem_tracker
    auto s = tenann::AnnSearcherFactory::CreateSearcherFromMeta(meta_copy);
    s->index_reader()->SetIndexCache(cache);
    if (fs != nullptr) {
        ASSIGN_OR_RETURN_TENANN(auto fr, VectorIndexFileReader::open(fs, path));
        s->ReadIndex(fr);
    } else {
        s->ReadIndex(path);
    }
    return s->index_ref();
};
```

All allocations inside `ReadIndex` flow through tcmalloc hook → `tls_mem_tracker` (= `vector_index_mem_tracker`) → propagates up to `process_mem_tracker`. Single-entry accounting, parent aggregation is correct.

### Evict path (SCOPED setter in deleter)

The LRU deleter (registered at Insert time) captures the tracker pointer and swaps `tls_mem_tracker` before freeing the holder:

```cpp
auto deleter = [tracker](void* v, size_t) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(tracker);
    delete static_cast<Holder*>(v);
    // ~Index() → type-specific free → tcmalloc hook → tls_tracker (=vector_index)
};
```

Whichever thread triggers eviction (another `Insert`, `SetCapacity` shrink, BE shutdown), the deleter temporarily sets tls to `vector_index_mem_tracker`, freeing bytes attribute correctly. The hosting thread's original tracker is restored via RAII on deleter return.

### Why this is correct

| Property                         | Guarantee                                                                 |
|----------------------------------|---------------------------------------------------------------------------|
| No double-counting               | Allocations go through tcmalloc hook exactly once; Consume/Release on vector_index_tracker only happens via hook → parent propagation, never via explicit call |
| `process_mem_tracker` accuracy   | All allocations propagate up, regardless of which leaf tracker was active |
| Cross-thread eviction correctness| Deleter's SCOPED setter decouples attribution from the hosting thread     |
| VI tracker can be queried for "how much is cache holding" | Direct tracker consumption read                          |

### Contrast with current Morsel Dedup code

| Scenario                             | Current (loader + tenann cache) | New (SR cache + deleter SCOPED)                 |
|--------------------------------------|---------------------------------|-------------------------------------------------|
| Load-time allocations                | Scoped → vector_index tracker   | Same                                            |
| Post-AttachIndex query-time scratch  | Query tracker (correct)         | Same                                            |
| Eviction free on random thread       | **Mis-attributed** to that thread's tracker | Scoped → vector_index tracker           |
| vector_index tracker usefulness       | Grows monotonically, misleading | Accurate, can be gated by a future subsystem limit |

## 6. Interface Specification

### tenann/index/index_cache_interface.h (new)

```cpp
namespace tenann {

class IndexCacheHandle;

class IndexCacheInterface {
public:
    using IndexLoader = std::function<IndexRef()>;

    virtual ~IndexCacheInterface() = default;

    // Lookup key. If hit, fills handle with a pinned reference. Returns true on hit.
    virtual bool Lookup(const CacheKey& key, IndexCacheHandle* handle) = 0;

    // Insert ref into the cache. The cache calls ref->EstimateMemoryUsage() to
    // determine charge. Fills handle with a pinned reference to the new entry.
    virtual void Insert(const CacheKey& key, IndexRef ref, IndexCacheHandle* handle) = 0;

    // Atomic get-or-create. Concurrent calls for the same key run loader at most
    // once (single-flight). On loader success, the entry is inserted; on loader
    // exception, nothing is cached and the exception propagates to the caller
    // currently running loader. Callers blocked on the per-key lock retry.
    // Returns true if fast-path hit, false if loader ran.
    virtual bool GetOrCreate(const CacheKey& key, const IndexLoader& loader,
                             IndexCacheHandle* handle) = 0;
};

class IndexCacheHandle {
public:
    IndexCacheHandle() = default;
    IndexCacheHandle(IndexRef ref, std::shared_ptr<void> releaser)
        : ref_(std::move(ref)), releaser_(std::move(releaser)) {}

    IndexCacheHandle(IndexCacheHandle&&) = default;
    IndexCacheHandle& operator=(IndexCacheHandle&&) = default;
    IndexCacheHandle(const IndexCacheHandle&) = delete;
    IndexCacheHandle& operator=(const IndexCacheHandle&) = delete;

    IndexRef index_ref() const { return ref_; }
    bool valid() const { return ref_ != nullptr; }

private:
    IndexRef ref_;
    std::shared_ptr<void> releaser_;  // destructor triggers Cache::Release
};

// Global injection point — BE calls SetGlobalIndexCache once at ExecEnv::init.
// tenann internals call GetGlobalIndexCache(); T_CHECK for nullptr at use site.
void SetGlobalIndexCache(IndexCacheInterface* cache);
IndexCacheInterface* GetGlobalIndexCache();

} // namespace tenann
```

### tenann/index/index.h (modified)

```cpp
class Index {
public:
    // Existing ctor unchanged for HNSW/IVFPQ whole-index (computed via EstimateMemoryUsage).
    // Block entries pass explicit_bytes = malloc size.
    Index(void* raw, IndexType type, Deleter d, size_t explicit_bytes = 0);

    size_t EstimateMemoryUsage() {
        if (explicit_bytes_ > 0) return explicit_bytes_;  // block entries
        switch (index_type_) {
            case kFaissHnsw:  return compute_hnsw_bytes();
            case kFaissIvfPq: return compute_ivfpq_bytes();
            default:          return 1;  // unchanged fallback
        }
    }

private:
    size_t explicit_bytes_ = 0;
};
```

Block construction point (`index_ivfpq_reader.cc`) passes `size` to ctor — one line.

### tenann deletions

- `tenann/index/index_cache.{h,cc}` — **deleted**
- `tenann/test/index/test_index_cache.cc` — **deleted**
- `tenann/examples/index_cache_example.cc` — **deleted** or rewritten against `FakeIndexCache`

### tenann/test/util/fake_index_cache.h (new)

Minimal `IndexCacheInterface` implementation for tenann's own UT: `std::unordered_map<string, IndexRef> + std::mutex`. No LRU, no charge, no shard. ~80 lines.

### be/src/storage/index/vector/vector_index_cache.h (new, SR)

```cpp
namespace starrocks {

class MemTracker;

class VectorIndexCache final : public tenann::IndexCacheInterface {
public:
    VectorIndexCache(size_t capacity, MemTracker* tracker);
    ~VectorIndexCache() override;

    // IndexCacheInterface
    bool Lookup(const tenann::CacheKey& key, tenann::IndexCacheHandle* handle) override;
    void Insert(const tenann::CacheKey& key, tenann::IndexRef ref, tenann::IndexCacheHandle* handle) override;
    bool GetOrCreate(const tenann::CacheKey& key, const IndexLoader& loader,
                     tenann::IndexCacheHandle* handle) override;

    // Management (BE-specific; not on interface)
    void SetCapacity(size_t new_capacity);
    size_t capacity() const;
    size_t memory_usage() const;
    uint64_t lookup_count() const;
    uint64_t hit_count() const;
    uint64_t loader_run_count() const;

private:
    std::unique_ptr<starrocks::Cache> lru_;
    MemTracker* tracker_;  // not owned

    std::mutex load_locks_mu_;
    std::unordered_map<std::string, std::weak_ptr<std::mutex>> load_locks_;

    std::atomic<uint64_t> lookup_count_{0};
    std::atomic<uint64_t> hit_count_{0};
    std::atomic<uint64_t> loader_run_count_{0};

    std::shared_ptr<std::mutex> get_or_create_load_lock(const std::string& key);
};

} // namespace starrocks
```

### ExecEnv wiring

```cpp
// exec_env.h: add member
std::unique_ptr<VectorIndexCache> _vector_index_cache;
VectorIndexCache* vector_index_cache() { return _vector_index_cache.get(); }

// exec_env.cpp, GlobalEnv::_init_mem_tracker or early init:
int64_t capacity = config::vector_index_cache_limit;
if (capacity <= 0) capacity = config::vector_query_cache_capacity;  // one-shot warn
LOG(INFO) << "Vector index cache capacity = " << capacity << " bytes";
_vector_index_cache = std::make_unique<VectorIndexCache>(capacity, _vector_index_mem_tracker.get());
tenann::SetGlobalIndexCache(_vector_index_cache.get());
```

### TenANNReader (simplified)

`init_searcher` / `init_searcher(with fs)` / `init_searcher_shared` → a single `init_searcher(meta, path, fs)`. `_shared_handle` / `_index_id` members deleted. `enable_shared_vector_index` config deleted. `SetCapacity` calls removed from per-query path.

```cpp
Status TenANNReader::init_searcher(const tenann::IndexMeta& meta, const std::string& path, FileSystem* fs) {
    auto meta_copy = meta;
    apply_index_reader_cache_options(&meta_copy);

    auto* cache = tenann::GetGlobalIndexCache();
    T_CHECK(cache != nullptr);

    auto* tracker = GlobalEnv::GetInstance()->vector_index_mem_tracker();
    auto loader = [&, tracker]() -> tenann::IndexRef {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(tracker);
        auto s = tenann::AnnSearcherFactory::CreateSearcherFromMeta(meta_copy);
        s->index_reader()->SetIndexCache(cache);
        if (fs != nullptr) {
            ASSIGN_OR_RETURN_TENANN(auto fr, VectorIndexFileReader::open(fs, path));
            s->ReadIndex(fr);
        } else {
            s->ReadIndex(path);
        }
        return s->index_ref();
    };

    try {
        cache->GetOrCreate(path, loader, &_cache_handle);
    } catch (tenann::Error& e) {
        return tenann_error_to_status(e);
    }

    _searcher = tenann::AnnSearcherFactory::CreateSearcherFromMeta(meta_copy);
    _searcher->index_reader()->SetIndexCache(cache);
    _searcher->AttachIndex(_cache_handle.index_ref());
    _searcher->SetSearchParams(meta_copy.search_params());
    return Status::OK();
}
```

### Config changes

| Config                              | Action                                                  |
|-------------------------------------|--------------------------------------------------------|
| `vector_index_cache_limit`          | **Kept.** Drives VectorIndexCache capacity.            |
| `vector_query_cache_capacity`       | **Deprecated** (kept as alias for one release, WARN on use) |
| `enable_shared_vector_index`        | **Deleted.** Cache is always active; capacity=0 disables. |
| `enable_vector_index_block_cache`   | **Kept** (IVF-PQ behavior switch, independent).        |

### update_config_action

Simplified to a single callback on `vector_index_cache_limit`:

```cpp
_config_callback.emplace("vector_index_cache_limit", [&]() -> Status {
    int64_t limit = config::vector_index_cache_limit;
    if (limit > 0 && _exec_env->vector_index_cache() != nullptr) {
        _exec_env->vector_index_cache()->SetCapacity(static_cast<size_t>(limit));
    }
    return Status::OK();
});
```

No per-query lazy-init. No fallback branching in hot paths.

## 7. Lifecycle and Error Handling

### Load sequence (cache miss)

1. `TenANNReader::init_searcher(meta, path, fs)` called from `SegmentIterator`.
2. `cache->GetOrCreate(path, loader, &_cache_handle)` acquires per-key mutex (single-flight).
3. loader runs: SCOPED setter swaps tls → `vector_index_mem_tracker`; `ReadIndex` allocates via tcmalloc hook → tracker propagates to `process_mem_tracker`.
4. loader returns `IndexRef`; cache inserts into LRU, charge = `ref->EstimateMemoryUsage()`, deleter captures tracker.
5. Concurrent blocked callers wake, fast-path Lookup returns the same `IndexRef`.
6. `_searcher = CreateSearcherFromMeta(meta); _searcher->AttachIndex(ref)` — zero I/O.

### Load sequence (cache hit)

Steps 2 → 6 skip loader entirely. Fast-path returns within microseconds.

### Missing .vi file

`VectorIndexFileReader::open(fs, path)` returns `Status::NotFound` → loader wraps into `tenann::Error` → propagates out of `GetOrCreate` → `tenann_error_to_status` maps back to `Status::NotFound` → SegmentIterator enters brute-force fallback (existing code path, unchanged).

### Loader exception

Entry **not** cached. Next caller for the same key re-runs loader. Other callers blocked on the per-key lock also re-run. Per tenann's current semantics; preserved here.

### Eviction

`ShardedLRUCache::insert` (invoked internally by `Insert`) evicts LRU victims when `total_charge > capacity`. Each evicted entry's deleter runs: SCOPED tracker swap → `delete Holder` → `~Index()` → type-specific free. Bytes correctly attribute to `vector_index_mem_tracker` and propagate down from `process_mem_tracker`.

### Capacity change at runtime

`vector_index_cache_limit` config update → `UpdateConfigAction` callback → `VectorIndexCache::SetCapacity(new)` → `ShardedLRUCache::set_capacity` → synchronous eviction to new limit. Deleters fire normally.

### BE shutdown

`ExecEnv::_vector_index_cache` unique_ptr destructor → `~VectorIndexCache` → LRU destructor → all remaining entries' deleters fire. Deleters use the cached tracker pointer (captured at Insert time); tracker must outlive the cache. `ExecEnv` destruction order: cache destructed **before** the mem tracker map, so this holds.

## 8. Testing

### VectorIndexCache unit tests (new, `be/test/storage/index/vector/vector_index_cache_test.cpp`)

| Test                                            | Verifies                                                                 |
|-------------------------------------------------|--------------------------------------------------------------------------|
| `Lookup_Miss_ReturnsFalse`                      | Empty cache → Lookup returns false, handle remains invalid               |
| `Insert_ThenLookup_ReturnsSameRef`              | Insert and Lookup return identical `IndexRef.get()`                      |
| `GetOrCreate_FirstCallRunsLoader`               | Cache miss → loader executes once; handle has loader's ref               |
| `GetOrCreate_SecondCallHitsCache`               | Second call skips loader; two refs point to same instance                |
| `GetOrCreate_ConcurrentCallers_SingleFlight`    | 16 threads same key → `loader_run_count == 1`; all handles share ref     |
| `GetOrCreate_LoaderThrows_NotCached`            | Loader exception propagates; entry absent; retry re-runs loader          |
| `Insert_OverCapacity_EvictsLRU`                 | capacity=100B, insert 200B → oldest entry evicted; deleter called        |
| `Evict_WhilePinned_DeferredRelease`             | Pinned entry not destructed until handle drops                           |
| `SetCapacity_Shrink_EvictsImmediately`          | 100MB → 10MB shrink triggers synchronous eviction                        |
| `MemTracker_Consume_AfterInsert`                | Insert raises `vector_index_tracker.consumption()`                       |
| `MemTracker_Release_AfterEvict`                 | Evict returns tracker consumption to baseline                            |
| `MemTracker_CrossThread_EvictReleasesCorrectly` | Thread A inserts, thread B triggers evict (via SetCapacity); tracker deltas verified — confirms deleter SCOPED setter works across threads |

`MemTracker_CrossThread_EvictReleasesCorrectly` is the headline correctness test for this design.

### tenann test migration

- `tenann/test/util/fake_index_cache.h` — new minimal impl.
- `tenann/test/index/test_index_cache.cc` — delete; behavioral tests (single-flight, loader exception) migrate to `VectorIndexCache` unit tests.
- `tenann/test/searcher/test_*.cc` — each test fixture's `SetUp()` calls `tenann::SetGlobalIndexCache(&fake_cache_)`.

### Integration regressions (SR)

- `be/test/storage/index/vector_search_test.cpp` and `vector_index_test.cpp` — unchanged call patterns; verify cache-hit path returns byte-identical search results.
- Missing `.vi` fallback path preserved via existing brute-force fallback tests.
- Delete `be/test/storage/lake/vector_index_loader_test.cpp`; migrate its three cases (first-load, concurrent-share, missing-file) into `vector_index_cache_test.cpp` and `vector_search_test.cpp`.

### Manual / end-to-end

- HNSW 1M-vector index, multi-morsel query: cache hit rate after first query = 100% for subsequent morsels; concurrent-morsel latency ≈ current Morsel Dedup PR.
- IVF-PQ with nlist=1024: observe `memory_usage()` grows as `nprobe` blocks are touched; capacity-induced eviction of cold blocks works.
- Rolling upgrade from v0.5.0 tenann (existing `.vi` files on OSS) to new BE: no recall / latency regression.

### Out of scope (this PR)

- Hard-limit enforcement tests (tracker `limit` stays `-1` this round).
- block-cache IOHook refactor tests — tenann's `BlockCacheInvertedLists` path is untouched.

## 9. Rollout — PR Breakdown

Two PRs across two repos, merged sequentially (tenann is statically linked into BE; no independent release).

### PR-A (tenann)

| File                                             | Change                                                 | LOC (net) |
|--------------------------------------------------|--------------------------------------------------------|-----------|
| `tenann/index/index_cache_interface.h`           | New                                                    | +60       |
| `tenann/index/index_cache.{h,cc}`                | Delete                                                 | -400      |
| `tenann/index/index.{h,cc}`                      | Add `explicit_bytes` ctor param + block-case accounting | +15      |
| `tenann/index/index_ivfpq_reader.cc`             | Pass size to `tenann::Index` ctor; switch to `IndexCacheInterface*` | +10 / -5 |
| `tenann/index/index_reader.{h,cc}`               | Type migration to `IndexCacheInterface*`                | +5 / -5  |
| `tenann/searcher/*.{h,cc}`                       | Type migration                                          | +10 / -10|
| `tenann/test/util/fake_index_cache.h`            | New                                                     | +80      |
| `tenann/test/index/test_index_cache.cc`          | Delete                                                  | -300     |
| `tenann/test/searcher/test_*.cc`                 | SetUp injects `FakeIndexCache`                          | +50      |
| `tenann/examples/index_cache_example.cc`         | Delete or rewrite                                       | -20      |

Net: ~+230 / -740 ≈ removes ~500 LOC. Standalone tenann UT suite green.

### PR-B (StarRocks)

| File                                                          | Change                                  | LOC (net)   |
|---------------------------------------------------------------|-----------------------------------------|-------------|
| `be/src/storage/index/vector/vector_index_cache.{h,cpp}`      | New                                     | +250        |
| `be/test/storage/index/vector/vector_index_cache_test.cpp`    | New                                     | +300        |
| `be/src/runtime/exec_env.{h,cpp}`                             | Cache member + `SetGlobalIndexCache`    | +10         |
| `be/src/storage/index/vector/tenann_index_reader.{h,cpp}`     | Consolidate three init paths; drop members | +50 / -120 |
| `be/src/storage/index/vector/vector_index_reader_factory.{h,cpp}` | Drop `index_id` overload            | -25         |
| `be/src/storage/index/vector/tenann/tenann_index_utils.{h,cpp}` | `tenann_error_to_status` helper        | +15         |
| `be/src/storage/index/vector/vector_index_file_reader.{h,cpp}` | `static StatusOr<…> open(fs, path)`    | +20         |
| `be/src/storage/rowset/segment_iterator.cpp`                   | Drop `index_id` argument                | -2          |
| `be/src/common/config*`                                        | Remove `enable_shared_vector_index`     | +2 / -15    |
| `be/src/http/action/update_config_action.cpp`                  | Simplified capacity callback            | +10 / -20   |
| `be/src/storage/lake/vector_index_loader.{h,cpp,_test.cpp}`    | Delete                                  | -300        |
| `be/src/storage/CMakeLists.txt`, `be/test/CMakeLists.txt`      | Source list updates                     | ±2          |
| `docs/en/**`, `docs/zh/**`                                     | Param doc updates                       | +30 / -30   |

Net: ~+690 / -550. Business-logic code actually shrinks (300 LOC loader gone).

### Merge order

1. PR-A lands in tenann master.
2. SR updates tenann pin (thirdparty or submodule bump).
3. PR-B lands in SR; passes CI with new tenann.

Parallel development allowed; strict sequential merge. No feature-flag or dual-path bridge needed (tenann is statically linked, no wire-compat concern).

## 10. Effort Estimate

| Phase                                                | Estimate         |
|------------------------------------------------------|------------------|
| PR-A implementation + tenann UT green                | 1 – 1.5 days     |
| PR-B implementation + SR unit + integration tests    | 2 – 3 days       |
| Manual regression: HNSW 1M / IVF-PQ 1M / multi-morsel | 1 day            |
| Review / rebase / CI                                 | 1 – 2 days       |
| **Total**                                            | **5 – 8 days**   |

## 11. Risks and Mitigations

| Risk                                                                          | Mitigation                                                                                                       |
|-------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| Charge semantics drift for IVF-PQ block entries                               | Block entries use `explicit_bytes = read_bytes`, exactly matching current tenann `[read_bytes](){}` callback. Numeric output unchanged. |
| tenann pin bump missed in SR                                                  | PR-B CI fails to compile; caught before merge.                                                                    |
| BE forgets `SetGlobalIndexCache` after tenann upgrade                         | tenann internal `T_CHECK(GetGlobalIndexCache() != nullptr)` at use site → fail-fast at first `.vi` read.          |
| Existing production `.vi` files unreadable                                     | On-disk format untouched; deserialisation path is bit-identical to current tenann logic.                          |
| `vector_index_mem_tracker` parent destruction ordering bug                     | Documented in §7; ExecEnv destruction order ensures cache dies before tracker map. Covered by `MemTracker_CrossThread_*` unit test. |

## 12. Follow-ups (Not in scope)

- Add hard-limit enforcement on `vector_index_mem_tracker.limit` to bound VI subsystem memory.
- Read-through cache for `.vi` raw bytes in datacache (disk tier) — independent optimisation.
- Per-tablet cache metrics for operator-level observability (current metrics are global per cache).
- Evict-on-table-drop: explicit invalidation of entries for dropped tablets so LRU doesn't waste cycles caching dead segments.

## 13. Review Items Resolved

From `project_vi_cache_review_findings.md` (18 items), this design resolves 16 in one shot:

- #1 hot-path SetCapacity — removed (ExecEnv init + update_config callback only)
- #2 factory `index_id=0` collision — `index_id` parameter deleted entirely
- #4 `error_to_status` matches `e.what()` — `tenann_error_to_status` uses `e.message()`
- #5 `new_random_access_file + get_size + VectorIndexFileReader` triplet — `VectorIndexFileReader::open` static factory
- #6 cache-limit fallback duplication — single `ExecEnv::init` path
- #7 `init_searcher` 4-way preamble duplication — consolidated into one `init_searcher`
- #9 `VectorIndexLoader` wrong directory — deleted entirely
- #10 `VectorIndexLoader` zero-member class — deleted entirely
- #11 `fs == nullptr` silent mode — documented in single `init_searcher`
- #12 cache key separator — key is the path itself; no separator
- #13 `DCHECK(is_index_loaded)` — promoted to Status check
- #14 double IndexMeta copy — single copy in `init_searcher`
- #15 cache-key string rebuild — key is `path`, zero-copy `string_view`
- #16 SCOPED setter only wraps loader body — extended to deleter via captured tracker
- #17 no BE warning on legacy config — consolidated warning in `ExecEnv::init`
- #18 header references design doc — irrelevant (loader deleted)

Remaining (addressed as cleanups in PR-B):

- #3 NotFound test loose — tightened in new `vector_index_cache_test.cpp`
- #8 test util proliferation — opportunistic: add `vector_index_test_util.h` if test code duplication hurts, else defer
