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

#ifdef WITH_TENANN
#pragma once

#include "common/status.h"
#include "storage/index/vector/vector_index_reader.h"
#include "tenann/common/seq_view.h"
#include "tenann/common/type_traits.h"
#include "tenann/factory/ann_searcher_factory.h"
#include "tenann/factory/index_factory.h"
#include "tenann/index/index_cache.h"
#include "tenann/searcher/ann_searcher.h"
#include "tenann/searcher/faiss_hnsw_ann_searcher.h"
#include "tenann/searcher/id_filter.h"
#include "tenann/store/index_meta.h"

namespace starrocks {

class TenANNReader final : public VectorIndexReader {
public:
    TenANNReader() = default;
    ~TenANNReader() override = default;

    // Single unified init path. When `fs == nullptr` the index file is read
    // directly from the local filesystem; otherwise reads go through
    // VectorIndexFileReader on the provided FileSystem (S3/HDFS/OSS).
    //
    // Single-flight on the SR-owned VectorIndexCache (via
    // tenann::GetGlobalIndexCache()) dedups concurrent cold misses for the
    // same `index_path`; the loader runs under the vector_index mem tracker
    // so the cache charge is attributed correctly.
    Status init_searcher(const tenann::IndexMeta& meta, const std::string& index_path,
                         FileSystem* fs = nullptr) override;

    Status init_searcher(const tenann::IndexMeta& meta, const std::string& index_path, FileSystem* fs,
                         size_t segment_num_rows, int query_k, bool user_set_ef) override;

    Status search(tenann::PrimitiveSeqView query_vector, int k, int64_t* result_ids, uint8_t* result_distances,
                  tenann::IdFilter* id_filter = nullptr) override;
    Status range_search(tenann::PrimitiveSeqView query_vector, int k, std::vector<int64_t>* result_ids,
                        std::vector<float>* result_distances, tenann::IdFilter* id_filter, float range,
                        int order) override;

private:
    std::shared_ptr<tenann::AnnSearcher> _searcher;
    // Pin the cached IndexRef for the lifetime of this reader so the entry
    // cannot be evicted while this searcher is still servicing queries.
    tenann::IndexCacheHandle _cache_handle;
};

} // namespace starrocks
#endif
