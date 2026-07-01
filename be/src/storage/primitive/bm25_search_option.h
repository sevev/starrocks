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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/global_types.h" // SlotId

namespace starrocks {

// Per-query BM25 statistics, computed in Phase-1 (tablet-local) and injected into the segment
// read options, then consumed by the scoring kernel in Phase-2. Small: idf[] holds one double
// per query term. See docs/design/builtin-gin-bm25-reference-impl.md (interface I3).
struct BM25Stats {
    int64_t N = 0;           // document count (tablet-local)
    double avgdl = 0.0;      // average document length = sum_len / N
    std::vector<double> idf; // per query term, aligned to query-term order
    double k1 = 1.2;
    double b = 0.75;
};

// FE->BE BM25 request, built from TBM25SearchOptions in the scan source and carried through the
// read options into the segment iterator. Mirrors VectorSearchOption. (interface I5a)
struct BM25SearchOption {
    bool enable = false;
    std::string query;             // the MATCH_ANY / MATCH_ALL query string
    int index_column_id = -1;      // the GIN-indexed column being scored
    std::string score_column_name; // synthetic score column, e.g. "__bm25_score"
    SlotId score_slot_id = -1;
    double k1 = 1.2;
    double b = 0.75;
};

using BM25SearchOptionPtr = std::shared_ptr<BM25SearchOption>;

} // namespace starrocks
