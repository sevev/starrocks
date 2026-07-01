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

#include "storage/primitive/bm25_search_option.h" // BM25Stats

namespace starrocks {

// Single-term BM25 contribution (pure function). `idf` is the precomputed scalar for this query
// term; avgdl/k1/b come from `stats`. The scoring loop sums this over a doc's matching query
// terms. See docs/design/builtin-gin-bm25-reference-impl.md (interface I3, section F1).
double bm25_term(uint32_t tf, uint32_t doc_len, double idf, const BM25Stats& stats);

} // namespace starrocks
