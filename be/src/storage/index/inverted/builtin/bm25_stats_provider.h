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

#include <string>
#include <vector>

#include "common/statusor.h"
#include "storage/primitive/bm25_search_option.h" // BM25Stats

namespace starrocks {

// Identifies a query term for stats lookup; resolved to a per-segment dict ordinal internally.
using TermKey = std::string;

// Produces per-query BM25Stats. TabletLocalProvider runs Phase-1: it aggregates df/N/sum_len
// across all segments of the tablet using cheap scalar reads only (doc_freq column, sum_len
// scalar, segment row counts), then computes idf. Swapping the provider changes the IDF/avgdl
// scope without touching the scoring kernel. See reference-impl (interface I4, section E).
class BM25StatsProvider {
public:
    virtual ~BM25StatsProvider() = default;
    virtual StatusOr<BM25Stats> get_stats(const std::vector<TermKey>& query_terms) = 0;
};

class TabletLocalProvider : public BM25StatsProvider {
public:
    TabletLocalProvider();
    ~TabletLocalProvider() override;
    StatusOr<BM25Stats> get_stats(const std::vector<TermKey>& query_terms) override;
};

} // namespace starrocks
