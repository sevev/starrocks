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

#include "storage/index/inverted/builtin/bm25_stats_provider.h"

namespace starrocks {

// PR0 walking-skeleton stub: returns empty stats while the feature is gated off. The real
// tablet-local Phase-1 prescan lands in the stats PR (PR-D).
TabletLocalProvider::TabletLocalProvider() = default;

TabletLocalProvider::~TabletLocalProvider() = default;

StatusOr<BM25Stats> TabletLocalProvider::get_stats(const std::vector<TermKey>& /*query_terms*/) {
    return BM25Stats{};
}

} // namespace starrocks
