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

#include "storage/index/inverted/builtin/bm25_scorer.h"

namespace starrocks {

// PR0 walking-skeleton stub: produces no scores while the feature is gated off. The real
// term-at-a-time co-iteration (posting + doc_len -> bm25_term) lands in the scoring PR (PR-B).
ScoreAllScorer::ScoreAllScorer() = default;

ScoreAllScorer::~ScoreAllScorer() = default;

Status ScoreAllScorer::run(std::unordered_map<rowid_t, double>* /*id2score_map*/) {
    return Status::OK();
}

} // namespace starrocks
