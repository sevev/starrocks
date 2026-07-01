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
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "storage/primitive/bm25_search_option.h" // BM25Stats
#include "storage/primitive/rowid_types.h"         // rowid_t

namespace starrocks {

class BlockPostingReader;

// Query-execution engine for BM25. Drives the per-doc scoring loop and writes scores into a
// rowid->score map (mirror of the ANN id2distance_map). ScoreAllScorer is term-at-a-time with no
// pruning; WandScorer (block-max pruning) is added later as a sibling implementation.
// See docs/design/builtin-gin-bm25-reference-impl.md (interface I8, sections F2/F4).
class BM25Scorer {
public:
    virtual ~BM25Scorer() = default;
    // Fill *id2score_map with per-doc BM25 scores for the candidate set.
    virtual Status run(std::unordered_map<rowid_t, double>* id2score_map) = 0;
};

class ScoreAllScorer : public BM25Scorer {
public:
    ScoreAllScorer();
    ~ScoreAllScorer() override;
    Status run(std::unordered_map<rowid_t, double>* id2score_map) override;
};

// segment_iterator-internal runtime state for a BM25 query (mirror of VectorIndexContext). Holds
// the per-doc score map filled by the scorer and read during chunk emission. (interface I5b)
struct BM25Context {
    std::unordered_map<rowid_t, double> id2score_map;
    BM25Stats stats;
    BlockPostingReader* reader = nullptr;
    std::vector<uint32_t> term_ords; // query-term index -> this segment's dict ordinal
    SlotId score_slot_id = -1;
};

} // namespace starrocks
