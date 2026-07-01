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

#include <cstddef>
#include <cstdint>

#include "common/status.h"

namespace starrocks {

class IndexReadOptions;
class PostingIndexPB;

// Reads per-term block posting lists written by BlockPostingWriter. The score-all path uses
// seek_to_term + block iteration (docids/tfs). The WAND path additionally uses the per-block max
// statistics and seek_block; those methods are declared now to freeze the interface and filled in
// the WAND PR. See docs/design/builtin-gin-bm25-reference-impl.md (interface I2, section C1).
class BlockPostingReader {
public:
    BlockPostingReader();
    ~BlockPostingReader();

    BlockPostingReader(const BlockPostingReader&) = delete;
    BlockPostingReader& operator=(const BlockPostingReader&) = delete;

    Status load(const IndexReadOptions& opts, const PostingIndexPB& meta);

    // Position at a term's posting list (term_ordinal aligned with the dict ordinal).
    Status seek_to_term(uint32_t term_ordinal);

    // Score-all iteration over the current term's blocks.
    bool has_next_block() const;
    Status next_block();
    size_t cur_block_size() const;
    const uint32_t* docids() const; // segment rowids of the current block
    const uint32_t* tfs() const;    // term frequencies of the current block

    // WAND-facing (implemented in the WAND PR; declared now to freeze the interface).
    uint32_t cur_block_max_tf() const;
    uint32_t cur_block_min_doclen() const;
    uint32_t cur_block_last_docid() const;
    Status seek_block(uint32_t target_docid);
};

} // namespace starrocks
