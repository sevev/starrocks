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

#include "common/status.h"

namespace starrocks {

class WritableFile;
class IndexedColumnMetaPB;

// Encodes per-term (docid, tf) posting lists into fixed-size (128-doc) blocks, plus a directory
// with per-block {last_docid, max_tf, min_doclen} for WAND. Produces two IndexedColumns written
// inline into the segment .dat: a block column (keyed by block id) and a directory column (keyed
// by term ordinal). Block payloads use the architecture-neutral PFOR codec (gin_pfor).
// See docs/design/builtin-gin-bm25-reference-impl.md (interface I2, section B3).
class BlockPostingWriter {
public:
    explicit BlockPostingWriter(WritableFile* wfile);
    ~BlockPostingWriter();

    BlockPostingWriter(const BlockPostingWriter&) = delete;
    BlockPostingWriter& operator=(const BlockPostingWriter&) = delete;

    // Begin a new term's posting list (terms fed in dict-ordinal order).
    void start_term(uint32_t term_ordinal);
    // Append one posting. doc_len feeds the per-block min_doclen used for the WAND upper bound.
    void add(uint32_t docid, uint32_t tf, uint32_t doc_len);
    // Flush the current term's blocks and its directory entry.
    Status finish_term();
    // Finalize both IndexedColumns and populate their metadata.
    Status finish(IndexedColumnMetaPB* block_meta, IndexedColumnMetaPB* dir_meta);

private:
    WritableFile* _wfile;
};

} // namespace starrocks
