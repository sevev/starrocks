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

#include "storage/index/inverted/builtin/block_posting_reader.h"

namespace starrocks {

// PR0 walking-skeleton stubs. IndexedColumn loading and block decoding land in the storage PR
// (PR-A); the WAND-facing methods land in the WAND PR.
BlockPostingReader::BlockPostingReader() = default;

BlockPostingReader::~BlockPostingReader() = default;

Status BlockPostingReader::load(const IndexReadOptions& /*opts*/, const PostingIndexPB& /*meta*/) {
    return Status::OK();
}

Status BlockPostingReader::seek_to_term(uint32_t /*term_ordinal*/) {
    return Status::OK();
}

bool BlockPostingReader::has_next_block() const {
    return false;
}

Status BlockPostingReader::next_block() {
    return Status::OK();
}

size_t BlockPostingReader::cur_block_size() const {
    return 0;
}

const uint32_t* BlockPostingReader::docids() const {
    return nullptr;
}

const uint32_t* BlockPostingReader::tfs() const {
    return nullptr;
}

uint32_t BlockPostingReader::cur_block_max_tf() const {
    return 0;
}

uint32_t BlockPostingReader::cur_block_min_doclen() const {
    return 0;
}

uint32_t BlockPostingReader::cur_block_last_docid() const {
    return 0;
}

Status BlockPostingReader::seek_block(uint32_t /*target_docid*/) {
    return Status::OK();
}

} // namespace starrocks
