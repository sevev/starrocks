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

#include "storage/index/inverted/builtin/block_posting_writer.h"

namespace starrocks {

// PR0 walking-skeleton stubs. Block chunking, PFOR encoding, and IndexedColumn writing are
// implemented in the storage PR (PR-A).
BlockPostingWriter::BlockPostingWriter(WritableFile* wfile) : _wfile(wfile) {}

BlockPostingWriter::~BlockPostingWriter() = default;

void BlockPostingWriter::start_term(uint32_t /*term_ordinal*/) {}

void BlockPostingWriter::add(uint32_t /*docid*/, uint32_t /*tf*/, uint32_t /*doc_len*/) {}

Status BlockPostingWriter::finish_term() {
    return Status::OK();
}

Status BlockPostingWriter::finish(IndexedColumnMetaPB* /*block_meta*/, IndexedColumnMetaPB* /*dir_meta*/) {
    return Status::OK();
}

} // namespace starrocks
