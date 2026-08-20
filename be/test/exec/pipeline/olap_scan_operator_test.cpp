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

#include "exec/pipeline/scan/olap_scan_operator.h"

#include "compute_env/global_dict/fragment_dict_state.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "exec/exec_env.h"
#include "exec/olap_scan_node.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/scan/olap_chunk_source.h"
#include "exec/pipeline/scan/olap_scan_prepare_operator.h"
#include "exec_primitive/pipeline/scan/scan_morsel.h"
#include "gtest/gtest.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/query/olap_fixed_morsel_queue.h"
#include "storage/tablet_scan_key_pruner.h"
#include "storage/tablet_schema.h"

#include <set>

namespace starrocks::pipeline {

namespace {

// A minimal DUP_KEYS schema with one INT key, enough for the pruner to type a scan key and for
// OlapPredicateParser to be constructed.
TabletSchemaCSPtr make_int_key_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_num_rows_per_row_block(1024);
    auto* key = schema_pb.add_column();
    key->set_unique_id(1);
    key->set_name("k1");
    key->set_type("INT");
    key->set_is_key(true);
    key->set_is_nullable(false);
    key->set_aggregation("NONE");
    auto* value = schema_pb.add_column();
    value->set_unique_id(2);
    value->set_name("v1");
    value->set_type("INT");
    value->set_is_key(false);
    value->set_is_nullable(true);
    value->set_aggregation("NONE");
    return TabletSchema::create(schema_pb);
}

void expect_vector_index_counter(RuntimeProfile* profile, const char* name, const char* parent) {
    auto it = profile->_counter_map.find(name);
    ASSERT_NE(it, profile->_counter_map.end()) << name;
    EXPECT_EQ(it->second.second, parent) << name;
    EXPECT_EQ(it->second.first->value(), 0) << name;
}

void expect_vector_index_counters(RuntimeProfile* profile) {
    ASSERT_NE(profile, nullptr);
    expect_vector_index_counter(profile, "VectorIndex", "SegmentInit");
    expect_vector_index_counter(profile, "VectorIndexLoad", "VectorIndex");
    expect_vector_index_counter(profile, "VectorIndexCacheLookup", "VectorIndexLoad");
    expect_vector_index_counter(profile, "VectorIndexFileOpenAndGetSize", "VectorIndexLoad");
    expect_vector_index_counter(profile, "VectorIndexFileRead", "VectorIndexLoad");
    expect_vector_index_counter(profile, "VectorIndexDeserialize", "VectorIndexLoad");
    expect_vector_index_counter(profile, "VectorIndexSearcherCreate", "VectorIndexLoad");
    expect_vector_index_counter(profile, "VectorIndexCacheHit", "VectorIndexCacheLookup");
    expect_vector_index_counter(profile, "VectorIndexCacheMiss", "VectorIndexCacheLookup");
    expect_vector_index_counter(profile, "VectorIndexSearch", "VectorIndex");
    expect_vector_index_counter(profile, "VectorANNSearch", "VectorIndexSearch");
    expect_vector_index_counter(profile, "VectorResultProcess", "VectorIndexSearch");
    expect_vector_index_counter(profile, "VectorIndexFilterRows", "VectorIndexSearch");
}

} // namespace

class OlapScanOperatorTest : public ::testing::Test {
public:
    void SetUp() override;

protected:
    // Builds a prepared OlapChunkSource over an empty scan range. Keeps ownership so the caller can
    // use the raw pointer for the length of the test.
    OlapChunkSource* make_chunk_source(OlapScanNode* scan_node, ChunkBufferLimiterPtr limiter) {
        auto scan_ctx_factory =
                std::make_shared<OlapScanContextFactory>(scan_node, 1, false, false, std::move(limiter));
        auto* factory = _object_pool.add(new OlapScanOperatorFactory(1, scan_node, scan_ctx_factory));
        auto scan_operator = std::make_shared<OlapScanOperator>(factory, 1, 0, 1, scan_node,
                                                                scan_ctx_factory->get_or_create(0));
        _scan_operators.push_back(scan_operator);
        TScanRange scan_range;
        auto chunk_source = scan_operator->create_chunk_source(std::make_unique<ScanMorsel>(1, scan_range), 0);
        auto* olap_chunk_source = down_cast<OlapChunkSource*>(chunk_source.get());
        CHECK(olap_chunk_source->ChunkSource::prepare(&_runtime_state).ok());
        // OlapChunkSource::prepare would set this, but it also opens the tablet reader, which needs a
        // real tablet. The reader params only read query options off it.
        olap_chunk_source->_runtime_state = &_runtime_state;
        olap_chunk_source->_init_counter(&_runtime_state);
        // _init_reader_params reaches into the context's conjuncts manager, which stays null until
        // parse_conjuncts runs. There are no conjuncts here; this just builds the manager.
        CHECK(olap_chunk_source->_scan_ctx->parse_conjuncts(&_runtime_state, {}, &_runtime_filters, 0).ok());
        // _slots is normally filled by the full prepare() path, which needs a real tablet. The
        // reader params only walk it to map conjunct slots onto schema fields.
        olap_chunk_source->_slots = &_tbl->get_tuple_descriptor(1)->slots();
        _chunk_sources.push_back(std::move(chunk_source));
        return olap_chunk_source;
    }

    ObjectPool _object_pool;
    std::vector<std::shared_ptr<OlapScanOperator>> _scan_operators;
    std::vector<ChunkSourcePtr> _chunk_sources;
    RuntimeFilterProbeCollector _runtime_filters;
    FragmentRuntimeState _fragment_runtime_state;
    RuntimeState _runtime_state;
    TDescriptorTable _thrift_tbl;
    const int64_t _chunk_size = 4096;
    DescriptorTbl* _tbl = nullptr;
    TPlanNode _tnode;
    ChunkBufferLimiterPtr _chunk_buffer_limiter;
    QueryContext _query_ctx;
    std::unique_ptr<FragmentDictState> _fragment_dict_state;
};

void OlapScanOperatorTest::SetUp() {
    TTableDescriptor t_table_desc;
    t_table_desc.id = 1;
    t_table_desc.tableType = TTableType::OLAP_TABLE;
    _thrift_tbl.tableDescriptors.emplace_back(t_table_desc);

    TTupleDescriptor t_tuple_desc;
    t_tuple_desc.id = 1;
    t_tuple_desc.tableId = 1;
    _thrift_tbl.tupleDescriptors.emplace_back(t_tuple_desc);

    _tnode.row_tuples.emplace_back(1);

    Status st = DescriptorTbl::create(&_runtime_state, &_object_pool, _thrift_tbl, &_tbl, _chunk_size);
    ASSERT_TRUE(st.ok());

    _runtime_state.set_desc_tbl(_tbl);
    _fragment_dict_state = std::make_unique<FragmentDictState>();
    _runtime_state.set_fragment_dict_state(_fragment_dict_state.get());
    _chunk_buffer_limiter = std::make_unique<UnlimitedChunkBufferLimiter>();

    _query_ctx.init_mem_tracker(-1, RuntimeEnv::GetInstance()->process_mem_tracker());
    _runtime_state.set_query_ctx(&_query_ctx, &_query_ctx.query_runtime_state(), _query_ctx.object_pool());
    // parse_conjuncts reads pred_tree_params off the fragment runtime state; without one it is a
    // null dereference.
    _runtime_state.set_fragment_runtime_state(&_fragment_runtime_state);
}

TEST_F(OlapScanOperatorTest, test_finish_sequence) {
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("OlapScanPrepareOperator::prepare",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });
    SyncPoint::GetInstance()->SetCallBack("ScanOperatorFactory::prepare",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });
    SyncPoint::GetInstance()->SetCallBack("OlapScanContext::parse_conjuncts",
                                          [](void* arg) { *(Status*)arg = Status::EndOfFile(""); });

    Morsels morsels;
    OlapFixedMorselQueue morsel_queue(std::move(morsels));

    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    auto scan_ctx_factory =
            std::make_shared<OlapScanContextFactory>(&scan_node, 1, false, false, std::move(_chunk_buffer_limiter));

    // create operator factory
    OlapScanPrepareOperatorFactory scan_prepare_operator_factory(1, 1, &scan_node, scan_ctx_factory);
    Status st = scan_prepare_operator_factory.prepare(&_runtime_state);
    ASSERT_TRUE(st.ok());

    OlapScanOperatorFactory scan_operator_factory(1, &scan_node, scan_ctx_factory);
    st = scan_operator_factory.prepare(&_runtime_state);
    ASSERT_TRUE(st.ok());

    // create operator
    auto scan_prepare_operator = scan_prepare_operator_factory.create(1, 0);
    ASSERT_TRUE(scan_prepare_operator != nullptr);
    down_cast<OlapScanPrepareOperator*>(scan_prepare_operator.get())->add_morsel_queue(&morsel_queue);

    auto scan_operator = scan_operator_factory.create(1, 0);
    ASSERT_TRUE(scan_operator != nullptr);

    // operator prepare
    st = scan_prepare_operator->prepare(&_runtime_state);
    ASSERT_TRUE(st.ok());

    // pull chunk
    SyncPoint::GetInstance()->SetCallBack("OlapScnPrepareOperator::pull_chunk::before_set_finished",
                                          [&scan_operator](void* arg) { ASSERT_FALSE(scan_operator->has_output()); });
    SyncPoint::GetInstance()->SetCallBack("OlapScnPrepareOperator::pull_chunk::after_set_finished",
                                          [&scan_operator](void* arg) { ASSERT_FALSE(scan_operator->has_output()); });
    SyncPoint::GetInstance()->SetCallBack("OlapScnPrepareOperator::pull_chunk::after_set_prepare_finished",
                                          [&scan_operator](void* arg) { ASSERT_FALSE(scan_operator->has_output()); });

    auto ret = scan_prepare_operator->pull_chunk(&_runtime_state);
    ASSERT_TRUE(ret.status().is_end_of_file());

    scan_node.close(&_runtime_state);

    SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(OlapScanOperatorTest, legacy_scan_registers_vector_index_counters) {
    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    // Legacy Gin counters still attach to the node profile instead of the scan profile.
    ADD_TIMER(scan_node._runtime_profile, "SegmentInit");

    scan_node._init_counter(&_runtime_state);

    expect_vector_index_counters(scan_node._scan_profile);
    scan_node.close(&_runtime_state);
}

TEST_F(OlapScanOperatorTest, pipeline_chunk_source_registers_vector_index_counters) {
    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    auto scan_ctx_factory =
            std::make_shared<OlapScanContextFactory>(&scan_node, 1, false, false, std::move(_chunk_buffer_limiter));
    OlapScanOperatorFactory scan_operator_factory(1, &scan_node, scan_ctx_factory);
    auto scan_operator = std::make_shared<OlapScanOperator>(&scan_operator_factory, 1, 0, 1, &scan_node,
                                                            scan_ctx_factory->get_or_create(0));
    TScanRange scan_range;
    auto chunk_source = scan_operator->create_chunk_source(std::make_unique<ScanMorsel>(1, scan_range), 0);
    auto* olap_chunk_source = down_cast<OlapChunkSource*>(chunk_source.get());

    ASSERT_TRUE(olap_chunk_source->ChunkSource::prepare(&_runtime_state).ok());
    olap_chunk_source->_init_counter(&_runtime_state);

    expect_vector_index_counters(olap_chunk_source->_runtime_profile);
    scan_node.close(&_runtime_state);
}

// The pruned range set must be what reaches TabletReaderParams: the two _init_reader_params
// overloads exist so the pruned path can pass borrowed pointers while the untouched path keeps
// passing the owning vector, and nothing else in the BE suite drives either of them.
TEST_F(OlapScanOperatorTest, reader_params_take_the_pruned_range_set) {
    _tnode.olap_scan_node.tuple_id = 1;
    _tnode.olap_scan_node.is_preaggregation = false;
    _tnode.__isset.olap_scan_node = true;
    auto schema = make_int_key_schema();

    std::vector<std::unique_ptr<OlapScanRange>> owned;
    for (int32_t value = 0; value < 16; ++value) {
        auto range = std::make_unique<OlapScanRange>();
        range->begin_include = true;
        range->end_include = true;
        range->begin_scan_range = OlapTuple({std::to_string(value)});
        range->end_scan_range = OlapTuple({std::to_string(value)});
        owned.emplace_back(std::move(range));
    }
    std::vector<OlapScanRange*> all;
    for (const auto& range : owned) {
        all.push_back(range.get());
    }

    // Untouched path: the owning vector overload hands every range to the reader params.
    {
        OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
        auto* source = make_chunk_source(&scan_node, std::make_unique<UnlimitedChunkBufferLimiter>());
        source->_tablet_schema = schema;
        ASSERT_TRUE(source->_init_reader_params(owned).ok());
        EXPECT_EQ(owned.size(), source->_params.start_key.size());
        EXPECT_EQ(owned.size(), source->_params.end_key.size());
        scan_node.close(&_runtime_state);
    }

    // Pruned path: whatever the pruner keeps for a bucket is exactly what the reader is told to
    // seek. Asserting the two buckets partition the input is the property that matters -- a range
    // dropped by both buckets would be rows silently missing from the query.
    constexpr int32_t kBucketNum = 2;
    size_t kept_total = 0;
    std::set<std::string> kept_keys;
    for (int32_t bucket_id = 0; bucket_id < kBucketNum; ++bucket_id) {
        TabletHashBucketConstraint constraint;
        constraint.distribution_key_positions = {0};
        constraint.bucket_id = bucket_id;
        constraint.bucket_num = kBucketNum;
        constraint.pruning_was_exact = false;
        auto pruned = TabletScanKeyPruner::prune_hash(constraint, *schema, all);
        ASSERT_FALSE(pruned.fallback);

        OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
        auto* source = make_chunk_source(&scan_node, std::make_unique<UnlimitedChunkBufferLimiter>());
        source->_tablet_schema = schema;
        ASSERT_TRUE(source->_init_reader_params(pruned.ranges).ok());
        EXPECT_EQ(pruned.ranges.size(), source->_params.start_key.size());

        kept_total += pruned.ranges.size();
        for (const auto* range : pruned.ranges) {
            // No range may be kept by more than one bucket.
            EXPECT_TRUE(kept_keys.insert(range->begin_scan_range.get_value(0)).second);
        }
        scan_node.close(&_runtime_state);
    }
    EXPECT_EQ(owned.size(), kept_total);
    EXPECT_EQ(owned.size(), kept_keys.size());
}

} // namespace starrocks::pipeline
