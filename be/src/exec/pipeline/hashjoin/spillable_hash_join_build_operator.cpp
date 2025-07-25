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

#include "exec/pipeline/hashjoin/spillable_hash_join_build_operator.h"

#include <atomic>
#include <memory>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/hash_join_components.h"
#include "exec/hash_join_node.h"
#include "exec/hash_joiner.h"
#include "exec/join/join_hash_map.h"
#include "exec/pipeline/hashjoin/hash_join_build_operator.h"
#include "exec/pipeline/hashjoin/hash_joiner_factory.h"
#include "exec/pipeline/query_context.h"
#include "exec/spill/options.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller.hpp"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"
#include "util/bit_util.h"
#include "util/defer_op.h"

namespace starrocks::pipeline {

Status SpillableHashJoinBuildOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(HashJoinBuildOperator::prepare(state));
    _join_builder->spiller()->set_metrics(
            spill::SpillProcessMetrics(_unique_metrics.get(), state->mutable_total_spill_bytes()));
    RETURN_IF_ERROR(_join_builder->spiller()->prepare(state));
    if (state->spill_mode() == TSpillMode::FORCE) {
        set_spill_strategy(spill::SpillStrategy::SPILL_ALL);
    }
    _peak_revocable_mem_bytes = _unique_metrics->AddHighWaterMarkCounter(
            "PeakRevocableMemoryBytes", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    return Status::OK();
}

void SpillableHashJoinBuildOperator::close(RuntimeState* state) {
    HashJoinBuildOperator::close(state);
}

size_t SpillableHashJoinBuildOperator::estimated_memory_reserved(const ChunkPtr& chunk) {
    if (chunk && !chunk->is_empty()) {
        return chunk->memory_usage() + _join_builder->hash_join_builder()->ht_mem_usage();
    }
    return 0;
}

size_t SpillableHashJoinBuildOperator::estimated_memory_reserved() {
    return _join_builder->hash_join_builder()->ht_mem_usage() * 2;
}

bool SpillableHashJoinBuildOperator::need_input() const {
    return !is_finished() && !(_join_builder->spiller()->is_full() || _join_builder->spill_channel()->has_task());
}

Status SpillableHashJoinBuildOperator::set_finishing(RuntimeState* state) {
    ONCE_DETECT(_set_finishing_once);
    auto defer_set_finishing = DeferOp([this]() { _join_builder->spill_channel()->set_finishing(); });

    if (spill_strategy() == spill::SpillStrategy::NO_SPILL ||
        (!_join_builder->spiller()->spilled() && _join_builder->hash_join_builder()->hash_table_row_count() == 0)) {
        return HashJoinBuildOperator::set_finishing(state);
    }

    DCHECK(spill_strategy() == spill::SpillStrategy::SPILL_ALL);
    // if this operator is changed to spill mode just before set_finishing,
    // we should create spill task
    if (!_join_builder->spiller()->spilled()) {
        DCHECK(_is_first_time_spill);
        _is_first_time_spill = false;
        RETURN_IF_ERROR(init_spiller_partitions(state, _join_builder->hash_join_builder()));
        ASSIGN_OR_RETURN(_hash_table_slice_iterator, _convert_hash_map_to_chunk());
        RETURN_IF_ERROR(_join_builder->append_spill_task(state, _hash_table_slice_iterator));
    }

    if (state->is_cancelled()) {
        _join_builder->spiller()->cancel();
    }

    auto flush_function = [this](RuntimeState* state) {
        auto& spiller = _join_builder->spiller();
        return spiller->flush(state, TRACKER_WITH_SPILLER_GUARD(state, spiller));
    };

    auto set_call_back_function = [this](RuntimeState* state) {
        auto& spiller = _join_builder->spiller();
        return spiller->set_flush_all_call_back(
                [this]() {
                    _is_finished = true;
                    _join_builder->enter_probe_phase();
                    return Status::OK();
                },
                state, TRACKER_WITH_SPILLER_GUARD(state, spiller));
    };

    WARN_IF_ERROR(publish_runtime_filters(state),
                  fmt::format("spillable hash join operator of query {} publish runtime filter failed, ignore it...",
                              print_id(state->query_id())));
    SpillProcessTasksBuilder task_builder(state);
    task_builder.then(flush_function).finally(set_call_back_function);

    RETURN_IF_ERROR(_join_builder->spill_channel()->execute(task_builder));

    return Status::OK();
}

Status SpillableHashJoinBuildOperator::publish_runtime_filters(RuntimeState* state) {
    // publish empty runtime filters
    // Building RuntimeMembershipFilter need to know the initial hash table size and all join keys datas.
    // It usually involves re-reading all the data that has been spilled
    // which cannot be streamed process in the spill scenario when build phase is finished
    // (unless FE can give an estimate of the hash table size), so we currently empty all the hash tables first
    // we could build global runtime filter for this case later.

    bool is_colocate_runtime_filter = runtime_filter_hub()->is_colocate_runtime_filters(_plan_node_id);
    if (is_colocate_runtime_filter) {
        // init local colocate in/bloom filters
        RuntimeInFilterList in_filter_lists;
        RuntimeMembershipFilterList bloom_filters;
        runtime_filter_hub()->set_collector(_plan_node_id, _driver_sequence,
                                            std::make_unique<RuntimeFilterCollector>(in_filter_lists));
        state->runtime_filter_port()->publish_local_colocate_filters(bloom_filters);
    } else {
        auto merged = _partial_rf_merger->set_always_true();
        // for spillable operator, this interface never returns error status because we skip building rf here
        DCHECK(merged.ok());

        if (merged.value()) {
            RuntimeInFilterList in_filters;
            RuntimeMembershipFilterList bloom_filters;
            // publish empty runtime bloom-filters
            state->runtime_filter_port()->publish_runtime_filters(bloom_filters);
            // move runtime filters into RuntimeFilterHub.
            runtime_filter_hub()->set_collector(
                    _plan_node_id,
                    std::make_unique<RuntimeFilterCollector>(std::move(in_filters), std::move(bloom_filters)));
        }
    }
    return Status::OK();
}

Status SpillableHashJoinBuildOperator::append_hash_columns(const ChunkPtr& chunk) {
    auto factory = down_cast<SpillableHashJoinBuildOperatorFactory*>(_factory);
    const auto& build_partition = factory->build_side_partition();

    size_t num_rows = chunk->num_rows();
    auto hash_column = spill::SpillHashColumn::create(num_rows);
    auto& hash_values = hash_column->get_data();

    // TODO: use different hash method
    for (auto& expr_ctx : build_partition) {
        ASSIGN_OR_RETURN(auto res, expr_ctx->evaluate(chunk.get()));
        res->fnv_hash(hash_values.data(), 0, num_rows);
    }
    chunk->append_column(std::move(hash_column), Chunk::HASH_JOIN_SPILL_HASH_SLOT_ID);
    return Status::OK();
}

Status SpillableHashJoinBuildOperator::init_spiller_partitions(RuntimeState* state, HashJoinBuilder* builder) {
    if (builder->hash_table_row_count() > 0) {
        // We estimate the size of the hash table to be twice the size of the already input hash table
        auto num_partitions =
                builder->ht_mem_usage() * 2 / _join_builder->spiller()->options().spill_mem_table_bytes_size;
        _join_builder->spiller()->set_partition(state, num_partitions);
    }
    return Status::OK();
}

bool SpillableHashJoinBuildOperator::is_finished() const {
    return _is_finished || _join_builder->is_finished();
}

Status SpillableHashJoinBuildOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    DeferOp update_revocable_bytes{
            [this]() { set_revocable_mem_bytes(_join_builder->hash_join_builder()->ht_mem_usage()); }};

    if (spill_strategy() == spill::SpillStrategy::NO_SPILL) {
        return HashJoinBuildOperator::push_chunk(state, chunk);
    }

    if (!chunk || chunk->is_empty()) {
        return Status::OK();
    }

    // Estimate the appropriate number of partitions
    if (_is_first_time_spill) {
        RETURN_IF_ERROR(init_spiller_partitions(state, _join_builder->hash_join_builder()));
    }

    auto spill_chunk = _join_builder->hash_join_builder()->convert_to_spill_schema(chunk);
    RETURN_IF_ERROR(append_hash_columns(spill_chunk));

    RETURN_IF_ERROR(_join_builder->append_chunk_to_spill_buffer(state, spill_chunk));

    if (_is_first_time_spill) {
        _is_first_time_spill = false;
        ASSIGN_OR_RETURN(_hash_table_slice_iterator, _convert_hash_map_to_chunk());
        RETURN_IF_ERROR(_join_builder->append_spill_task(state, _hash_table_slice_iterator));
    }

    return Status::OK();
}

void SpillableHashJoinBuildOperator::set_execute_mode(int performance_level) {
    if (!_is_finished) {
        _join_builder->set_spill_strategy(spill::SpillStrategy::SPILL_ALL);
    }
}

void SpillableHashJoinBuildOperator::set_spill_strategy(spill::SpillStrategy strategy) {
    _join_builder->set_spill_strategy(strategy);
}

spill::SpillStrategy SpillableHashJoinBuildOperator::spill_strategy() const {
    return _join_builder->spill_strategy();
}

StatusOr<std::function<StatusOr<ChunkPtr>()>> SpillableHashJoinBuildOperator::_convert_hash_map_to_chunk() {
    _hash_tables.clear();
    _hash_table_iterate_idx = 0;

    _join_builder->hash_join_builder()->visitHt([this](JoinHashTable* ht) { _hash_tables.push_back(ht); });

    for (auto* ht : _hash_tables) {
        auto build_chunk = ht->get_build_chunk();
        DCHECK_GT(build_chunk->num_rows(), 0);
        RETURN_IF_ERROR(build_chunk->upgrade_if_overflow());
    }

    _hash_table_build_chunk_slice.reset(_hash_tables[_hash_table_iterate_idx]->get_build_chunk());
    _hash_table_build_chunk_slice.skip(kHashJoinKeyColumnOffset);

    return [this]() -> StatusOr<ChunkPtr> {
        if (_hash_table_build_chunk_slice.empty()) {
            _hash_table_iterate_idx++;
            for (; _hash_table_iterate_idx < _hash_tables.size(); _hash_table_iterate_idx++) {
                auto build_chunk = _hash_tables[_hash_table_iterate_idx]->get_build_chunk();
                if (build_chunk->num_rows() > 0) {
                    _hash_table_build_chunk_slice.reset(build_chunk);
                    _hash_table_build_chunk_slice.skip(kHashJoinKeyColumnOffset);
                    break;
                }
            }
            if (_hash_table_build_chunk_slice.empty()) {
                _join_builder->hash_join_builder()->reset(_join_builder->hash_table_param());
                return Status::EndOfFile("eos");
            }
        }

        ChunkPtr chunk = _hash_table_build_chunk_slice.cutoff(runtime_state()->chunk_size());
        RETURN_IF_ERROR(chunk->downgrade());
        RETURN_IF_ERROR(append_hash_columns(chunk));
        _join_builder->update_build_rows(chunk->num_rows());
        return chunk;
    };
}

Status SpillableHashJoinBuildOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(HashJoinBuildOperatorFactory::prepare(state));

    // no order by, init with 4 partitions
    _spill_options = std::make_shared<spill::SpilledOptions>(config::spill_init_partition);
    _spill_options->spill_mem_table_bytes_size = state->spill_mem_table_size();
    _spill_options->mem_table_pool_size = state->spill_mem_table_num();
    _spill_options->spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    _spill_options->min_spilled_size = state->spill_operator_min_bytes();
    _spill_options->block_manager = state->query_ctx()->spill_manager()->block_manager();
    _spill_options->name = "hash-join-build";
    _spill_options->plan_node_id = _plan_node_id;
    _spill_options->encode_level = state->spill_encode_level();
    _spill_options->wg = state->fragment_ctx()->workgroup();
    // TODO: Our current adaptive dop for non-broadcast functions will also result in a build hash_joiner corresponding to multiple prob hash_join prober.
    //
    _spill_options->read_shared =
            _hash_joiner_factory->hash_join_param()._distribution_mode == TJoinDistributionMode::BROADCAST ||
            _hash_joiner_factory->hash_join_param()._distribution_mode == TJoinDistributionMode::LOCAL_HASH_BUCKET ||
            state->fragment_ctx()->enable_adaptive_dop();

    _spill_options->enable_buffer_read = state->enable_spill_buffer_read();
    _spill_options->max_read_buffer_bytes = state->max_spill_read_buffer_bytes_per_driver();

    const auto& param = _hash_joiner_factory->hash_join_param();

    _build_side_partition = param._build_expr_ctxs;

    return Status::OK();
}

void SpillableHashJoinBuildOperatorFactory::close(RuntimeState* state) {
    HashJoinBuildOperatorFactory::close(state);
}

OperatorPtr SpillableHashJoinBuildOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    if (_string_key_columns.empty()) {
        _string_key_columns.resize(degree_of_parallelism);
    }

    auto spiller = _spill_factory->create(*_spill_options);
    auto spill_channel = _spill_channel_factory->get_or_create(driver_sequence);
    spill_channel->set_spiller(spiller);

    auto joiner = _hash_joiner_factory->create_builder(degree_of_parallelism, driver_sequence);

    joiner->set_spill_channel(spill_channel);
    joiner->set_spiller(spiller);

    return std::make_shared<SpillableHashJoinBuildOperator>(this, _id, "spillable_hash_join_build", _plan_node_id,
                                                            driver_sequence, joiner, _partial_rf_merger.get(),
                                                            _distribution_mode);
}

} // namespace starrocks::pipeline
