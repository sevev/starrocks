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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "butil/time.h"
#include "column/column_hash.h"
#include "column/fixed_length_column.h"
#include "exprs/arithmetic_expr.h"
#include "exprs/exprs_test_helper.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class JITCacheTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::INT);
        engine = JITEngine::get_instance();
        engine->init();
    }

public:
    RuntimeState runtime_state;
    TExprNode expr_node;
    JITEngine* engine;
};

// Each shard in LRU cache has one entry capacity. In each loop, the 3 expressions' first run insert the compiled func
// into the cache, and the second run hit the cache, and at last, invalid all cache.
TEST_F(JITCacheTest, cache) {
    for (auto i = 0; i < 10; i++) {
        // Normal int8, shard = 2
        {
            expr_node.opcode = TExprOpcode::ADD;
            expr_node.type = gen_type_desc(TPrimitiveType::TINYINT);

            std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

            MockVectorizedExpr<TYPE_TINYINT> col1(expr_node, 10, 1);
            MockVectorizedExpr<TYPE_TINYINT> col2(expr_node, 10, 2);

            expr->_children.push_back(&col1);
            expr->_children.push_back(&col2);

            auto expr_name = expr->jit_func_name(&runtime_state);
            auto callable = engine->lookup(expr_name);
            ASSERT_TRUE(callable == nullptr);

            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
                ASSERT_FALSE(ptr->is_nullable());
                ASSERT_TRUE(ptr->is_numeric());

                auto v = Int8Column::static_pointer_cast(ptr);
                ASSERT_EQ(10, v->size());

                for (int j = 0; j < v->size(); ++j) {
                    ASSERT_EQ(3, v->get_data()[j]);
                }
            });

            // cached
            callable = engine->lookup(expr_name);
            ASSERT_TRUE(callable != nullptr);
            ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
                ASSERT_FALSE(ptr->is_nullable());
                ASSERT_TRUE(ptr->is_numeric());

                auto v = Int8Column::static_pointer_cast(ptr);
                ASSERT_EQ(10, v->size());

                for (int j = 0; j < v->size(); ++j) {
                    ASSERT_EQ(3, v->get_data()[j]);
                }
            });
        }

        // Normal int, shard = 2
        {
            expr_node.opcode = TExprOpcode::ADD;
            expr_node.type = gen_type_desc(TPrimitiveType::INT);

            std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

            MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 1);
            MockVectorizedExpr<TYPE_INT> col2(expr_node, 10, 2);

            expr->_children.push_back(&col1);
            expr->_children.push_back(&col2);

            auto expr_name = expr->jit_func_name(&runtime_state);
            auto callable = engine->lookup(expr_name);
            ASSERT_TRUE(callable == nullptr);

            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
                ASSERT_FALSE(ptr->is_nullable());
                ASSERT_TRUE(ptr->is_numeric());

                auto v = Int32Column::static_pointer_cast(ptr);
                ASSERT_EQ(10, v->size());

                for (int j = 0; j < v->size(); ++j) {
                    ASSERT_EQ(3, v->get_data()[j]);
                }
            });
            // cached
            callable = engine->lookup(expr_name);
            ASSERT_TRUE(callable != nullptr);
            ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
                ASSERT_FALSE(ptr->is_nullable());
                ASSERT_TRUE(ptr->is_numeric());

                auto v = Int32Column::static_pointer_cast(ptr);
                ASSERT_EQ(10, v->size());

                for (int j = 0; j < v->size(); ++j) {
                    ASSERT_EQ(3, v->get_data()[j]);
                }
            });
        }

        // Large int, shard = 4
        {
            expr_node.opcode = TExprOpcode::ADD;
            expr_node.type = gen_type_desc(TPrimitiveType::LARGEINT);

            std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

            MockVectorizedExpr<TYPE_LARGEINT> col1(expr_node, 10, 3);
            MockVectorizedExpr<TYPE_LARGEINT> col2(expr_node, 10, 4);

            expr->_children.push_back(&col1);
            expr->_children.push_back(&col2);

            auto expr_name = expr->jit_func_name(&runtime_state);
            auto callable = engine->lookup(expr_name);
            ASSERT_TRUE(callable == nullptr);

            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
                ASSERT_FALSE(ptr->is_nullable());
                ASSERT_TRUE(ptr->is_numeric());

                auto v = Int128Column::static_pointer_cast(ptr);
                ASSERT_EQ(10, v->size());

                for (int j = 0; j < v->size(); ++j) {
                    ASSERT_EQ(7, v->get_data()[j]);
                }
            });
            // cached
            callable = engine->lookup(expr_name);
            ASSERT_TRUE(callable != nullptr);
            ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
                ASSERT_FALSE(ptr->is_nullable());
                ASSERT_TRUE(ptr->is_numeric());

                auto v = Int128Column::static_pointer_cast(ptr);
                ASSERT_EQ(10, v->size());

                for (int j = 0; j < v->size(); ++j) {
                    ASSERT_EQ(7, v->get_data()[j]);
                }
            });
        }
        auto* cache = engine->get_callable_cache();
        auto capacity = cache->get_capacity();
        cache->set_capacity(0);
        cache->set_capacity(capacity);
    }
}
} // namespace starrocks
