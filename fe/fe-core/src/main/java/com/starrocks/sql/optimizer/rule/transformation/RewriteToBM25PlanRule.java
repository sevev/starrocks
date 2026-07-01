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
package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

// BM25 score() -> plan rewrite. Interface-PR skeleton: it freezes the rule's seam (RuleType,
// RuleSet registration, TopN -> OlapScan pattern, experimental gate) but performs no rewrite yet.
// Mirrors RewriteToVectorPlanRule.
//
// The real transform lands in the BM25 FE PR (PR-C): detect a zero-arg score() whose scan carries a
// WHERE col MATCH_ANY/MATCH_ALL 'q' over a builtin GIN (DOCS_AND_FREQS) column, validate the query
// shape (single MATCH column; ORDER BY score() DESC; LIMIT N), synthesize the __bm25_score column
// into the scan's colRefToColumnMetaMap / columnMetaToColRefMap ONLY (never OlapTable.addColumn --
// that duplicates the shared table schema, cf. PR #74785), read k1/b from the session vars, and set
// LogicalOlapScanOperator.getBm25SearchOptions().
//
// Until then check() is gated off by default (Config.enable_experimental_bm25 == false) and
// transform() returns an empty list, so both the flag-off and flag-on plans are unchanged
// (Optimizer Rule Contract: return empty when the transform does not change the input).
public class RewriteToBM25PlanRule extends TransformationRule {

    public RewriteToBM25PlanRule() {
        super(RuleType.TF_BM25_REWRITE_RULE,
                Pattern.create(OperatorType.LOGICAL_TOPN)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        // Experimental and off by default: the rule never fires, so the plan is identical to today.
        return Config.enable_experimental_bm25;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        // TODO(bm25 / PR-C): implement the score() rewrite (see class comment). Return empty for now
        // so this reports no change.
        return List.of();
    }
}
