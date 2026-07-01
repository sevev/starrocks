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

package com.starrocks.common;

import com.starrocks.thrift.TBM25SearchOptions;

// FE-side BM25 scoring request, carried on LogicalOlapScanOperator and serialized into
// TBM25SearchOptions on the (olap/lake) scan node. Mirrors VectorSearchOptions. Populated by
// RewriteToBM25PlanRule when a zero-arg score() over a builtin GIN (DOCS_AND_FREQS) column is
// rewritten; otherwise left disabled so the plan and thrift are unchanged.
public class BM25SearchOptions {
    private boolean enableScore = false;
    // The MATCH_ANY / MATCH_ALL query string whose matching docs are scored.
    private String query = "";
    // The GIN-indexed column being scored.
    private int indexColumnId = -1;
    // Synthetic score column, e.g. "__bm25_score". Lives only in the scan's colRef maps, never in
    // the table schema (cf. RewriteToVectorPlanRule / PR #74785).
    private String scoreColumnName = "";
    private int scoreSlotId = -1;
    // BM25 free parameters; sourced from session vars bm25_k1 / bm25_b by the rewrite rule.
    private double k1 = 1.2;
    private double b = 0.75;

    public boolean isEnableScore() {
        return enableScore;
    }

    public void setEnableScore(boolean enableScore) {
        this.enableScore = enableScore;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public int getIndexColumnId() {
        return indexColumnId;
    }

    public void setIndexColumnId(int indexColumnId) {
        this.indexColumnId = indexColumnId;
    }

    public String getScoreColumnName() {
        return scoreColumnName;
    }

    public void setScoreColumnName(String scoreColumnName) {
        this.scoreColumnName = scoreColumnName;
    }

    public int getScoreSlotId() {
        return scoreSlotId;
    }

    public void setScoreSlotId(int scoreSlotId) {
        this.scoreSlotId = scoreSlotId;
    }

    public double getK1() {
        return k1;
    }

    public void setK1(double k1) {
        this.k1 = k1;
    }

    public double getB() {
        return b;
    }

    public void setB(double b) {
        this.b = b;
    }

    public TBM25SearchOptions toThrift() {
        TBM25SearchOptions opts = new TBM25SearchOptions();
        opts.setEnable(true);
        opts.setQuery(query);
        opts.setIndex_column_id(indexColumnId);
        opts.setScore_column_name(scoreColumnName);
        opts.setScore_slot_id(scoreSlotId);
        opts.setK1(k1);
        opts.setB(b);
        return opts;
    }

    public String getExplainString(String prefix) {
        return prefix + "BM25: ON" + "\n" +
                prefix + prefix +
                "Query: " + query + ", " +
                "Score Column: <" + scoreSlotId + ":" + scoreColumnName + ">, " +
                "k1: " + k1 + ", b: " + b +
                "\n";
    }
}
