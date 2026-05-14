# VI PR Staging — Progress Notes (2026-05-14, B1 + B3)

Branch: `vi-pr-staging` based on `upstream/main` @ `ea8ee91ec97`.

## Staged commits (PR submission order)

| # | PR | Commits | Tag |
|---|---|---|---|
| 1 | **B1** `feat/async-vi-buffer-cap` | `f0be7389e02` cherry-pick of `934f530` (vector_index_build_flush_threshold_rows config + builder call) | `pr-b1-end` |
| 2 | **B3** `feat/vi-quantization` | `940e602` BE parse + `3164d4f` FE params + `a973ed0` FE DDL validate + `29433db` SQL UT + `cf38e82` EN/ZH doc | `pr-b3-end` |

Total: **6 commits across 2 PRs**.

## B1 audit result

Original plan: 6 commits. Audit found **5 are already in upstream/main** via other OSS PRs:
- `c2c86f1` adaptive CPU + tablet dedup → already in lake_service.cpp / exec_env.cpp
- `92a2939` cross-CN dedup → already in com.starrocks.lake.vector.VectorIndexBuildScheduler
- `607bb85` cooldown 5min → already (DEDUP_COOLDOWN_MS in same class)
- `0e18d81` dynamic OMP config → achieved via adaptive sizing path on each RPC
- `64bac04` configurable warehouse → already (Config.java:3378, WarehouseManager.java:406)

Only **`934f530` buffer cap** is net-new.

## B3 audit result

3 cherry-picks (no upstream duplication) + 2 new commits:
- `42b35725` BE parse — applied with include-block + test-file conflicts resolved (concat strategy on test file)
- `a4f84d9` FE params — clean
- `64e6fcc` FE DDL validate — applied with trivial commented-out block conflict resolved
- NEW: SQL UT — happy paths + DDL rejection cases. **R/ result file is placeholder, needs `--record` run locally to capture expected output**
- NEW: EN/ZH doc — added quantizer / m_pq / nbits_pq sections after `efconstruction` in vector_index.md

## Open items needing user decision

1. **B3 SQL UT result file**: needs `--record` run. Decision: run locally and commit recorded output, or accept TODO as-is?
2. **B3 doc placement**: added under HNSW parameters after `efconstruction`. Verify discoverability is OK or if a dedicated "Quantization" subsection is preferred.
3. **B3 BE parse cherry-pick (`vector_index_test.cpp`) 2nd conflict resolution**: concatenated both HEAD and cherry-pick test sections. Review for potential test name collisions or scaffolding inconsistencies.

## Remaining PRs (status, not on this branch)

| # | PR | Source | Notes |
|---|---|---|---|
| E1-new | `feat/fe-prepared-vector-search` | cherry-pick `a5b8de0` | Removed from this staging branch on user request; can stage later if needed |
| E4 | `test/be-shared-data-vector-index-ut` | parts of WIP 2705cc1 | 3-way merge has 4 conflict hunks; upstream test file evolved 271 → 807 lines |
| D1 | `feat/sr-owned-vector-index-cache` | 9 ent commits atomic | Large; must apply as one logical PR |
| C1 | `enh/be-vector-cache-warm-path` | tenann pin bump + 33128ad + reader_factory short-circuit | Depends D1 merge |
| F1 | `feat/fe-vector-late-materialization` | working-tree FE + NEW readiness check + NEW UT | Needs new code (FE readiness check on LakeTablet.getVectorIndexBuiltVersion); depends E1-new merge |

## OSS PRs already opened (not on this branch)

- tenann/#8 — `enh/index-reader-timing-stats` (T2) OPEN
