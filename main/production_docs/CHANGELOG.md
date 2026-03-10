# Changelog — Summary Pipeline (main)

All notable changes to the **production main pipeline** (`main/summary_inc.py`) are documented here.  
Earlier version history (v4.0 → v9.4.8) is recorded in `updates/summary_v9_production/docs/CHANGELOG.md`.

---

## main (March 2026) — Soft Delete, Rollback & Safety Enhancements

### Summary

Major additions to the production pipeline building on the February rewrite:

- **Soft Delete Support** — full lifecycle handling for `soft_del_cd` codes `1` and `4` across all cases
- **Automatic Rollback on Failure** — pipeline failures trigger Iceberg snapshot rollback for both `summary` and `latest_summary`
- **Enhanced Watermark Tracker** — now tracks snapshot IDs, run_id, status (RUNNING/SUCCESS/FAILURE), and error messages
- **Bucketed Temp Tables** — case temp tables are now partitioned + bucketed (64 buckets by `cons_acct_key`) to align with summary for faster merges
- **Case II Chunked Merge** — Case II now uses chunked month-based merge (same as Case III) for large forward batches
- **hist_rpt_dt Integration** — `acct_dt` resolution via left join on `hist_rpt_dt_table` with soft-delete-aware prioritization
- **Case II Soft-Delete-Aware Context** — accounts whose `latest_summary` row is soft-deleted are treated as Case-I-style (fresh arrays) instead of inheriting deleted context
- **Case III Soft Delete Processing** — dedicated `process_case_iii_soft_delete()` nullifies the deleted month's position in all future rows' history arrays

---

## Feature: Soft Delete Support (March 2026)

### Overview

Full lifecycle handling for account corrections via `soft_del_cd` column. Delete codes `1` and `4` trigger special processing instead of standard case logic.

### Schema

A `soft_del_cd STRING` column is added to both `summary` and `latest_summary` tables. The pipeline calls `ensure_soft_delete_columns()` at startup to add it if missing.

### How It Works

| Phase | Soft Delete Behavior |
|-------|---------------------|
| `prepare_source_data()` | Joins with `hist_rpt_dt_table` to resolve `acct_dt` — if both source and hist row are deleted, `acct_dt = NULL`; otherwise the non-deleted or newest `acct_dt` wins |
| Classification | Records with `soft_del_cd IN ('1','4')` are flagged as `_is_soft_delete = true` |
| Case III deletes | Routed to `process_case_iii_soft_delete()` instead of `process_case_iii()` |
| Case I/IV deletes | Filtered out — soft-deleted new accounts are not written |
| Case II deletes | Filtered out — only non-deleted records generate forward entries |
| Case II context | `latest_summary` rows with delete codes are excluded from the join context; affected accounts get Case-I-style fresh arrays |

### Case III Soft Delete Processing (`process_case_iii_soft_delete`)

Two-part operation:

1. **Part A: Month-row update** — writes `soft_del_cd` + `base_ts` to `case_3d_month` temp table
2. **Part B: Future array nullification** — for each future month row whose history array references the deleted position, sets that position to NULL. Writes to `case_3d_future` temp table.

### New Temp Tables

| Table | Case | Purpose |
|-------|------|---------|
| `temp_catalog.checkpointdb.case_3d_month` | III-D | Month-row soft_del_cd update |
| `temp_catalog.checkpointdb.case_3d_future` | III-D | Future array position nullification |

---

## Feature: Automatic Rollback on Failure (March 2026)

### Overview

When the pipeline fails mid-run, it now **automatically rolls back** both `summary` and `latest_summary` to their pre-run Iceberg snapshots.

### How It Works

```python
# In run_pipeline() exception handler:
rollback_statuses = rollback_tables_to_run_start(spark, config, start_states)
finalize_run_tracking(spark, config, run_id, start_states, success=False, error_message=...)
```

1. `log_current_snapshot_state()` captures snapshot IDs for both tables **before** processing
2. `mark_run_started()` writes RUNNING status to tracker with pre-run snapshot IDs
3. On success: `finalize_run_tracking(success=True)` advances watermark
4. On failure: `rollback_tables_to_run_start()` calls `CALL system.rollback_to_snapshot(...)` for each table, then `finalize_run_tracking(success=False)` records the error

### Tracker Schema (Enhanced)

```sql
CREATE TABLE watermark_tracker (
  source_name                      STRING,    -- 'summary' | 'latest_summary' | 'committed_ingestion_watermark'
  source_table                     STRING,
  max_base_ts                      TIMESTAMP,
  max_rpt_as_of_mo                 STRING,
  updated_at                       TIMESTAMP,
  previous_successful_snapshot_id  BIGINT,    -- NEW: last known-good snapshot
  current_snapshot_id              BIGINT,    -- NEW: current run's snapshot
  status                           STRING,    -- NEW: 'RUNNING' | 'SUCCESS' | 'FAILURE'
  run_id                           STRING,    -- NEW: unique per-run identifier
  error_message                    STRING     -- NEW: failure details (truncated to 500 chars)
) USING iceberg
```

---

## Feature: Bucketed Temp Tables (March 2026)

### Overview

All case temp tables are now written with `write_case_table_bucketed()` — partitioned by `rpt_as_of_mo` and bucketed into 64 buckets by `cons_acct_key`, matching the summary table layout.

```python
write_case_table_bucketed(spark, df, table_name, config, stage, expected_rows)
```

### Table Properties

```sql
PARTITIONED BY (rpt_as_of_mo, bucket(64, cons_acct_key))
TBLPROPERTIES (
    'write.distribution-mode' = 'hash',
    'write.merge.distribution-mode' = 'hash'
)
```

### Merge Alignment

`align_for_summary_merge()` repartitions and sorts the merge source to match the summary table's distribution before each MERGE, reducing shuffle during the merge join.

---

## Feature: Case II Chunked Merge (March 2026)

### Overview

Case II (forward entries) now uses the same month-chunked merge strategy as Case III for large batches:

```python
case_2_chunks = build_month_chunks_from_df(case_2_merge_df, prt, overflow_ratio=0.10)
```

Each chunk is merged independently, preventing oversized Iceberg commits that could cause memory pressure on large forward batches.

---

## Feature: hist_rpt_dt Integration (March 2026)

### Overview

`prepare_source_data()` now joins with a separate `hist_rpt_dt_table` (configured via `hist_rpt_dt_table` and `hist_rpt_dt_cols` in config) to resolve `acct_dt` when soft-delete corrections arrive from different source systems.

### Priority Logic

```sql
CASE
  WHEN both source AND hist are soft-deleted → acct_dt = NULL
  WHEN source is NOT deleted (and hist is, or source is newer) → use source acct_dt
  WHEN hist is NOT deleted (and source is, or hist is newer)   → use hist acct_dt
  ELSE use source acct_dt
END
```

## main (February 2026) — Production Rewrite

### Summary

A ground-up production hardening of v9.4.8, incorporating all accumulated bug fixes and architectural improvements, with several major additions:

- **Watermark Tracker** — idempotent incremental runs via committed watermark state
- **Per-Case Temp Tables** — dedicated `temp_catalog.checkpointdb.case_*` tables instead of a single pooled temp table
- **Chunked Case III Merge** — balanced bin-packing for large backfill commits
- **Source Window Filter** — strict upper/lower bound on `rpt_as_of_mo` to prevent re-processing closed partitions
- **Pre-flight Validation** — asserts `summary.max_month == latest_summary.max_month` before any processing

---

## Patch: Case III-B Timestamp Propagation

### Feature

Enhanced `base_ts` update logic for Case III-B (future row updates). The update now carries:

```python
GREATEST(existing_summary_ts, new_base_ts)
```

applied to **ALL** future rows affected by the backfill. This replaces earlier behaviour where only the current-month row (index 0) received a `base_ts` update.

#### Why This Matters

When a backfill is applied, each future summary row's history array is modified with the new data. The `base_ts` on those rows should reflect that they have been updated with newer source data — but it must never go *backwards* (e.g., a stale retry should not reduce a timestamp).

```
Before fix:
  Future row (Jan 2026): base_ts = 2025-12-01  ← unchanged after backfill
  Future row (Feb 2026): base_ts = 2026-01-15  ← unchanged after backfill

After fix:
  Backfill base_ts = 2026-02-01 (newer source)
  Future row (Jan 2026): base_ts = GREATEST(2025-12-01, 2026-02-01) = 2026-02-01 ✓
  Future row (Feb 2026): base_ts = GREATEST(2026-01-15, 2026-02-01) = 2026-02-01 ✓
```

---

## Patch: Case III-B Merge Correctness Fix

### Bug Fix

Replaced wildcard merge update (`UPDATE SET *`) for `case_3b` with **explicit column-level updates**.

#### Problem (`UPDATE SET *` — BROKEN)

```sql
-- Old merge for case_3b
MERGE INTO summary s USING case_3b c
ON s.cons_acct_key = c.cons_acct_key AND s.rpt_as_of_mo = c.rpt_as_of_mo
WHEN MATCHED THEN UPDATE SET *   -- ← overwrites ALL columns
```

Case III-B only needs to update rolling history and `base_ts`. Using `UPDATE SET *` would overwrite scalar columns (balances, dates, codes) with the backfill month's values — corrupting future rows that have their own correct scalar values.

#### Solution — Explicit Updates

```sql
MERGE INTO summary s USING case_3b c
ON s.cons_acct_key = c.cons_acct_key AND s.rpt_as_of_mo = c.rpt_as_of_mo
WHEN MATCHED THEN UPDATE SET
  s.base_ts                      = GREATEST(s.base_ts, c.base_ts),
  s.balance_am_history           = c.balance_am_history,
  s.actual_payment_am_history    = c.actual_payment_am_history,
  s.credit_limit_am_history      = c.credit_limit_am_history,
  s.past_due_am_history          = c.past_due_am_history,
  s.payment_rating_cd_history    = c.payment_rating_cd_history,
  s.days_past_due_history        = c.days_past_due_history,
  s.asset_class_cd_4in_history   = c.asset_class_cd_4in_history,
  s.payment_history_grid         = c.payment_history_grid
  -- Scalar columns (balance_am, acct_stat_cd, etc.) are NOT touched
```

#### Scope

Only the following column types are updated in Case III-B:
- `base_ts` (with GREATEST guard)
- All `*_history` rolling array columns
- All configured `grid_columns`

---

## Feature: Watermark Tracker

### Overview

A new Iceberg table (`watermark_tracker`) tracks committed pipeline state, enabling safe incremental reruns.

### Why Needed

Without a tracker, a failed mid-run would leave the `summary` table in a partially-updated state. On the next run, the pipeline used `MAX(base_ts)` from `summary` as the source filter, which could miss records that were not yet committed to `summary` but were already in `latest_summary` — leading to data gaps.

### How It Works

```
Source filter watermark = committed_ingestion_watermark.max_base_ts
                           (MIN of summary.max_base_ts and latest_summary.max_base_ts)

Committed watermark only advances after:
  ✓ All case processing complete
  ✓ All merges to summary complete
  ✓ All merges to latest_summary complete
  ✓ refresh_watermark_tracker(mark_committed=True) called
```

### Tracker Schema

```sql
CREATE TABLE watermark_tracker (
  source_name       STRING,    -- 'summary' | 'latest_summary' | 'committed_ingestion_watermark'
  source_table      STRING,
  max_base_ts       TIMESTAMP,
  max_rpt_as_of_mo  STRING,
  updated_at        TIMESTAMP
) USING iceberg
```

---

## Feature: Per-Case Temp Tables (Architecture Change from v9.4)

### Overview

v9.4 used a single pooled temp table (`pipeline_batch_{ts}_{uuid}`) with a `_case_type` column.  
The main pipeline uses **dedicated temp tables per case**:

| Temp Table | Case |
|------------|------|
| `temp_catalog.checkpointdb.case_1` | Case I — New Accounts |
| `temp_catalog.checkpointdb.case_2` | Case II — Forward Entries |
| `temp_catalog.checkpointdb.case_3a` | Case III-A — New backfill rows |
| `temp_catalog.checkpointdb.case_3b` | Case III-B — Future row updates |
| `temp_catalog.checkpointdb.case_iv` | Case IV — Bulk Historical |

### Benefits

- **Selective reads**: write phase can read only the cases it needs
- **Simpler schema**: no `_case_type` column needed; each table has the exact schema it requires
- **case_3b filtering**: `write_backfill_results` filters `case_3b` to exclude rows already handled by `case_3a` (`left_anti` join), preventing duplicate updates
- **Rerun-safe**: `cleanup()` drops all `checkpointdb.*` tables at run start

---

## Feature: Chunked Case III Merge

### Overview

Case III merges are split into **balanced month-chunks** to avoid Iceberg commit pressure on large backfill batches.

### Algorithm (Greedy Bin-Packing)

```python
weighted_load = case3a_count + (case3b_weight × case3b_count)
chunks = build_balanced_month_chunks(month_weights, overflow_ratio=0.10)
```

- `case3b_weight` default: 1.3 (configurable via `case3_merge_case3b_weight`)
- `overflow_ratio` default: 10% (configurable via `case3_merge_overflow_ratio`)

Each chunk is merged independently in sequence, so large backfill batches do not create a single oversized Iceberg commit.

---

## Feature: Source Window Filter

### Overview

The source read now strictly bounds `rpt_as_of_mo`:

```python
rpt_as_of_mo >= min_month_destination   # oldest partition already in summary
rpt_as_of_mo <  next_month              # one month beyond latest in summary
base_ts       >  effective_max_base_ts  # only pick up new/updated records
```

This prevents re-reading old closed partitions that were fully processed in earlier runs, reducing source I/O significantly for large tables.

---

## Feature: Pre-flight Validation

```python
if max_month_destination != max_month_latest_history:
    raise ValueError(...)
```

Guards against a split-brain state where `summary` and `latest_summary` have diverged (e.g., from a partial write failure). The pipeline will not proceed until both tables agree on the latest month.

---

## Inherited Fixes (from v9.4.x — fully carried into main)

All patches from the v9.4.x series are incorporated:

| Version | Fix |
|---------|-----|
| v9.4.5 | NULL value handling in Case III-B: replaced `COALESCE` with `filter + element_at` |
| v9.4.5 | NULL value handling in Case III-A: explicit `peer_map[key] IS NOT NULL` check |
| v9.4.4 | Current-month (index 0) now correctly updated via Case III-B |
| v9.4.3 | Backfill chaining fixed for accounts with no prior history |
| v9.4.3 | Double/Decimal column type support in aggregation |
| v9.4.2 | Chained backfill using Windowed Map-Lookup (`peer_map`) |
| v9.4.2 | Multi-forward chaining for Case II |
| v9.4.1 | Multiple backfills in same batch now all propagated to future rows |
| v9.4.1 | Type-aware accumulator (STRING vs BIGINT) for `aggregate()` |
| v9.3 | Case III partition pruning (50–90% I/O reduction) |
| v9.3 | Case II filter pushdown (Iceberg data-file pruning) |
| v9.2.1 | Case IV gap handling via `MAP_FROM_ENTRIES + TRANSFORM` |
| v9.2 | Case IV detection and processing |
| v9.1 | Case III row creation for backfill month |

---

## Open Items / Suggested Improvements

| Priority | Item |
|----------|------|
| P0 | Make Case I `case_i_result` lifecycle safe if `case_iv_records == 0` — currently may reference undefined variable |
| P1 | Avoid `count()` existence checks — use `tableExists()` already done, but count() is still called in some stats paths |
| P1 | Add explicit performance thresholds to benchmark tests |
| P2 | Run-scoped temp table names as an additional safety layer |
