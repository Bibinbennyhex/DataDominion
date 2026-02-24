# Test Cases — Summary Pipeline (main)

**Pipeline Version**: main (post v9.4.8)
**Test Framework**: Python + PySpark (Docker Iceberg)
**Last Updated**: February 2026

---

## Table of Contents

1. [Test Infrastructure](#1-test-infrastructure)
2. [Test Suite Overview](#2-test-suite-overview)
3. [Case I — New Accounts](#3-case-i--new-accounts)
4. [Case II — Forward Entries](#4-case-ii--forward-entries)
5. [Case III — Backfill](#5-case-iii--backfill)
6. [Case IV — Bulk Historical Load](#6-case-iv--bulk-historical-load)
7. [Cross-Case Integration Test](#7-cross-case-integration-test)
8. [Idempotency Tests](#8-idempotency-tests)
9. [Watermark Tracker Tests](#9-watermark-tracker-tests)
10. [latest_summary Consistency Tests](#10-latest_summary-consistency-tests)
11. [Edge Case & Regression Tests](#11-edge-case--regression-tests)
12. [Scenario Suite](#12-scenario-suite)
13. [Performance Benchmarks](#13-performance-benchmarks)
14. [Running Tests](#14-running-tests)
15. [Test Result Summary](#15-test-result-summary)

---

## 1. Test Infrastructure

### 1.1 Environment

| Component | Detail |
|-----------|--------|
| Container | Docker (spark-iceberg-main) |
| Spark | 3.5+ with Iceberg extensions |
| Catalog | Local Hadoop Iceberg catalog |
| Test Runner | Python scripts (no pytest — direct assertions) |
| Report | Optional HTML via `run_retest_with_report.ps1` |

### 1.2 Data Isolation

Each test suite uses its own Iceberg namespace to prevent cross-test contamination:

| Suite | Namespace |
|-------|-----------|
| `test_main_all_cases` | `demo.main_test` |
| `test_main_base_ts_propagation` | `demo.main_test_base_ts` |
| `test_idempotency` | `demo.main_idempotency` |
| `test_latest_summary_consistency` | `demo.main_latest_consistency` |
| scenario_suite | `demo.scenario_*` (per-scenario) |

### 1.3 Test Lifecycle

Every test follows this lifecycle:

```
1. reset_tables(spark, config)     → DROP + CREATE all tables in test namespace
2. Seed summary + latest_summary   → write_summary_rows() with known initial state
3. Write source batch              → write_source_rows() with test-specific records
4. Run pipeline                    → main_pipeline.cleanup() + run_pipeline()
5. Assert outputs                  → fetch_single_row() + field-level assertions
6. Assert watermark                → assert_watermark_tracker_consistent()
```

### 1.4 Test Utility Functions

| Function | Purpose |
|----------|---------|
| `create_spark_session(name)` | Create a local Spark session with Iceberg support |
| `load_main_test_config(namespace)` | Load `config.json` with test namespace override |
| `reset_tables(spark, config)` | DROP and recreate all tables (summary, latest_summary, watermark_tracker, source) |
| `build_summary_row(...)` | Construct a complete summary row with all 7 history arrays + scalars |
| `build_source_row(...)` | Construct a source row matching accounts_all schema |
| `history({pos: val, ...})` | Build a 36-element array with specified positions set, rest NULL |
| `write_summary_rows(spark, table, rows)` | Write rows to any Iceberg table |
| `write_source_rows(spark, table, rows)` | Write source records to accounts_all |
| `fetch_single_row(spark, table, acct, month)` | Retrieve exactly one row by PK + partition |
| `assert_watermark_tracker_consistent(spark, config)` | Validate tracker state (see §9) |

---

## 2. Test Suite Overview

| File | Tests | Category |
|------|-------|----------|
| `test_main_all_cases.py` | 10 assertions | Integration — all four cases in one batch |
| `test_main_base_ts_propagation.py` | 4 assertions | Regression — Case III-B base_ts GREATEST logic |
| `test_idempotency.py` | 6 assertions | Regression — rerun same batch → no data drift |
| `test_aggressive_idempotency.py` | ~20 assertions | E2E — multiple reruns with all cases, no drift |
| `test_latest_summary_consistency.py` | 2 assertions | Regression — latest_summary ↔ summary sync after backfill |
| `test_case3_current_max_month.py` | ~8 assertions | Edge case — backfill at current max month boundary |
| `test_consecutive_backfill.py` | — | Chained backfill — multiple months same batch |
| `test_non_continuous_backfill.py` | — | Gaps in backfill months |
| `test_long_backfill_gaps.py` | — | Very old backfill (>12 months ago) |
| `test_null_update_case_iii.py` | — | NULL propagation in Case III history arrays |
| `test_null_update_other_cases.py` | — | NULL propagation in Case I, II, IV |
| `test_performance_benchmark.py` | — | Runtime thresholds at TINY/SMALL/MEDIUM scale |
| `scenario_suite.py` (via wrappers) | ~50+ scenarios | Comprehensive scenario coverage |

---

## 3. Case I — New Accounts

### 3.1 Definition

- Account does **NOT** exist in `latest_summary`
- Only **ONE** month of data in current batch (or earliest month of a multi-month new account)

### 3.2 Test Scenario

**Account 1001 — brand new, single month entry**

```
Pre-existing state:  NONE (account not in summary or latest_summary)

Incoming source:
  Account: 1001
  Month:   2026-01
  base_ts: 2026-02-01 12:00:00
  balance: 3000
  actual_payment: 300
```

### 3.3 Expected Output

```
summary row created:
  cons_acct_key:          1001
  rpt_as_of_mo:           2026-01
  base_ts:                2026-02-01 12:00:00
  balance_am_history:     [3000, NULL, NULL, ..., NULL]   ← 36 elements
  actual_payment_am_history: [300, NULL, NULL, ..., NULL]

latest_summary row created:
  cons_acct_key:          1001
  rpt_as_of_mo:           2026-01
  (mirrors the summary row)
```

### 3.4 Array Construction Logic

```python
# For each rolling column:
array([current_value]) + array_repeat(NULL, 35)

# Example:
balance_am_history = [3000] + [NULL × 35] = [3000, NULL, NULL, ..., NULL]
```

### 3.5 Key Assertions

| # | Field | Expected | Why |
|---|-------|----------|-----|
| 1 | `balance_am_history[0]` | 3000 | Current month's value at position 0 |
| 2 | `balance_am_history[1]` | NULL | No prior history for new account |
| 3 | `len(balance_am_history)` | 36 | Fixed window size |
| 4 | `latest_summary` row exists | ✓ | New account must be registered |
| 5 | `latest_summary.rpt_as_of_mo` | 2026-01 | Latest month correct |

### 3.6 Edge Cases Tested

| Scenario | Test File | Detail |
|----------|-----------|--------|
| Single month new account | `test_main_all_cases.py` | Account 1001 |
| Earliest month of multi-month new account | `test_main_all_cases.py` | Account 4001 (seed row = Case I, subsequent = Case IV) |
| NULL values in source | `test_null_update_other_cases.py` | New account where some columns are NULL |
| All 46 columns populated | `test_full_46_columns.py` | Validates every column in output |

---

## 4. Case II — Forward Entries

### 4.1 Definition

- Account **EXISTS** in `latest_summary`
- New `month > latest month` (forward in time)

### 4.2 Test Scenario

**Account 2001 — existing account, one month forward**

```
Pre-existing state (summary + latest_summary):
  Account: 2001
  Latest:  2025-12
  base_ts: 2026-01-01 00:00:00
  balance_am_history:     [5000, 4500, NULL, ..., NULL]
  actual_payment_am_history: [500, 450, NULL, ..., NULL]

Incoming source:
  Account: 2001
  Month:   2026-01          ← one month forward (MONTH_DIFF = 1)
  base_ts: 2026-02-01 12:00:00
  balance: 5200
  actual_payment: 520
```

### 4.3 Expected Output

```
summary row (2026-01):
  balance_am_history:     [5200, 5000, 4500, NULL, ..., NULL]
                           ▲     ▲     ▲
                           │     │     └── Position 2: prior[1] shifted right
                           │     └── Position 1: prior[0] shifted right
                           └── Position 0: new month's value

latest_summary updated:
  rpt_as_of_mo:            2026-01
  balance_am_history[0]:   5200
```

### 4.4 Array Shift Logic

```
MONTH_DIFF = 1 (consecutive):
  new_array = [new_value] + old_array[0:35]    (left-shift by 1)

MONTH_DIFF = 3 (gap):
  new_array = [new_value, NULL, NULL] + old_array[0:33]    (insert NULLs for gap)
```

### 4.5 Key Assertions

| # | Field | Expected | Why |
|---|-------|----------|-----|
| 1 | `balance_am_history[0]` | 5200 | New month's value at position 0 |
| 2 | `balance_am_history[1]` | 5000 | Previous month shifted to position 1 |
| 3 | `latest_summary.rpt_as_of_mo` | 2026-01 | Latest_summary advanced |
| 4 | `base_ts` | 2026-02-01 12:00 | Newer timestamp wins |

### 4.6 Merge Guard

```sql
WHEN MATCHED AND c.base_ts >= s.base_ts THEN UPDATE SET *
```

If a stale source record arrives (lower `base_ts`), it is silently dropped — the summary is not overwritten.

### 4.7 Edge Cases Tested

| Scenario | Test File | Detail |
|----------|-----------|--------|
| Consecutive forward (MONTH_DIFF = 1) | `test_main_all_cases.py` | Account 2001 |
| Gap forward (MONTH_DIFF = 3) | `scenario_suite.py` | NULLs inserted for missing months |
| Large gap (MONTH_DIFF = 6) | `scenario_suite.py` | Half-year gap handling |
| Duplicate forward records (same month, different base_ts) | `test_duplicate_records.py` | Higher base_ts wins dedup |
| Forward with NULL columns | `test_null_update_other_cases.py` | NULL values propagated correctly |
| Stale record (base_ts < existing) | `scenario_suite.py` | Merge guard blocks overwrite |

---

## 5. Case III — Backfill

### 5.1 Definition

- Account **EXISTS** in `latest_summary`
- New `month ≤ latest month` (late-arriving historical data)

### 5.2 Test Scenario: Basic Backfill

**Account 3001 — backfill December when January already exists**

```
Pre-existing state (summary + latest_summary):
  Account: 3001
  Latest:  2026-01
  base_ts: 2026-01-01 00:00:00
  balance_am_history:     [4700, 4600, NULL, ..., NULL]

Incoming source:
  Account: 3001
  Month:   2025-12          ← one month back (MONTH_DIFF = -1)
  base_ts: 2026-02-01 12:00:00
  balance: 4800
```

### 5.3 Expected Output — Two-Part Write

**Part A (CASE III-A): New backfill row created**

```
summary row (2025-12):
  balance_am_history:     [4800, NULL, NULL, ..., NULL]
                           ▲
                           └── Backfill value at position 0
```

**Part B (CASE III-B): Future row (2026-01) patched**

```
summary row (2026-01) AFTER patch:
  balance_am_history:     [4700, 4800, NULL, ..., NULL]
                           ▲     ▲
                           │     └── Position 1: PATCHED with backfill value
                           └── Position 0: unchanged (January's own value)

  base_ts:                2026-02-01 12:00:00   ← GREATEST(old_ts, new_ts)
```

### 5.4 Key Assertions — Basic Backfill

| # | Month | Field | Expected | Why |
|---|-------|-------|----------|-----|
| 1 | 2025-12 | `balance_am_history[0]` | 4800 | III-A: backfill row created with correct value |
| 2 | 2026-01 | `balance_am_history[1]` | 4800 | III-B: future row patched at correct index |
| 3 | 2026-01 | `base_ts` | 2026-02-01 12:00 | III-B: GREATEST propagated newer ts |

### 5.5 Test Scenario: base_ts Propagation (Regression)

**Account 5001 — backfill with newer timestamp**

```
Pre-existing state:
  2025-12: base_ts = 2026-01-05 08:00, balance_history = [5000, NULL...]
  2026-01: base_ts = 2026-01-05 08:00, balance_history = [5100, 5000, NULL...]

Incoming source:
  Account: 5001
  Month:   2025-12
  base_ts: 2026-02-10 09:30:00       ← NEWER than existing
  balance: 4900
```

**Expected:**

| # | Month | Field | Expected | Why |
|---|-------|-------|----------|-----|
| 1 | 2025-12 | `balance_am_history[0]` | 4900 | Backfill row overwritten with new value |
| 2 | 2026-01 | `balance_am_history[1]` | 4900 | Future row patched at position 1 |
| 3 | 2026-01 | `base_ts` | 2026-02-10 09:30 | **GREATEST**(2026-01-05, 2026-02-10) = newer ts |

> **Key invariant:** `base_ts` only moves forward, never backwards. This is enforced by `GREATEST(existing_ts, new_base_ts)` in the merge.

### 5.6 Test Scenario: Consecutive Backfill (Peer Map)

**Multiple backfill months for same account in one batch:**

```
Account X, latest month = June 2025

Batch:
  March 2025: balance = 3000
  April 2025: balance = 4000
  May 2025:   balance = 5000

Expected June row after patch:
  balance_am_history = [June_val, 5000, 4000, 3000, ...]
                        pos 0     pos 1  pos 2  pos 3

Expected May row (built via peer_map):
  balance_am_history = [5000, 4000, 3000, ...]
                        pos 0  pos 1  pos 2
  (April and March filled from peer_map, not self-join)
```

### 5.7 CASE III-B Explicit Column Updates

The merge for Case III-B uses **explicit column updates**, NOT `UPDATE SET *`:

```
Columns UPDATED:
  - base_ts              → GREATEST(existing, new)
  - *_history arrays     → patched at calculated position
  - payment_history_grid → rebuilt from patched arrays

Columns NOT UPDATED:
  - All scalar columns (balance, actual_payment, credit_limit, etc.)
  - rpt_as_of_mo, cons_acct_key (partition key + primary key)
```

This prevents corruption of scalar columns that belong to the future row's own month.

### 5.8 Edge Cases Tested

| Scenario | Test File | Detail |
|----------|-----------|--------|
| Single month backfill | `test_main_all_cases.py` | Account 3001 |
| base_ts propagation | `test_main_base_ts_propagation.py` | Account 5001 — GREATEST logic |
| Backfill at current max month | `test_case3_current_max_month.py` | Boundary where month == latest |
| Consecutive months (peer_map) | `test_consecutive_backfill.py` | Chained backfill, gap filling |
| Non-continuous months | `test_non_continuous_backfill.py` | Gaps between backfill months |
| Very old backfill (>12 months) | `test_long_backfill_gaps.py` | Deep history patching |
| NULL in backfill value | `test_null_update_case_iii.py` | NULL correctly placed in history |
| latest_summary sync after backfill | `test_latest_summary_consistency.py` | Ensures latest matches summary |
| Backfill + forward in same batch | `test_aggressive_idempotency.py` | Mixed case batch |

---

## 6. Case IV — Bulk Historical Load

### 6.1 Definition

- Account does **NOT** exist in `latest_summary`
- **Multiple** months of data arrive for this account in the same batch

### 6.2 Test Scenario

**Account 4001 — new account with 2 months**

```
Pre-existing state:  NONE (account not in summary or latest_summary)

Incoming source:
  Account: 4001, Month: 2025-12, base_ts: 2026-02-01 12:00, balance: 7000
  Account: 4001, Month: 2026-01, base_ts: 2026-02-01 12:00, balance: 6800
```

### 6.3 Processing Order

```
1. Earliest month (2025-12) → classified as CASE I → seed row
2. Subsequent month (2026-01) → classified as CASE IV → MAP_FROM_ENTRIES build
```

### 6.4 Expected Output

```
summary row (2025-12) — from Case I:
  balance_am_history:     [7000, NULL, NULL, ..., NULL]

summary row (2026-01) — from Case IV:
  balance_am_history:     [6800, 7000, NULL, ..., NULL]
                           ▲     ▲
                           │     └── Position 1: MAP lookup → Dec value
                           └── Position 0: Jan's own value

latest_summary:
  rpt_as_of_mo: 2026-01   (latest month)
```

### 6.5 Key Assertions

| # | Month | Field | Expected | Why |
|---|-------|-------|----------|-----|
| 1 | 2026-01 | `balance_am_history[0]` | 6800 | Current month's value |
| 2 | 2026-01 | `balance_am_history[1]` | 7000 | Previous month via MAP lookup |
| 3 | `latest_summary` | `rpt_as_of_mo` | 2026-01 | Latest month registered |

### 6.6 MAP_FROM_ENTRIES Logic

```python
# Why MAP, not COLLECT_LIST:
# COLLECT_LIST skips gaps → wrong array positions
# MAP_FROM_ENTRIES assigns by month_int key → missing months = NULL

# For each position 0..35 in the history array:
history[i] = MAP.get(current_month_int - i, NULL)

# Example for Jan 2026 (month_int = 24313):
# history[0] = MAP[24313] = 6800 (Jan's own value)
# history[1] = MAP[24312] = 7000 (Dec's value from batch)
# history[2] = MAP[24311] = NULL  (Nov not in batch)
```

### 6.7 Edge Cases Tested

| Scenario | Test File | Detail |
|----------|-----------|--------|
| 2 months | `test_main_all_cases.py` | Account 4001 |
| 6 months | `test_bulk_historical_load.py` | Half-year upload |
| 12 months | `test_bulk_historical_load.py` | Full year upload |
| 36 months | `test_bulk_historical_load.py` | Full window upload |
| Gaps in bulk load | `scenario_suite.py` | Non-consecutive months |
| All 46 columns | `test_full_46_columns.py` | Complete column validation |
| NULL in bulk values | `test_null_update_other_cases.py` | NULL propagated through MAP |

---

## 7. Cross-Case Integration Test

### 7.1 Purpose

`test_main_all_cases.py` — Integration smoke test exercising all four cases simultaneously in a single pipeline run with a single mixed batch.

### 7.2 Test Accounts

| Account | Case | Pre-existing State |
|---------|------|--------------------|
| `1001` | I — new, single month | Not in summary |
| `2001` | II — forward entry | Latest: 2025-12, balance=[5000, 4500, ...] |
| `3001` | III — backfill | Latest: 2026-01, balance=[4700, 4600, ...] |
| `4001` | IV — new, 2 months | Not in summary |

### 7.3 Batch Composition

| Account | Month | base_ts | balance | actual_payment |
|---------|-------|---------|---------|----------------|
| 1001 | 2026-01 | 2026-02-01 12:00 | 3000 | 300 |
| 2001 | 2026-01 | 2026-02-01 12:00 | 5200 | 520 |
| 3001 | 2025-12 | 2026-02-01 12:00 | 4800 | 480 |
| 4001 | 2025-12 | 2026-02-01 12:00 | 7000 | 700 |
| 4001 | 2026-01 | 2026-02-01 12:00 | 6800 | 680 |

### 7.4 All 10 Assertions

| # | Account | Target | Field | Expected | Case Validated |
|---|---------|--------|-------|----------|----------------|
| 1 | 2001 | summary 2026-01 | `balance_am_history[0]` | 5200 | II — current value at pos 0 |
| 2 | 2001 | summary 2026-01 | `balance_am_history[1]` | 5000 | II — previous month shifted |
| 3 | 3001 | summary 2025-12 | `balance_am_history[0]` | 4800 | III-A — backfill row created |
| 4 | 3001 | summary 2026-01 | `balance_am_history[1]` | 4800 | III-B — future row patched |
| 5 | 3001 | summary 2026-01 | `base_ts` | 2026-02-01 12:00 | III-B — base_ts propagated |
| 6 | 4001 | summary 2026-01 | `balance_am_history[0]` | 6800 | IV — current month value |
| 7 | 4001 | summary 2026-01 | `balance_am_history[1]` | 7000 | IV — previous month via MAP |
| 8 | 2001 | latest_summary | `rpt_as_of_mo` | 2026-01 | II — latest_summary updated |
| 9 | 4001 | latest_summary | `rpt_as_of_mo` | 2026-01 | IV — latest_summary updated |
| 10 | — | watermark_tracker | consistent | ✓ | Watermark committed |

### 7.5 How to Run

```bash
docker compose -f main/docker_test/docker-compose.yml exec spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/test_main_all_cases.py
```

---

## 8. Idempotency Tests

### 8.1 Purpose

Verify that running the **same batch twice** produces identical outputs. Guards against double-processing, watermark regression, or duplicate row creation.

### 8.2 Procedure

```
1. reset_tables()
2. Seed summary + latest_summary with initial state
3. Write mixed source batch (Case I/II/III/IV — same as §7)
4. Run pipeline → snapshot summary + latest_summary (JSON serialization)
5. Run pipeline AGAIN with same source batch (no new source data)
6. Snapshot again → compare
```

### 8.3 How Idempotency Works

```
Run 1: Processes batch → writes to summary → advances watermark to base_ts = 2026-02-01 12:00

Run 2: Applies source filter: base_ts > 2026-02-01 12:00
       → Zero records match → zero records classified → no writes → tables unchanged
```

### 8.4 Assertions (6 total)

| # | Assertion | Expected |
|---|-----------|----------|
| 1 | `summary` row count after run 2 | == row count after run 1 |
| 2 | `latest_summary` row count after run 2 | == row count after run 1 |
| 3 | `summary` data snapshot (full JSON comparison) | Byte-identical to run 1 |
| 4 | `latest_summary` data snapshot | Byte-identical to run 1 |
| 5 | `latest_summary` — 1 row per account (run 1) | 0 duplicate accounts |
| 6 | `latest_summary` — 1 row per account (run 2) | 0 duplicate accounts |

### 8.5 Aggressive Idempotency (test_aggressive_idempotency.py)

More aggressive variant with:
- **3+ sequential reruns** (not just 2)
- Mixed batch including backfill + forward for the same account
- Multiple backfill months per account
- ~20 assertions per round
- Validates no drift in any column across all runs

### 8.6 How to Run

```bash
# Standard idempotency
docker compose -f main/docker_test/docker-compose.yml exec spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/test_idempotency.py

# Aggressive idempotency
docker compose -f main/docker_test/docker-compose.yml exec spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/test_aggressive_idempotency.py
```

---

## 9. Watermark Tracker Tests

### 9.1 Validation Helper

Every test suite calls `assert_watermark_tracker_consistent(spark, config)` after each pipeline run:

```python
# 1. Tracker row exists for 'committed_ingestion_watermark'
assert committed_row is not None

# 2. committed.max_base_ts == MIN(summary.max_base_ts, latest_summary.max_base_ts)
assert committed_ts == min(summary_max_ts, latest_max_ts)

# 3. committed.max_rpt_as_of_mo == MIN(summary.max_month, latest_summary.max_month)
assert committed_month == min(summary_max_month, latest_max_month)
```

### 9.2 Why MIN?

The committed watermark is the **conservative guard**: it uses `MIN` of both tables. If `summary` was updated but `latest_summary` was not (partial write), the watermark won't advance past the incomplete table.

### 9.3 Failure Semantics

| Scenario | Watermark Behaviour |
|----------|-------------------|
| Pipeline completes successfully | Watermark advances to `MIN(both tables)` |
| Pipeline fails midway | Watermark NOT updated (stays at prior value) |
| Rerun after failure | Source filter picks up same records → idempotent |
| Tracker assertion fails | Indicates partial write or logic bug — **P0** |

### 9.4 Tracker Schema Rows

| source_name | source_table | Tracks |
|-------------|-------------|--------|
| `summary` | FQDN of summary | MAX(base_ts) from summary table |
| `latest_summary` | FQDN of latest_summary | MAX(base_ts) from latest_summary |
| `committed_ingestion_watermark` | `N/A` | MIN of the above two — used for source filtering |

---

## 10. latest_summary Consistency Tests

### 10.1 Purpose

`test_latest_summary_consistency.py` — Verifies that `latest_summary` exactly matches the true latest rows from `summary` after a **backfill-only** batch. This is critical because Case III modifies existing summary rows in place, and `latest_summary` must reflect the updated state.

### 10.2 Test Scenario

```
Account 5001:
  Existing summary: Dec 2025 + Jan 2026
  Backfill batch:   Dec 2025 with newer balance (4900) and newer base_ts

Expected:
  - Jan 2026 row patched (balance_am_history[1] = 4900, base_ts updated)
  - latest_summary must reflect the patched Jan 2026 row
```

### 10.3 Consistency Check (Programmatic)

```python
# Compute expected latest = window-rank top-1 from summary, per account
expected_df = summary.withColumn("_rn", row_number().over(
    Window.partitionBy("cons_acct_key").orderBy(rpt_as_of_mo.desc(), base_ts.desc())
)).filter(_rn == 1)

# Assert exact match (no missing, no extra rows)
missing = expected.exceptAll(actual_latest)    # must be 0
extra   = actual_latest.exceptAll(expected)    # must be 0
```

### 10.4 Assertions

| # | Assertion | Expected |
|---|-----------|----------|
| 1 | Jan 2026 `balance_am_history[1]` | 4900 (sanity check — patch applied) |
| 2 | `latest_summary` exactly matches `summary` top-1 per account | 0 missing, 0 extra |

### 10.5 How to Run

```bash
docker compose -f main/docker_test/docker-compose.yml exec spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/test_latest_summary_consistency.py
```

---

## 11. Edge Case & Regression Tests

### 11.1 Backfill at Current Max Month

**`test_case3_current_max_month.py`** — Tests the boundary where `backfill month == current latest month`. This triggers Case III but with `MONTH_DIFF = 0`, which is a degenerate case.

| Scenario | Expected |
|----------|----------|
| Same month, newer base_ts | Row overwritten with newer values |
| Same month, older base_ts | Row unchanged (merge guard blocks) |

### 11.2 Consecutive Backfill

**`test_consecutive_backfill.py`** — Multiple backfill months for same account in one batch (e.g., Oct + Nov + Dec when Jan is latest).

| Aspect | Validated |
|--------|-----------|
| Peer map fills chained gaps | ✓ |
| Each new row's history references peers | ✓ |
| All future rows patched at correct positions | ✓ |
| base_ts propagated to all future rows | ✓ |

### 11.3 Non-Continuous Backfill

**`test_non_continuous_backfill.py`** — Backfill months with gaps (e.g., Oct + Dec but NOT Nov).

| Aspect | Validated |
|--------|-----------|
| Gap positions get NULL in history | ✓ |
| Non-gap positions filled correctly | ✓ |
| Peer map correctly excludes missing months | ✓ |

### 11.4 Long Backfill Gaps

**`test_long_backfill_gaps.py`** — Backfill 18+ months into the past.

| Aspect | Validated |
|--------|-----------|
| Partition pruning covers the wider range | ✓ |
| Array positions beyond 36 are silently dropped | ✓ |
| Future rows updated even with large gap | ✓ |

### 11.5 NULL Propagation

**`test_null_update_case_iii.py`** and **`test_null_update_other_cases.py`** — When source data contains NULL values for monetary/rating columns, the pipeline must propagate NULLs correctly into history arrays.

| Case | Scenario | Expected |
|------|----------|----------|
| Case I | New account with NULL balance | `balance_am_history[0] = NULL` |
| Case II | Forward entry with NULL payment | `payment_am_history[0] = NULL`, prior history shifted |
| Case III-A | Backfill with NULL value | `history[0] = NULL` in new row |
| Case III-B | NULL propagated to future rows | `history[position] = NULL` at correct index |
| Case IV | Bulk load with NULL in some months | MAP returns NULL for those months |

### 11.6 Duplicate Records

**`test_duplicate_records.py`** — Same `(cons_acct_key, rpt_as_of_mo)` appears twice in same batch with different `base_ts`.

Expected deduplication:
```
Rule: Keep the record with the highest base_ts
Tie-break: insert_ts DESC, then update_ts DESC
```

---

## 12. Scenario Suite

### 12.1 Purpose

`scenario_suite.py` is a shared Python module containing **50+ pre-built scenarios** that exercise every combination of case types, batch compositions, and edge conditions.

### 12.2 Coverage Matrix

| Category | Scenarios | Examples |
|----------|-----------|---------|
| Case I + II chaining | 5+ | New account, then forward next month |
| Case III (1 month) | 5+ | Single month backfill |
| Case III (2–3 months) | 5+ | Consecutive chained backfill |
| Case IV (6/12/36 months) | 3+ | Bulk uploads of varying sizes |
| Mixed batches | 10+ | All four cases in one batch |
| NULL propagation | 7+ | NULLs in every rolling column |
| Backfill beyond 36-month window | 2+ | Out-of-range backfill |
| Duplicate records | 3+ | Dedup via base_ts |
| Forward with 6-month gap | 2+ | Large gap forward entries |
| Backfill + forward same account | 3+ | Both cases for one account |

### 12.3 Wrapper Scripts

| Wrapper | Description |
|---------|-------------|
| `test_all_scenarios.py` | Runs all 50+ scenarios |
| `test_complex_scenarios.py` | Multi-step, multi-batch scenarios |
| `test_comprehensive_50_cases.py` | Full scenario sweep |
| `test_bulk_historical_load.py` | Case IV focus (6, 12, 36-month loads) |
| `test_consecutive_backfill.py` | Chained backfill cases |
| `test_non_continuous_backfill.py` | Non-contiguous backfill months |
| `test_long_backfill_gaps.py` | Backfill 18+ months in the past |
| `test_null_update.py` | NULL value propagation |
| `test_duplicate_records.py` | Dedup behaviour |
| `test_full_46_columns.py` | All 46 summary columns validated |
| `test_comprehensive_edge_cases.py` | Boundary condition sweep |

### 12.4 How to Run

```bash
# All scenarios
docker compose -f main/docker_test/docker-compose.yml exec spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/test_all_scenarios.py

# Specific wrapper
docker compose -f main/docker_test/docker-compose.yml exec spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/test_consecutive_backfill.py
```

---

## 13. Performance Benchmarks

### 13.1 Purpose

`test_performance_benchmark.py` — Validates that pipeline runtime stays within acceptable thresholds at different data scales.

### 13.2 Scale Tiers

| Tier | Records | Expected Runtime | Status |
|------|---------|-----------------|--------|
| TINY | ~100 records | < 60s | ✅ PASS |
| SMALL | ~10,000 records | < 5 min | ✅ PASS |
| MEDIUM | ~100,000 records | < 30 min | ✅ PASS |

### 13.3 Open Item (P1)

> **TODO:** Add explicit runtime threshold assertions to fail the test if performance regresses. Currently, benchmarks are observational only.

---

## 14. Running Tests

### 14.1 Single Suite

```bash
docker compose -f main/docker_test/docker-compose.yml exec spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/<test_file>.py
```

### 14.2 All Suites

```bash
# Sequential execution with report
docker compose -f main/docker_test/docker-compose.yml exec spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/run_all_tests.py

# With performance benchmarks included
docker compose -f main/docker_test/docker-compose.yml exec spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/run_all_tests.py --include-performance

# PowerShell helper with detailed HTML report
.\main\docker_test\tests\run_retest_with_report.ps1
```

### 14.3 Parallel Execution

Tests can run in parallel if they use separate namespaces. Parallel logs are written to:
```
main/docker_test/tests/_parallel_logs/<timestamp>_<run_id>/
```

---

## 15. Test Result Summary (February 2026)

| Suite | Result | Assertions |
|-------|--------|------------|
| test_main_all_cases | ✅ PASS | 10 |
| test_main_base_ts_propagation | ✅ PASS | 4 |
| test_idempotency | ✅ PASS | 6 |
| test_aggressive_idempotency | ✅ PASS | ~20 |
| test_latest_summary_consistency | ✅ PASS | 2 |
| test_case3_current_max_month | ✅ PASS | ~8 |
| test_consecutive_backfill | ✅ PASS | — |
| test_non_continuous_backfill | ✅ PASS | — |
| test_long_backfill_gaps | ✅ PASS | — |
| test_null_update_case_iii | ✅ PASS | — |
| test_null_update_other_cases | ✅ PASS | — |
| scenario_suite (50+ scenarios) | ✅ PASS | ~50+ |
| test_performance_benchmark | ✅ PASS | Observational |

### Open Items

| Priority | Item |
|----------|------|
| P0 | `case_i_result` lifecycle safety when `case_iv_records == 0` |
| P1 | Avoid `count()` existence checks where `tableExists()` suffices |
| P1 | Add explicit performance thresholds to benchmark tests |
