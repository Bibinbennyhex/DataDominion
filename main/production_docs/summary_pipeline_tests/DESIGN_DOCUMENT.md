# Summary Incremental Pipeline - Design Document

**Document Version**: 1.0  
**Pipeline Baseline**: `main/summary_inc.py` (current production logic)  
**Last Updated**: February 13, 2026

---

## 1. Executive Summary

The pipeline maintains a denormalized `summary` table with 36-month rolling arrays and a `latest_summary` table with one latest row per account. Incoming monthly account data is classified and processed by case type:

- Case I: new account, single month.
- Case II: existing account, forward month.
- Case III: existing account, backfill month.
- Case IV: new account with multiple months in the same batch.

The current implementation uses merge-based writes and watermark tracking to support reruns and controlled incremental ingestion.

---

## 2. System Overview

### 2.1 Core Tables

| Table | Purpose | Partitioning |
|---|---|---|
| `source_table` | Incoming account-level snapshots | `rpt_as_of_mo` |
| `destination_table` (`summary`) | Monthly account summary with 36-length histories | `rpt_as_of_mo` |
| `latest_history_table` (`latest_summary`) | Latest row per `cons_acct_key` | unpartitioned |
| `summary_watermark_tracker` (derived) | Tracks summary/latest/committed watermarks | unpartitioned |

### 2.2 Rolling Data Model

Each output row carries rolling arrays of length `history_length` (default 36), for example:

- `actual_payment_am_history`
- `balance_am_history`
- `credit_limit_am_history`
- `past_due_am_history`
- `payment_rating_cd_history`
- `days_past_due_history`
- `asset_class_cd_4in_history`

Grid column:

- `payment_history_grid`

---

## 3. Pipeline Flow

1. `cleanup()` drops temp case tables (`temp_catalog.checkpointdb.case_1/2/3a/3b/4`).
2. `load_and_classify_accounts()`:
   - validates `summary` and `latest_summary` max-month consistency,
   - reads committed ingestion watermark from tracker (or summary fallback),
   - filters source increment (`base_ts > watermark`, month range bounded),
   - applies mappings, transformations, inferred columns, date guards, dedupe,
   - classifies into Case I/II/III/IV.
3. Case execution order in `run_pipeline()`:
   - Case III (backfill) first,
   - Case I (new single-month),
   - Case IV (new multi-month),
   - write backfill/new merges,
   - Case II (forward),
   - write forward merges.
4. On success: refresh tracker with `mark_committed=True`.
5. On failure: refresh tracker with `mark_committed=False` and re-raise.

---

## 4. Case Logic

## 4.1 Case I

- Input: account not present in `latest_summary`, single month in batch.
- Output: new summary row with initialized arrays and grid.
- Write path: merged into `summary`; latest-per-account merged into `latest_summary`.

## 4.2 Case II

- Input: account present in `latest_summary`, incoming month after latest month.
- Output: shifted forward arrays with current value at position 0.
- Write path:
  - merge into `summary` on `(cons_acct_key, rpt_as_of_mo)` with `source.base_ts >= target.base_ts` for updates,
  - merge into `latest_summary` only if incoming month is newer (or same month with newer/equal `base_ts`).

## 4.3 Case III

- Input: account present, incoming month less than or equal to latest month.
- Output:
  - `case_3a`: create missing backfill-month rows,
  - `case_3b`: patch future rows by inserting backfill value at correct offset.
- Write path:
  - merge `case_3a` into `summary` (insert or update),
  - merge `case_3b` into `summary` (matched updates for history/grid/base_ts),
  - update `latest_summary` from latest per-account candidate rows derived from case_3a/case_3b.

## 4.4 Case IV

- Input: new account with multiple months in same batch.
- Output: ordered month chain with rolling histories built from within-batch sequence.
- Write path: unioned with Case I results and merged into `summary`; latest row merged into `latest_summary`.

---

## 5. Merge and Idempotency Model

### 5.1 Merge Strategy

- Case I + Case IV: merge into `summary` with match key `(cons_acct_key, rpt_as_of_mo)`.
- Case II: merge into `summary` with update guard `c.base_ts >= s.base_ts`.
- Case III:
  - `case_3a` merge insert/update by key,
  - `case_3b` merge matched update only for rolling/grid/base_ts fields.
- `latest_summary` updated by merge from latest candidates per account.

### 5.2 Watermark Tracker

Tracker rows maintained by `source_name`:

- `summary`
- `latest_summary`
- `committed_ingestion_watermark`

`committed_ingestion_watermark` is advanced only on successful end-to-end completion and uses conservative minimum semantics across summary/latest maxima.

### 5.3 Practical Idempotency Semantics

- Re-running same batch should not grow row counts or drift row values due to merge keys and timestamp guards.
- Ingestion filter uses committed watermark; repeated runs with no new source data naturally produce no-op behavior.
- Concurrency with overlapping source slices still requires external run-level serialization or locking in orchestration.

---

## 6. Failure and Recovery

- Exception path attempts tracker refresh with `mark_committed=False`.
- Temp tables are removed by `cleanup()` before each run.
- Recovery behavior is validated by `test_recovery.py` and idempotency suites.

---

## 7. Configuration Reference (Key Fields)

| Key | Purpose |
|---|---|
| `source_table` | Source table for incoming records |
| `destination_table` | Summary output table |
| `latest_history_table` | Latest summary table |
| `primary_column` | Account key (for example `cons_acct_key`) |
| `partition_column` | Month partition column (`rpt_as_of_mo`) |
| `max_identifier_column` | Ingestion timestamp (`base_ts`) |
| `history_length` | Rolling array length (default 36) |
| `rolling_columns` | Column definitions for rolling arrays |
| `grid_columns` | Grid derivation definitions |
| `columns` | Source-to-target column mapping |
| `column_transformations` | Cleansing/transformation expressions |
| `inferred_columns` | Derived fields |
| `spark` | Spark runtime parameters |

---

## 8. Assumptions and Constraints

- `summary` and `latest_summary` max month must remain aligned before run start.
- `rpt_as_of_mo` values must be valid and comparable (`YYYY-MM`).
- `base_ts` must be reliable for latest-record precedence.
- Two runs operating on same logical source window should be orchestrated to avoid write contention.
- Backfill correctness depends on availability of required summary partitions.

---

## 9. Related Documents

- `PERFORMANCE.md`
- `TEST_CASES_COMBINED.md`
- `EXECUTION_RUNBOOK.md`
- `RETEST_RESULTS_2026-02-13.md`
