# 10M Consumer Account Keys - Simulation Test Documentation

## Executive Summary

This document details the simulation test plan for processing 10 million consumer account keys through the incremental summary update pipeline. The test validates all three processing cases at production-representative scale.

| Metric | Value |
|--------|-------|
| Total Unique Accounts | 10,000,000 |
| Base Summary Rows | ~360,000,000 |
| New Batch Records | 10,000,000 |
| Estimated Processing Time | 25-35 minutes |
| Target Throughput | ~5,500 records/second |

---

## 1. Test Environment

### 1.1 Infrastructure Specifications

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PRODUCTION SPARK CLUSTER                              │
├─────────────────────────────────────────────────────────────────────────────┤
│  Driver Node                                                                 │
│  ├── Memory: 16 GB                                                          │
│  ├── Cores: 8                                                               │
│  └── Role: Job coordination, broadcast management                           │
│                                                                              │
│  Executor Nodes (50 nodes)                                                  │
│  ├── Memory per executor: 8 GB                                              │
│  ├── Cores per executor: 4                                                  │
│  ├── Total cores: 200                                                       │
│  └── Total memory: 400 GB                                                   │
│                                                                              │
│  Storage Layer                                                               │
│  ├── Type: MinIO (S3-compatible)                                            │
│  ├── Capacity: 10 TB                                                        │
│  └── Throughput: 10 Gbps                                                    │
│                                                                              │
│  Metadata Catalog                                                            │
│  ├── Type: Iceberg REST Catalog                                             │
│  └── Backend: PostgreSQL                                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Spark Configuration

```python
# Production configuration for 10M scale
spark.sql.shuffle.partitions = 2000
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.skewJoin.enabled = true
spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB
spark.sql.autoBroadcastJoinThreshold = 50MB
spark.executor.memory = 8g
spark.executor.cores = 4
spark.driver.memory = 16g
spark.dynamicAllocation.enabled = true
spark.dynamicAllocation.minExecutors = 10
spark.dynamicAllocation.maxExecutors = 100
spark.sql.files.maxPartitionBytes = 128MB
spark.sql.iceberg.planning.preserve-data-grouping = true
```

---

## 2. Test Data Model

### 2.1 Source Table Schema (accounts_all)

```sql
CREATE TABLE default.accounts_all (
    cons_acct_key     BIGINT      NOT NULL,  -- Unique account identifier
    acct_dt           DATE        NOT NULL,  -- Account date
    rpt_as_of_mo      STRING      NOT NULL,  -- Reporting month (YYYY-MM)
    current_balance   INT,                    -- Current balance
    current_dpd       INT,                    -- Days past due
    payment_am        INT,                    -- Payment amount
    status_cd         STRING,                 -- Status code
    account_status    STRING,                 -- Account status
    created_ts        TIMESTAMP,              -- Record creation time
    updated_ts        TIMESTAMP,              -- Last update time
    base_ts           TIMESTAMP   NOT NULL   -- Source system timestamp
)
USING iceberg
PARTITIONED BY (rpt_as_of_mo)
```

### 2.2 Target Table Schema (summary_testing)

```sql
CREATE TABLE default.summary_testing (
    cons_acct_key     BIGINT      NOT NULL,  -- Unique account identifier
    rpt_as_of_mo      STRING      NOT NULL,  -- Reporting month (YYYY-MM)
    bal_history       ARRAY<INT>,             -- 36-month balance history
    dpd_history       ARRAY<INT>,             -- 36-month DPD history
    payment_history   ARRAY<INT>,             -- 36-month payment history
    status_history    ARRAY<STRING>,          -- 36-month status history
    current_balance   INT,                    -- Current balance
    current_dpd       INT,                    -- Current DPD
    account_status    STRING,                 -- Account status
    created_ts        TIMESTAMP,              -- Record creation time
    updated_ts        TIMESTAMP,              -- Last update time
    base_ts           TIMESTAMP   NOT NULL   -- Source system timestamp
)
USING iceberg
PARTITIONED BY (rpt_as_of_mo)
```

### 2.3 Data Volume Estimates

| Dataset | Records | Columns | Est. Size (Parquet) |
|---------|---------|---------|---------------------|
| Base Summary | 360,000,000 | 12 | ~500 GB |
| New Batch | 10,000,000 | 11 | ~15 GB |
| Output Delta | 10,000,000 | 12 | ~15 GB |

---

## 3. Test Case Distribution

### 3.1 Case Distribution for 10M Batch

```
┌────────────────────────────────────────────────────────────────────────────┐
│                         TEST CASE DISTRIBUTION                              │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Case I (New Accounts)                                                     │
│   ████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  5% = 500,000 records           │
│                                                                             │
│   Case II (Forward Entries)                                                 │
│   ████████████████████████████████████████  85% = 8,500,000 records        │
│   ████████████████████████████████████░░░░                                  │
│                                                                             │
│   Case III (Backfill Entries)                                               │
│   ████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  10% = 1,000,000 records        │
│                                                                             │
│   TOTAL: 10,000,000 records                                                 │
└────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Rationale for Distribution

| Case | Percentage | Justification |
|------|------------|---------------|
| Case I | 5% | New accounts represent a small fraction of monthly updates. Typical churn rate is 3-7%. |
| Case II | 85% | Most updates are forward entries for the current month. This is the normal business flow. |
| Case III | 10% | Backfills occur due to late-arriving data, corrections, or reprocessing. |

---

## 4. Detailed Test Cases

### 4.1 Case I - New Account Tests

#### TC-001: Single New Account
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test ID: TC-001                                                             │
│ Name: Case I - Single New Account                                           │
│ Priority: HIGH                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description:                                                                │
│   Insert a single new account that doesn't exist in the summary table.      │
│   Verify that a new row is created with proper history array initialization.│
│                                                                             │
│ Input:                                                                      │
│   cons_acct_key: 100000001 (does not exist in summary)                      │
│   rpt_as_of_mo: "2025-06"                                                   │
│   current_balance: 5000                                                     │
│   current_dpd: 0                                                            │
│   base_ts: "2025-07-20 12:00:00"                                           │
│                                                                             │
│ Expected Results:                                                           │
│   ✓ New row created in summary table                                        │
│   ✓ bal_history[0] = 5000 (current value)                                   │
│   ✓ bal_history[1:35] = [NULL, NULL, ..., NULL]                            │
│   ✓ size(bal_history) = 36                                                  │
│   ✓ All other history arrays similarly initialized                         │
│                                                                             │
│ Validation Query:                                                           │
│   SELECT cons_acct_key, rpt_as_of_mo,                                       │
│          size(bal_history) as arr_len,                                      │
│          bal_history[0] as current_val,                                     │
│          bal_history[35] as oldest_val                                      │
│   FROM summary_testing                                                      │
│   WHERE cons_acct_key = 100000001                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### TC-002: Bulk New Accounts (500K)
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test ID: TC-002                                                             │
│ Name: Case I - Bulk New Accounts                                            │
│ Priority: HIGH                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description:                                                                │
│   Insert 500,000 new accounts in a single batch. Verify all accounts        │
│   are created correctly with no duplicates.                                 │
│                                                                             │
│ Input:                                                                      │
│   cons_acct_key range: 10,000,001 to 10,500,000                            │
│   rpt_as_of_mo: Random distribution across 2025-01 to 2025-12              │
│   Various balance/DPD values                                                │
│                                                                             │
│ Expected Results:                                                           │
│   ✓ 500,000 new rows created                                                │
│   ✓ All arrays have length 36                                               │
│   ✓ No duplicate (cons_acct_key, rpt_as_of_mo) pairs                       │
│   ✓ Processing time < 60 seconds                                            │
│                                                                             │
│ Validation Query:                                                           │
│   SELECT COUNT(*) as total,                                                 │
│          COUNT(DISTINCT cons_acct_key) as unique_accounts,                  │
│          SUM(CASE WHEN size(bal_history) != 36 THEN 1 ELSE 0 END) as bad    │
│   FROM summary_testing                                                      │
│   WHERE cons_acct_key BETWEEN 10000001 AND 10500000                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Case II - Forward Entry Tests

#### TC-003: Forward Entry - Adjacent Month
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test ID: TC-003                                                             │
│ Name: Case II - Forward Entry Same Month                                    │
│ Priority: HIGH                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description:                                                                │
│   Account exists with max month 2025-06. Insert new record for 2025-07.     │
│   Verify array is shifted by 1 position.                                    │
│                                                                             │
│ Input:                                                                      │
│   cons_acct_key: 1 (exists with max month 2025-06)                         │
│   rpt_as_of_mo: "2025-07"                                                   │
│   current_balance: 8000                                                     │
│   Existing bal_history[0:5]: [5000, 4800, 4600, 4400, 4200, 4000]          │
│                                                                             │
│ Expected Results:                                                           │
│   ✓ Row updated for 2025-07                                                 │
│   ✓ bal_history[0] = 8000 (new value)                                       │
│   ✓ bal_history[1] = 5000 (previous index 0)                               │
│   ✓ bal_history[2] = 4800 (previous index 1)                               │
│   ✓ bal_history[35] dropped (shifted out)                                   │
│                                                                             │
│ Array Transformation:                                                       │
│   BEFORE: [5000, 4800, 4600, 4400, ..., val_35]                            │
│   AFTER:  [8000, 5000, 4800, 4600, ..., val_34]                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### TC-004: Forward Entry - With Gap
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test ID: TC-004                                                             │
│ Name: Case II - Forward Entry With Gap                                      │
│ Priority: HIGH                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description:                                                                │
│   Account exists with max month 2025-06. Insert new record for 2025-09      │
│   (3 month gap). Verify array is shifted with NULL padding.                 │
│                                                                             │
│ Input:                                                                      │
│   cons_acct_key: 2                                                          │
│   Existing max month: 2025-06                                               │
│   New rpt_as_of_mo: "2025-09" (gap = 3 months)                             │
│   current_balance: 7500                                                     │
│                                                                             │
│ Expected Results:                                                           │
│   ✓ bal_history[0] = 7500 (new value)                                       │
│   ✓ bal_history[1] = NULL (gap month 2025-08)                              │
│   ✓ bal_history[2] = NULL (gap month 2025-07)                              │
│   ✓ bal_history[3] = previous bal_history[0] (2025-06 value)               │
│   ✓ Last 3 values shifted out                                              │
│                                                                             │
│ Array Transformation:                                                       │
│   BEFORE: [5000, 4800, 4600, ..., val_33, val_34, val_35]                  │
│   AFTER:  [7500, NULL, NULL, 5000, 4800, 4600, ..., val_33]                │
│                      ↑    ↑                                                 │
│                   gap months                                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### TC-005: Bulk Forward Entries (8.5M)
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test ID: TC-005                                                             │
│ Name: Case II - Bulk Forward Entries                                        │
│ Priority: CRITICAL                                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description:                                                                │
│   Process 8.5 million forward entries for existing accounts.                │
│   This is the primary use case representing normal monthly updates.         │
│                                                                             │
│ Input:                                                                      │
│   cons_acct_key range: 1 to 8,500,000                                      │
│   rpt_as_of_mo: "2026-01" (forward from existing max months)               │
│   Gap distribution:                                                         │
│     - 90% have gap = 1 (adjacent month)                                     │
│     - 8% have gap = 2-3 months                                              │
│     - 2% have gap = 4-12 months                                             │
│                                                                             │
│ Expected Results:                                                           │
│   ✓ 8,500,000 rows updated                                                  │
│   ✓ All arrays correctly shifted                                            │
│   ✓ Gap months filled with NULL                                             │
│   ✓ Processing time < 10 minutes                                            │
│   ✓ No executor failures                                                    │
│                                                                             │
│ Performance Targets:                                                        │
│   - Throughput: > 14,000 records/second                                     │
│   - Shuffle data: < 50 GB                                                   │
│   - Peak memory per executor: < 6 GB                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.3 Case III - Backfill Tests

#### TC-006: Backfill Single Month Gap
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test ID: TC-006                                                             │
│ Name: Case III - Backfill Single Month                                      │
│ Priority: HIGH                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description:                                                                │
│   Account has history with gap: 2025-03, [missing 2025-04], 2025-05.       │
│   Insert backfill for 2025-04. Verify history arrays rebuilt for future.   │
│                                                                             │
│ Input:                                                                      │
│   cons_acct_key: 5                                                          │
│   Existing months: 2025-03, 2025-05, 2025-06 (gap at 2025-04)             │
│   New rpt_as_of_mo: "2025-04"                                               │
│   current_balance: 3500                                                     │
│   base_ts: "2025-07-20" (newer than existing)                              │
│                                                                             │
│ Expected Results:                                                           │
│   ✓ New row created for 2025-04                                             │
│   ✓ 2025-05 row's bal_history[1] now = 3500 (was NULL)                     │
│   ✓ 2025-06 row's bal_history[2] now = 3500 (was NULL)                     │
│   ✓ All affected future rows have history rebuilt                          │
│                                                                             │
│ History Rebuild Visualization:                                              │
│                                                                             │
│   Month    Index 0   Index 1   Index 2   Index 3                           │
│   ──────────────────────────────────────────────                           │
│   2025-03    3000     2800      2600      ...                              │
│   2025-04    3500     3000      2800      2600    ← NEW ROW                │
│   2025-05    4000     3500*     3000      2800    ← REBUILT (*was NULL)    │
│   2025-06    4500     4000      3500*     3000    ← REBUILT (*was NULL)    │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### TC-007: Backfill With Newer Timestamp
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test ID: TC-007                                                             │
│ Name: Case III - Backfill With Newer base_ts                                │
│ Priority: HIGH                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description:                                                                │
│   Account already has value for 2025-04 with base_ts = 2025-07-15.         │
│   Insert new value with base_ts = 2025-07-20. Value should be replaced.    │
│                                                                             │
│ Input:                                                                      │
│   cons_acct_key: 7                                                          │
│   rpt_as_of_mo: "2025-04"                                                   │
│   Existing: current_balance = 3000, base_ts = "2025-07-15 12:00:00"        │
│   New: current_balance = 3500, base_ts = "2025-07-20 12:00:00"             │
│                                                                             │
│ Expected Results:                                                           │
│   ✓ 2025-04 value updated to 3500                                          │
│   ✓ base_ts updated to 2025-07-20                                          │
│   ✓ All future rows rebuilt with new value                                  │
│                                                                             │
│ Timestamp Comparison:                                                       │
│   new.base_ts (2025-07-20) > existing.base_ts (2025-07-15) → UPDATE        │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### TC-008: Backfill Ignored (Older Timestamp)
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test ID: TC-008                                                             │
│ Name: Case III - Backfill Ignored (Older base_ts)                           │
│ Priority: MEDIUM                                                            │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description:                                                                │
│   Account already has value for 2025-04 with base_ts = 2025-07-20.         │
│   Attempt insert with base_ts = 2025-07-15. Should be ignored.             │
│                                                                             │
│ Input:                                                                      │
│   cons_acct_key: 8                                                          │
│   rpt_as_of_mo: "2025-04"                                                   │
│   Existing: current_balance = 3500, base_ts = "2025-07-20 12:00:00"        │
│   New: current_balance = 3000, base_ts = "2025-07-15 12:00:00"             │
│                                                                             │
│ Expected Results:                                                           │
│   ✓ 2025-04 value remains 3500                                              │
│   ✓ base_ts remains 2025-07-20                                              │
│   ✓ No changes to any rows                                                  │
│                                                                             │
│ Timestamp Comparison:                                                       │
│   new.base_ts (2025-07-15) <= existing.base_ts (2025-07-20) → SKIP         │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### TC-009: Bulk Backfill (1M)
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test ID: TC-009                                                             │
│ Name: Case III - Bulk Backfill                                              │
│ Priority: CRITICAL                                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description:                                                                │
│   Process 1 million backfill entries. This tests the most complex           │
│   processing path that requires history array reconstruction.               │
│                                                                             │
│ Input:                                                                      │
│   cons_acct_key range: 8,500,001 to 9,500,000                              │
│   rpt_as_of_mo: Random distribution 2023-01 to 2025-06                     │
│   All with base_ts newer than existing                                      │
│                                                                             │
│ Expected Results:                                                           │
│   ✓ 1,000,000 backfill records processed                                    │
│   ✓ All affected future rows rebuilt                                        │
│   ✓ All arrays have length 36                                               │
│   ✓ Processing time < 20 minutes                                            │
│                                                                             │
│ Performance Targets:                                                        │
│   - Throughput: > 800 records/second                                        │
│   - Shuffle data: < 100 GB                                                  │
│   - Executor memory usage stable                                            │
│                                                                             │
│ Note: Case III is most expensive due to history rebuild                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.4 Mixed Batch & Edge Case Tests

#### TC-010: Mixed Batch - All Cases Combined
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test ID: TC-010                                                             │
│ Name: Mixed Batch - All Cases Combined                                      │
│ Priority: CRITICAL                                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description:                                                                │
│   Full production-representative batch with all three cases.                │
│   Tests the complete pipeline end-to-end.                                   │
│                                                                             │
│ Input:                                                                      │
│   Total records: 10,000,000                                                 │
│   Case I (new accounts): 500,000 (5%)                                       │
│   Case II (forward entries): 8,500,000 (85%)                               │
│   Case III (backfill): 1,000,000 (10%)                                     │
│                                                                             │
│ Expected Results:                                                           │
│   ✓ All 10M records processed                                               │
│   ✓ Correct case classification                                             │
│   ✓ No duplicate rows created                                               │
│   ✓ All arrays have length 36                                               │
│   ✓ Total processing time < 35 minutes                                      │
│                                                                             │
│ Success Criteria:                                                           │
│   - Zero data loss                                                          │
│   - Zero duplicate violations                                               │
│   - All validations pass                                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### TC-011: Edge Case - Maximum Gap (36 months)
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test ID: TC-011                                                             │
│ Name: Edge Case - 36 Month Gap Forward                                      │
│ Priority: MEDIUM                                                            │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description:                                                                │
│   Forward entry with exactly 36 month gap. The entire history array         │
│   should be replaced with the new value and NULLs.                          │
│                                                                             │
│ Input:                                                                      │
│   cons_acct_key: 11                                                         │
│   Existing max month: "2022-06"                                             │
│   New rpt_as_of_mo: "2025-06" (36 month gap)                               │
│   current_balance: 10000                                                    │
│                                                                             │
│ Expected Results:                                                           │
│   ✓ bal_history[0] = 10000                                                  │
│   ✓ bal_history[1:35] = all NULL (entire old history shifted out)          │
│   ✓ Effectively a fresh start for this account                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### TC-013: Performance - Skewed Distribution
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test ID: TC-013                                                             │
│ Name: Performance - Skewed Account Distribution                             │
│ Priority: HIGH                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description:                                                                │
│   Test with highly skewed data where 1% of accounts have 50% of records.   │
│   Validates that skew handling mechanisms work correctly.                   │
│                                                                             │
│ Input:                                                                      │
│   Hot accounts: 100,000 accounts with 5,000,000 records (avg 50 per acct)  │
│   Regular accounts: 9,900,000 accounts with 5,000,000 records (0.5 avg)    │
│                                                                             │
│ Expected Results:                                                           │
│   ✓ No executor OOM errors                                                  │
│   ✓ AQE skew join optimization triggered                                    │
│   ✓ Processing completes within 2x normal time                              │
│   ✓ All data correctly processed                                            │
│                                                                             │
│ Monitoring Points:                                                          │
│   - Check Spark UI for skew warnings                                        │
│   - Monitor executor memory usage                                           │
│   - Verify salt bucketing activation                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.5 Validation Tests

#### TC-014: No Duplicates Validation
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test ID: TC-014                                                             │
│ Name: Validation - No Duplicates After Update                               │
│ Priority: CRITICAL                                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│ Validation Query:                                                           │
│                                                                             │
│   SELECT cons_acct_key, rpt_as_of_mo, COUNT(*) as cnt                       │
│   FROM summary_testing                                                      │
│   GROUP BY cons_acct_key, rpt_as_of_mo                                      │
│   HAVING COUNT(*) > 1                                                       │
│                                                                             │
│ Expected Result: 0 rows                                                     │
│                                                                             │
│ Failure Action:                                                             │
│   If duplicates found → Critical failure, investigate merge logic           │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### TC-015: Array Length Validation
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test ID: TC-015                                                             │
│ Name: Validation - Array Length Consistency                                 │
│ Priority: CRITICAL                                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│ Validation Query:                                                           │
│                                                                             │
│   SELECT cons_acct_key, rpt_as_of_mo,                                       │
│          size(bal_history) as bal_len,                                      │
│          size(dpd_history) as dpd_len                                       │
│   FROM summary_testing                                                      │
│   WHERE size(bal_history) != 36                                             │
│      OR size(dpd_history) != 36                                             │
│      OR size(payment_history) != 36                                         │
│      OR size(status_history) != 36                                          │
│                                                                             │
│ Expected Result: 0 rows                                                     │
│                                                                             │
│ Failure Action:                                                             │
│   If invalid arrays found → Critical failure, check concat/slice logic      │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Execution Timeline

```
┌────────────────────────────────────────────────────────────────────────────┐
│                         EXECUTION TIMELINE                                  │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  T+0:00  ┬─ Job Start                                                       │
│          │  └─ Load configuration                                           │
│          │  └─ Initialize Spark session                                     │
│          │                                                                   │
│  T+0:05  ┼─ Stage 1: Get max(base_ts) from summary                         │
│          │  └─ Single aggregate, ~5 seconds                                 │
│          │                                                                   │
│  T+0:10  ┼─ Stage 2: Filter new records from accounts_all                  │
│          │  └─ Partition pruning on base_ts                                 │
│          │  └─ Load 10M new records, ~30 seconds                            │
│          │                                                                   │
│  T+0:40  ┼─ Stage 3: Classify records into cases                           │
│          │  └─ Left join with summary metadata                              │
│          │  └─ Case I/II/III determination                                  │
│          │  └─ ~2 minutes                                                   │
│          │                                                                   │
│  T+2:40  ┼─ Stage 4: Process Case I (500K new accounts)                    │
│          │  └─ Generate initial history arrays                              │
│          │  └─ ~30 seconds                                                  │
│          │                                                                   │
│  T+3:10  ┼─ Stage 5: Process Case II (8.5M forward entries)                │
│          │  └─ Shift existing arrays                                        │
│          │  └─ Fill gaps with NULLs                                         │
│          │  └─ ~5 minutes                                                   │
│          │                                                                   │
│  T+8:10  ┼─ Stage 6: Process Case III (1M backfill)                        │
│          │  └─ Validate base_ts ordering                                    │
│          │  └─ Rebuild history for affected rows                            │
│          │  └─ ~15 minutes (most expensive)                                 │
│          │                                                                   │
│  T+23:10 ┼─ Stage 7: Union and Merge                                       │
│          │  └─ Combine all case outputs                                     │
│          │  └─ MERGE INTO summary table                                     │
│          │  └─ ~5 minutes                                                   │
│          │                                                                   │
│  T+28:10 ┼─ Stage 8: Validation                                            │
│          │  └─ Check for duplicates                                         │
│          │  └─ Verify array lengths                                         │
│          │  └─ ~2 minutes                                                   │
│          │                                                                   │
│  T+30:10 ┴─ Job Complete                                                    │
│                                                                             │
│  TOTAL ESTIMATED TIME: 30-35 minutes                                        │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Expected Metrics

### 6.1 Processing Metrics

| Metric | Expected Value | Alert Threshold |
|--------|----------------|-----------------|
| Total records processed | 10,000,000 | < 10,000,000 |
| Case I rows created | 500,000 | Deviation > 1% |
| Case II rows updated | 8,500,000 | Deviation > 1% |
| Case III rows affected | ~5,000,000 | N/A (varies) |
| Duplicate count | 0 | > 0 |
| Invalid array count | 0 | > 0 |

### 6.2 Performance Metrics

| Stage | Expected Duration | Shuffle Size | Records/Second |
|-------|-------------------|--------------|----------------|
| Filter new records | 30 sec | 15 GB | 333,333 |
| Classify | 2 min | 30 GB | 83,333 |
| Case I processing | 30 sec | 5 GB | 16,667 |
| Case II processing | 5 min | 50 GB | 28,333 |
| Case III processing | 15 min | 100 GB | 1,111 |
| Merge | 5 min | 70 GB | 33,333 |
| **Total** | **~30 min** | **~270 GB** | **~5,500** |

### 6.3 Resource Utilization

| Resource | Target | Max Acceptable |
|----------|--------|----------------|
| Executor memory avg | 4 GB | 6 GB |
| Executor memory peak | 6 GB | 7.5 GB |
| CPU utilization | 70% | 90% |
| Shuffle spill | 0 GB | 50 GB |
| GC time % | < 5% | < 10% |

---

## 7. Failure Scenarios & Recovery

### 7.1 Executor OOM
```
Symptom: java.lang.OutOfMemoryError: GC overhead limit exceeded
Root Cause: Large partitions during Case III history rebuild
Recovery:
  1. Increase shuffle partitions: 2000 → 4000
  2. Reduce broadcast threshold: 50MB → 20MB
  3. Enable more aggressive spill: spark.memory.fraction = 0.5
```

### 7.2 Skew Detected
```
Symptom: Some tasks take 10x longer than average
Root Cause: Hot accounts with disproportionate data
Recovery:
  1. AQE should auto-handle (verify spark.sql.adaptive.skewJoin.enabled)
  2. If persists, enable salt bucketing in config
  3. Increase spark.sql.adaptive.skewJoin.skewedPartitionFactor
```

### 7.3 S3 Throttling
```
Symptom: SlowDown errors from S3/MinIO
Root Cause: Too many parallel requests
Recovery:
  1. Reduce spark.sql.files.maxPartitionBytes: 128MB → 256MB
  2. Add retry configuration with exponential backoff
  3. Coalesce output files before write
```

---

## 8. Validation Checklist

### Pre-Execution Checklist
- [ ] Spark cluster healthy with all executors
- [ ] Sufficient storage space (> 1 TB free)
- [ ] Network connectivity verified
- [ ] Source tables accessible
- [ ] Configuration reviewed and approved

### Post-Execution Checklist
- [ ] Job completed without errors
- [ ] TC-014 passed (no duplicates)
- [ ] TC-015 passed (array lengths correct)
- [ ] Record counts match expected
- [ ] Processing time within bounds
- [ ] No data corruption detected
- [ ] Metrics logged and archived

---

## 9. Appendix

### A. Quick Reference Commands

```bash
# Run simulation with dry-run
spark-submit simulation_10m_test.py --dry-run

# Print execution plan
spark-submit simulation_10m_test.py --print-plan

# Print test cases
spark-submit simulation_10m_test.py --print-tests

# Full simulation run
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 50 \
  --conf spark.sql.shuffle.partitions=2000 \
  simulation_10m_test.py
```

### B. Validation Queries

```sql
-- Count by case type
SELECT 
    CASE 
        WHEN cons_acct_key > 10000000 THEN 'CASE_I'
        WHEN rpt_as_of_mo = '2026-01' THEN 'CASE_II'
        ELSE 'CASE_III'
    END as case_type,
    COUNT(*) as cnt
FROM summary_testing
WHERE base_ts > TIMESTAMP '2025-07-15 12:00:00'
GROUP BY 1;

-- Check history integrity
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN bal_history[0] IS NULL THEN 1 ELSE 0 END) as null_current,
    AVG(size(bal_history)) as avg_len
FROM summary_testing;
```

---

**Document Version:** 1.0  
**Last Updated:** 2026-01-21  
**Author:** DataDominion Team
