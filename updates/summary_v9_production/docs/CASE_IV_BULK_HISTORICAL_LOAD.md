# Case IV: Bulk Historical Load

## Overview

Case IV handles new accounts that upload multiple months of historical data in a single batch.

| Version | Status | Gap Handling | I/O Optimization |
|---------|--------|--------------|------------------|
| v9.0/v9.1 | Broken | N/A (all months were Case I) | None |
| v9.2 | Fixed | Broken (gaps not preserved) | None |
| v9.2.1 | Fixed | Fixed (gaps correctly show NULL) | None |
| v9.3 | Fixed | Fixed | **Single MERGE, partition pruning** |

## The Problem

When a **brand new account** uploads **multiple months of historical data** in a single batch (e.g., 72 months), the v9.0/v9.1 pipeline incorrectly processed all records as **Case I (New Accounts)**.

### v9.0/v9.1 Behavior (WRONG)

```
Account 9001 uploads 72 months: Jan 2020 - Dec 2025

Classification (WRONG):
- Jan 2020 → Case I (no summary exists) → bal_history = [10000, NULL, NULL, ...]
- Feb 2020 → Case I (no summary exists) → bal_history = [9900, NULL, NULL, ...]
- Mar 2020 → Case I (no summary exists) → bal_history = [9800, NULL, NULL, ...]
... (all 72 months classified as Case I)

Result: 72 summary rows, each with ONLY position 0 filled - no rolling history!
```

### v9.2+ Behavior (CORRECT)

```
Account 9001 uploads 72 months: Jan 2020 - Dec 2025

Processing Order:
- Jan 2020 → Case I   → bal_history = [10000, NULL, NULL, ...]
- Feb 2020 → Case IV  → bal_history = [9900, 10000, NULL, ...]
- Mar 2020 → Case IV  → bal_history = [9800, 9900, 10000, ...]
...
- Dec 2025 → Case IV  → bal_history = [Rolling 36-month window with proper values]
```

## Classification Logic

```
For each record in the batch:
├── Account NOT in latest_summary (new account)?
│   ├── Only 1 month for this account in batch? → CASE_I
│   ├── Multiple months AND this is the earliest? → CASE_I  
│   └── Multiple months AND this is NOT earliest? → CASE_IV
├── Month > max existing month? → CASE_II (Forward)
└── Otherwise → CASE_III (Backfill)
```

## Gap Handling (v9.2.1 Fix)

### The Problem with v9.2

The original v9.2 implementation used `COLLECT_LIST` to build arrays:

```sql
-- v9.2 approach (BROKEN for gaps)
COLLECT_LIST(value) OVER (
    PARTITION BY account_key
    ORDER BY month_int
    ROWS BETWEEN 35 PRECEDING AND CURRENT ROW
)
-- Then reverse and pad with NULLs
```

**Problem**: `COLLECT_LIST` only collects existing values, completely ignoring gaps.

```
Account 5001 uploads: Jan 2025, Mar 2025, Apr 2025 (Feb MISSING)

COLLECT_LIST for Mar 2025 returns: [6000, 5400]  (Jan, Mar - no Feb!)
After reverse: [5400, 6000, NULL, NULL, ...]

RESULT (WRONG):
  Mar 2025: bal[0] = 5400 (Mar - correct)
            bal[1] = 6000 (WRONG! This is Jan, should be NULL for Feb gap)
            bal[2] = NULL (WRONG! Jan should be here)
```

### The Solution (v9.2.1)

Use `MAP_FROM_ENTRIES` + `TRANSFORM` to look up values by month_int:

```sql
-- Step 1: Build MAP for each column by account
-- MAP key = month_int, value = column value
SELECT 
    account_key,
    MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(month_int, balance_am))) as value_map_balance_am
FROM bulk_records
GROUP BY account_key

-- Step 2: Build arrays using TRANSFORM to look up by position
-- For each position 0-35, calculate month_int and look up in map
-- Missing months return NULL automatically!
SELECT 
    r.*,
    TRANSFORM(
        SEQUENCE(0, 35),
        pos -> CAST(m.value_map_balance_am[r.month_int - pos] AS INT)
    ) as balance_am_history
FROM bulk_records r
JOIN account_value_maps m ON r.account_key = m.account_key
```

**Why This Works**:
- The MAP only contains entries for months that exist in the data
- Looking up `month_int - pos` for a missing month returns NULL
- No need to explicitly handle gaps - they're NULL by default

```
Account 5001 with MAP approach:

MAP for balance_am: {202501: 6000, 202503: 5400, 202504: 5100}

For Mar 2025 (month_int = 202503):
  pos 0: MAP[202503 - 0] = MAP[202503] = 5400 ✓ (Mar)
  pos 1: MAP[202503 - 1] = MAP[202502] = NULL ✓ (Feb gap!)
  pos 2: MAP[202503 - 2] = MAP[202501] = 6000 ✓ (Jan)

RESULT (CORRECT):
  Mar 2025: bal[0] = 5400 (Mar)
            bal[1] = NULL (Feb gap - correct!)
            bal[2] = 6000 (Jan - correct!)
```

## Test Cases

### Test 1: Continuous 72 Months
- Input: Account 9001 with 72 consecutive months (Jan 2020 - Dec 2025)
- Expected: 72 summary rows, Dec 2025 has full 36-month rolling window

### Test 2: 12 Months with Gaps
- Input: Account 5001 with 9 months (missing Feb, May, Aug 2025)
- Expected: 9 summary rows, arrays show NULL at gap positions

### Test 3: Alternating Months
- Input: Account 9004 with every-other-month (Jan, Mar, May, Jul, Sep, Nov)
- Expected: 6 summary rows, bal[1] is NULL for each (gap)

### Test 4: Extreme Gap (35 months)
- Input: Account 9005 with only Jan 2023 and Dec 2025
- Expected: 2 rows, Dec 2025 has bal[35]=value, positions 1-34 are NULL

### Test 5: Full 36 Months
- Input: Account 9001 with exactly 36 months
- Expected: Dec summary has all 36 positions filled with correct values

### Test 6: Exceeds 36 Months (48 months)
- Input: Account 9002 with 48 months
- Expected: Dec summary has positions 0-35 filled (36-month window), oldest 12 months dropped

## Processing Order

**Critical for correctness:**

1. **Case III (Backfill)** - FIRST - highest priority
2. **Case I (New Accounts)** - SECOND - no dependencies  
3. **Case IV (Bulk Historical)** - THIRD - uses Case I first months
4. **Case II (Forward)** - LAST - uses corrected history

## Performance Considerations

| Approach | Complexity | Performance | Memory | Gap Handling |
|----------|------------|-------------|--------|--------------|
| COLLECT_LIST (v9.2) | Low | Fast | Low | Broken |
| MAP + TRANSFORM (v9.2.1) | Medium | Fast | Medium | Correct |

The MAP approach adds:
- One additional GROUP BY for building account-level MAPs
- One JOIN to connect MAPs back to records
- TRANSFORM for each rolling column

Measured overhead: ~10-15% additional time compared to v9.2, but correctness is mandatory.

## Code Location

**File**: `pipeline/summary_pipeline.py`
**Function**: `process_case_iv()` 
**Lines**: 1050-1246

Key sections:
- Lines 1133-1150: Gap handling explanation comments
- Lines 1152-1178: MAP building for each rolling column
- Lines 1180-1214: TRANSFORM-based array generation

## Files to Run Tests

```bash
# All scenarios including gap handling
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_all_scenarios.py

# Comprehensive edge cases
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_comprehensive_edge_cases.py

# 72-month bulk historical
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_bulk_historical_load.py

# Performance benchmark
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_performance_benchmark.py --scale SMALL
```

## Priority

**HIGH** - This bug meant any new account with historical data had incorrect summary arrays.

Fixed in v9.2 (bulk historical), gap handling fixed in v9.2.1, optimizations added in v9.3.

## v9.3 Optimizations

Case IV benefits from v9.3 optimizations:
- **Single MERGE**: Case IV results are combined with other cases and written in one MERGE operation
- **Priority-based deduplication**: Case IV has priority 3 (after III=1, I=2, before II=4)
