# Cascade Backfill Test v2 Results

**Test Date**: January 28, 2026  
**Pipeline Version**: v9  
**Test Status**: **5 FAILED** (7 passed, 5 failed)

---

## BUG IDENTIFIED

The v9 pipeline has a **critical bug** in the backfill logic:

### The Problem

When backfill data arrives (e.g., Jan 2025 data arriving late):
1. **WORKS**: Updates all future summaries (Jan/Feb/Mar 2026) with the backfill data at correct positions
2. **BUG**: Does NOT create a new summary row for the backfill month itself

### Expected Behavior

When Jan 2025 backfill data arrives, the pipeline should:
1. **CREATE** a new `2025-01` summary row with:
   - `bal[0] = 3000` (Jan 2025 data)
   - `bal[2] = 4000` (Nov 2024 data, inherited from the closest prior summary)
2. **UPDATE** Jan 2026, Feb 2026, Mar 2026 summaries (cascade)

### Actual Behavior

- Only 4 summary rows (Nov 2024, Jan/Feb/Mar 2026) - no Jan 2025 row created
- Future summaries correctly updated

---

## Test Results

### Before Pipeline

```
+-------------+------------+-----+-----+------+------+------+------+------+
|cons_acct_key|rpt_as_of_mo|bal_0|bal_2|bal_12|bal_13|bal_14|bal_15|bal_16|
+-------------+------------+-----+-----+------+------+------+------+------+
|8001         |2024-11     |4000 |NULL |NULL  |NULL  |NULL  |NULL  |NULL  |
|8001         |2026-01     |6000 |NULL |NULL  |NULL  |4000  |NULL  |NULL  |
|8001         |2026-02     |6500 |NULL |NULL  |NULL  |NULL  |4000  |NULL  |
|8001         |2026-03     |7000 |NULL |NULL  |NULL  |NULL  |NULL  |4000  |
+-------------+------------+-----+-----+------+------+------+------+------+
```

### After Pipeline (Actual)

```
+-------------+------------+-----+-----+------+------+------+------+------+
|cons_acct_key|rpt_as_of_mo|bal_0|bal_2|bal_12|bal_13|bal_14|bal_15|bal_16|
+-------------+------------+-----+-----+------+------+------+------+------+
|8001         |2024-11     |4000 |NULL |NULL  |NULL  |NULL  |NULL  |NULL  |
|8001         |2026-01     |6000 |NULL |3000  |NULL  |4000  |NULL  |NULL  |
|8001         |2026-02     |6500 |NULL |NULL  |3000  |NULL  |4000  |NULL  |
|8001         |2026-03     |7000 |NULL |NULL  |NULL  |3000  |NULL  |4000  |
+-------------+------------+-----+-----+------+------+------+------+------+
```

### After Pipeline (Expected)

```
+-------------+------------+-----+-----+------+------+------+------+------+
|cons_acct_key|rpt_as_of_mo|bal_0|bal_2|bal_12|bal_13|bal_14|bal_15|bal_16|
+-------------+------------+-----+-----+------+------+------+------+------+
|8001         |2024-11     |4000 |NULL |NULL  |NULL  |NULL  |NULL  |NULL  |
|8001         |2025-01     |3000 |4000 |NULL  |NULL  |NULL  |NULL  |NULL  |  <-- MISSING!
|8001         |2026-01     |6000 |NULL |3000  |NULL  |4000  |NULL  |NULL  |
|8001         |2026-02     |6500 |NULL |NULL  |3000  |NULL  |4000  |NULL  |
|8001         |2026-03     |7000 |NULL |NULL  |NULL  |3000  |NULL  |4000  |
+-------------+------------+-----+-----+------+------+------+------+------+
```

---

## Code Location

**File**: `updates/summary_v9/base/summary_pipeline.py`
**Function**: `process_case_iii()` (lines 456-589)

### The Bug (lines 510-523)

```python
joined = spark.sql(f"""
    SELECT 
        b.*,
        s.{prt} as summary_month,
        ...
    FROM backfill_records b
    JOIN summary_affected s ON b.{pk} = s.{pk}
    WHERE {month_to_int_expr(f"s.{prt}")} > {month_to_int_expr(f"b.{prt}")}  # <-- Only joins FUTURE summaries
      AND ({month_to_int_expr(f"s.{prt}")} - {month_to_int_expr(f"b.{prt}")}) < {HISTORY_LENGTH}
""")
```

This JOIN only matches:
- Future summaries (summary_month > backfill_month)

It should ALSO:
- Create a new summary row for the backfill month itself

---

## Required Fix

The `process_case_iii` function needs to be updated to:

1. **Create backfill month summary rows**: For each backfill record that doesn't have a summary row for its month, create one by:
   - Setting position 0 to the backfill data
   - Inheriting historical data from the closest prior summary

2. **Continue updating future summaries** (existing behavior - works correctly)

### Pseudocode for Fix

```python
def process_case_iii(spark, case_iii_df, config):
    # ... existing code ...
    
    # STEP A: Identify backfill records that need NEW summary rows
    backfill_needing_new_row = spark.sql("""
        SELECT b.*
        FROM backfill_records b
        LEFT JOIN summary_affected s 
            ON b.{pk} = s.{pk} AND b.{prt} = s.{prt}
        WHERE s.{pk} IS NULL  -- No existing summary for this month
    """)
    
    # STEP B: For each needing new row, find closest prior summary
    # and create new summary with inherited history + backfill at position 0
    new_summaries = create_backfill_summaries(backfill_needing_new_row)
    
    # STEP C: Update future summaries (existing logic - works)
    updated_futures = update_future_summaries(case_iii_df)
    
    # STEP D: Union new + updated
    return new_summaries.unionByName(updated_futures)
```

---

## Test Cases

| Test | Expected | Actual | Status |
|------|----------|--------|--------|
| Row count | 5 | 4 | **FAIL** |
| Nov 2024 unchanged | bal[0]=4000 | bal[0]=4000 | PASS |
| Jan 2025 row EXISTS | EXISTS | NOT FOUND | **FAIL** |
| Jan 2025 bal[0] | 3000 | - | **FAIL** |
| Jan 2025 bal[2] (Nov 2024) | 4000 | - | **FAIL** |
| Jan 2025 pay[0] | 300 | - | **FAIL** |
| Jan 2026 bal[12] | 3000 | 3000 | PASS |
| Jan 2026 bal[14] | 4000 | 4000 | PASS |
| Feb 2026 bal[13] | 3000 | 3000 | PASS |
| Feb 2026 bal[15] | 4000 | 4000 | PASS |
| Mar 2026 bal[14] | 3000 | 3000 | PASS |
| Mar 2026 bal[16] | 4000 | 4000 | PASS |

---

## Pipeline Statistics

```
Total records:       1
Case I (new):        0     <-- Should be 0 (backfill has existing account)
Case II (forward):   0
Case III (backfill): 1     <-- Correct
Records written:     3     <-- Should be 4 (3 updates + 1 new row)
Duration:            0.84 minutes
```

---

## Files

| File | Purpose |
|------|---------|
| `run_cascade_test_v2.py` | Test script that identified the bug |
| `CASCADE_TEST_RESULTS_V2.md` | This documentation |

---

## Next Steps

1. **Fix the pipeline**: Update `process_case_iii()` to create summary rows for backfill months
2. **Re-run tests**: Verify all 12 tests pass
3. **Update documentation**: Update test case documentation with corrected expectations
