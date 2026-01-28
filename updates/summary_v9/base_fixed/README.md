# Summary Pipeline v9 FIXED

## Bug Fix Summary

**Date**: January 28, 2026  
**Version**: v9 FIXED  
**Status**: **ALL TESTS PASSED** (12/12)

---

## The Bug

The original v9 pipeline had a bug in the backfill logic (`process_case_iii`):

**Original Behavior:**
- When backfill data arrived (e.g., Jan 2025 data arriving late)
- The pipeline correctly **updated** all future summaries (Jan/Feb/Mar 2026)
- But it **DID NOT create** a new summary row for the backfill month itself

**Expected Behavior:**
- Create a new summary row for the backfill month (Jan 2025)
- Update all future summaries with the backfill data

---

## The Fix

The `process_case_iii` function was rewritten to do THREE things:

### Part A: Create New Summary Rows for Backfill Months
```python
# Find the closest PRIOR summary for each backfill record
prior_summary = spark.sql("""
    SELECT b.*, s.* as prior_*, 
           (backfill_month - prior_month) as months_since_prior
    FROM backfill_records b
    LEFT JOIN summary s ON b.pk = s.pk AND s.month < b.month
    ORDER BY s.month DESC  -- Closest prior first
""")

# Create new summary row with:
# - Position 0: backfill data
# - Positions 1+: shifted data from prior summary
new_row = array(
    backfill_data,  -- Position 0
    NULL * (months_since_prior - 1),  -- Gap months
    prior_array[0:35-gap]  -- Inherited history
)
```

### Part B: Update Future Summary Rows (existing logic)
```python
# Join backfill with future summaries
# Update array at correct position using transform()
```

### Part C: Combine Results
```python
# Union new rows and updated futures
result = new_rows.unionByName(updated_futures)
```

---

## Test Results

### Before (Original v9)

```
+-------------+------------+-----+-----+------+------+------+------+------+
|cons_acct_key|rpt_as_of_mo|bal_0|bal_2|bal_12|bal_13|bal_14|bal_15|bal_16|
+-------------+------------+-----+-----+------+------+------+------+------+
|8001         |2024-11     |4000 |NULL |NULL  |NULL  |NULL  |NULL  |NULL  |
|8001         |2026-01     |6000 |NULL |3000  |NULL  |4000  |NULL  |NULL  |
|8001         |2026-02     |6500 |NULL |NULL  |3000  |NULL  |4000  |NULL  |
|8001         |2026-03     |7000 |NULL |NULL  |NULL  |3000  |NULL  |4000  |
+-------------+------------+-----+-----+------+------+------+------+------+
4 rows - MISSING Jan 2025 row!
```

### After (Fixed v9)

```
+-------------+------------+-----+-----+------+------+------+------+------+
|cons_acct_key|rpt_as_of_mo|bal_0|bal_2|bal_12|bal_13|bal_14|bal_15|bal_16|
+-------------+------------+-----+-----+------+------+------+------+------+
|8001         |2024-11     |4000 |NULL |NULL  |NULL  |NULL  |NULL  |NULL  |
|8001         |2025-01     |3000 |4000 |NULL  |NULL  |NULL  |NULL  |NULL  |  <-- NEW!
|8001         |2026-01     |6000 |NULL |3000  |NULL  |4000  |NULL  |NULL  |
|8001         |2026-02     |6500 |NULL |NULL  |3000  |NULL  |4000  |NULL  |
|8001         |2026-03     |7000 |NULL |NULL  |NULL  |3000  |NULL  |4000  |
+-------------+------------+-----+-----+------+------+------+------+------+
5 rows - Jan 2025 row CREATED with inherited Nov 2024 data!
```

---

## Detailed Test Results

| Test | Description | Status |
|------|-------------|--------|
| 0 | Summary row count = 5 | PASS |
| 1 | Nov 2024 bal[0] = 4000 (unchanged) | PASS |
| 2a | Jan 2025 row EXISTS | PASS |
| 2b | Jan 2025 bal[0] = 3000 | PASS |
| 2c | Jan 2025 bal[2] = 4000 (Nov 2024) | PASS |
| 2d | Jan 2025 pay[0] = 300 | PASS |
| 3a | Jan 2026 bal[12] = 3000 | PASS |
| 3b | Jan 2026 bal[14] = 4000 | PASS |
| 4a | Feb 2026 bal[13] = 3000 | PASS |
| 4b | Feb 2026 bal[15] = 4000 | PASS |
| 5a | Mar 2026 bal[14] = 3000 | PASS |
| 5b | Mar 2026 bal[16] = 4000 | PASS |

---

## Pipeline Statistics

```
Total records:       1
Case I (new):        0
Case II (forward):   0
Case III (backfill): 1
Records written:     4   <-- 1 new row + 3 updated rows
Duration:            1.79 minutes
```

---

## Files

| File | Purpose |
|------|---------|
| `summary_pipeline.py` | Fixed pipeline with corrected backfill logic |
| `run_fixed_test.py` | Test script that verifies the fix |
| `README.md` | This documentation |

---

## Key Changes in process_case_iii

### Original (lines 510-562)
```python
# Only joined to FUTURE summaries
joined = spark.sql("""
    SELECT ...
    FROM backfill_records b
    JOIN summary s ON b.pk = s.pk
    WHERE s.month > b.month  -- Only future!
""")
# Updated future summaries - but never created new row
```

### Fixed (lines 510-650)
```python
# PART A: Create new summary rows for backfill months
# Find closest prior summary and inherit history
prior_joined = spark.sql("""
    SELECT b.*, s.* as prior_*
    FROM backfill_records b
    LEFT JOIN summary s ON b.pk = s.pk AND s.month < b.month
    ORDER BY s.month DESC LIMIT 1
""")
new_rows = create_backfill_summary(prior_joined)

# PART B: Update future summaries (existing logic)
future_joined = spark.sql("""
    SELECT ...
    FROM backfill_records b
    JOIN summary s ON b.pk = s.pk
    WHERE s.month > b.month
""")
updated_futures = update_future_summaries(future_joined)

# PART C: Combine
result = new_rows.unionByName(updated_futures)
```

---

## Running the Test

```bash
# Copy files to Docker
docker cp base_fixed/summary_pipeline.py spark-iceberg:/home/iceberg/pipeline_fixed/
docker cp base_fixed/run_fixed_test.py spark-iceberg:/home/iceberg/test/

# Run test
docker exec spark-iceberg python3 /home/iceberg/test/run_fixed_test.py
```

---

## Conclusion

The fix correctly handles backfill by:
1. **Creating** a new summary row for the backfill month
2. **Inheriting** historical data from the closest prior summary
3. **Updating** all future summaries with the backfill data
4. **Preserving** existing data in all summaries
