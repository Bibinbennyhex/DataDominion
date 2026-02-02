# Changelog - Summary Pipeline

## v9.4.1 (February 2026)

### Bug Fix: Multiple Backfills in Same Batch

**CRITICAL**: Fixed issue where multiple backfills for the same account in the same batch were not all propagated to future months.

#### Problem (v9.4 - BROKEN)
```
Account 8002 has existing: Dec 2025, Jan 2026
Backfills arriving: Oct 2025 (6500), Nov 2025 (6200)

RESULT in Jan 2026:
  bal[2] = NULL  (WRONG! Should be 6200 - Nov backfill)
  bal[3] = 6500  (Oct backfill - this one worked)
```

#### Root Cause
In `process_case_iii()`, each backfill created a separate update with a single position, then deduplication picked just ONE update per account+month using arbitrary ordering (`F.lit(1)`).

#### Solution (v9.4.1 - FIXED)
```python
# 1. Collect all backfills per account+month
aggregated_df = future_joined.groupBy(pk, "summary_month").agg(
    F.collect_list(F.struct("backfill_position", "backfill_balance_am", ...)).alias("backfill_list"),
    F.first("existing_balance_am_history").alias("existing_balance_am_history"),
    ...
)

# 2. Apply ALL backfills using aggregate() inside transform()
update_expr = f"""
    transform(
        existing_{array_name},
        (x, i) -> COALESCE(
            aggregate(
                backfill_list,
                CAST(NULL AS {sql_type}),  # STRING or BIGINT based on column type
                (acc, b) -> CASE WHEN b.backfill_position = i THEN b.{backfill_col} ELSE acc END
            ),
            x
        )
    ) as {array_name}
"""
```

#### Result (v9.4.1 - CORRECT)
```
Account 8002 Jan 2026:
  bal[2] = 6200  (Nov backfill) ✓
  bal[3] = 6500  (Oct backfill) ✓
```

### Bug Fix: Type-Aware Aggregate Accumulator

Fixed type mismatch error when processing STRING columns (payment_rating_cd, asset_class_cd_4in).

#### Problem
```
[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "aggregate(...)" 
due to data type mismatch: Parameter 3 requires the "BIGINT" type, 
however "..." has the type "STRING"
```

#### Solution
```python
# Get the data type for this column from config
data_type = rc.get('type', rc.get('data_type', 'Integer'))
sql_type = 'STRING' if data_type.lower() == 'string' else 'BIGINT'

# Use type-aware accumulator
CAST(NULL AS {sql_type})  # Now correctly STRING for string columns
```

### New Test: Duplicate Record Handling

Added `tests/test_duplicate_records.py` to verify that when the same `(cons_acct_key, rpt_as_of_mo)` appears multiple times with different values, the record with latest `base_ts` wins.

#### Test Cases
| Account | Scenario | Expected Winner |
|---------|----------|-----------------|
| 12001 | 2 records same month (10:00, 11:00) | 11:00 → balance=5500 |
| 12002 | 3 records same month (09:00, 10:00, 11:00) | 11:00 → balance=7000 |
| 12003 | Multi-month with Feb duplicate | 11:00 → Feb balance=9000 |

### Test Results

| Test Suite | Passed | Failed |
|------------|--------|--------|
| `test_all_scenarios.py` | 26 | 0 |
| `test_comprehensive_edge_cases.py` | 34 | 0 |
| `test_duplicate_records.py` | 14 | 0 |
| **Total** | **74** | **0** |

### Files Modified
- `pipeline/summary_pipeline.py`:
  - Lines 1207-1277: Multiple backfill merge using `collect_list` + `aggregate`
  - Lines 1245-1247: Type-aware SQL generation (STRING vs BIGINT)
- `tests/test_comprehensive_edge_cases.py`:
  - Lines 813-816: Fixed 9002 test expectation (46800 not 36000)
- `tests/test_duplicate_records.py`: **NEW** - Duplicate record handling test

---

## v9.4 (January 2026)

### Temp Table Processing Architecture

Major architectural change to break Spark DAG lineage and improve memory efficiency:

#### 1. Temp Table Approach (Breaking DAG Lineage)
- **Problem**: Previous versions used in-memory union of all case results, causing Spark to hold entire DAG lineage in memory
- **Solution**: Each case writes results to a temporary Iceberg table, materializing data and breaking the lineage
- **Impact**: Significantly reduced driver memory pressure for large batches

```python
# Before (v9.3): In-memory union
case_i_results = process_case_i(...)
case_ii_results = process_case_ii(...)
all_results = case_i_results.unionByName(case_ii_results).unionByName(...)
write_results(all_results)  # DAG lineage held until write

# After (v9.4): Temp table materialization
temp_table = generate_temp_table_name()  # demo.temp.pipeline_batch_{ts}_{uuid}
create_temp_table(temp_table)

write_to_temp_table(case_i_results, temp_table, "CASE_I")
write_to_temp_table(case_ii_results, temp_table, "CASE_II")  # DAG lineage broken
# ... each case materializes independently

final_results = spark.read.table(temp_table)
write_results(final_results)
drop_temp_table(temp_table)
```

#### 2. Unique Batch ID Generation
- **Format**: `{schema}.pipeline_batch_{timestamp}_{uuid}`
- **Example**: `demo.temp.pipeline_batch_20260131_143052_a1b2c3d4`
- **Purpose**: Ensures no collision between concurrent runs or retries

#### 3. Automatic Cleanup (try/finally)
- **Implementation**: Temp table wrapped in try/finally block
- **Behavior**: Temp table dropped on success AND failure
- **Safety**: No orphaned temp tables under normal operation

```python
try:
    create_temp_table(temp_table)
    process_all_cases(...)
    write_final_results(...)
finally:
    drop_temp_table(temp_table)  # Always executes
```

#### 4. Orphan Cleanup Utility
- **New Function**: `cleanup_orphaned_temp_tables(spark, config, max_age_hours=24)`
- **Purpose**: Clean up temp tables from crashed/killed runs
- **Usage**: Run as scheduled job or manual maintenance

```bash
# CLI usage
python summary_pipeline.py --config config.json --cleanup-orphans --max-age-hours 24
```

#### 5. Legacy Mode
- **Flag**: `--legacy`
- **Purpose**: Fall back to v9.3 in-memory union approach
- **Use Case**: Debugging, comparison testing

```bash
# Run with temp table (default, recommended)
python summary_pipeline.py --config config.json

# Run legacy mode (in-memory union)
python summary_pipeline.py --config config.json --legacy
```

### Test Results
- **25 tests passing** (up from 21 in v9.3)
- Added 4 new tests for Case IV gap handling scenarios

### Files Modified
- `pipeline/summary_pipeline.py`:
  - Lines 230-255: `generate_temp_table_name()` function
  - Lines 257-293: `create_temp_table()` function
  - Lines 296-350: `write_to_temp_table()` function
  - Lines 353-372: `drop_temp_table()` function
  - Lines 375-437: `cleanup_orphaned_temp_tables()` function
  - Lines 1450-1600: Updated `run_pipeline()` with temp table orchestration

### Migration from v9.3
No config changes required. The temp table approach is the default. Use `--legacy` flag to revert to v9.3 behavior if needed.

---

## v9.3 (January 2026)

### Performance Optimizations

Major performance improvements targeting I/O reduction and write amplification:

#### 1. Case III Partition Pruning (50-90% I/O Reduction)
- **Problem**: Previous version read entire 50B+ row summary table for backfill processing
- **Solution**: Added partition filter based on backfill month range (±36 months)
- **Impact**: Reads only relevant partitions instead of full table scan

```python
# Before (v9.2): Full table scan
summary_df = spark.read.table(config.summary_table)

# After (v9.3): Partition-pruned read
summary_df = spark.sql(f"""
    SELECT ... FROM {config.summary_table}
    WHERE {prt} >= '{earliest_partition}' AND {prt} <= '{latest_partition}'
""")
```

#### 2. Single Consolidated MERGE (4x Less Write Amplification)
- **Problem**: 4 separate MERGE operations (one per case type) caused 4x write amplification
- **Solution**: Collect all results, union, deduplicate by priority, single MERGE
- **Impact**: 4x reduction in write I/O, better Iceberg commit efficiency

```python
# Collect all results with case type
all_results = [case_iii, case_i, case_iv, case_ii]

# Priority-based deduplication (III > I > IV > II)
combined = union_all(all_results).deduplicate_by_priority()

# Single MERGE
write_results(spark, combined, config)
```

#### 3. Case II Filter Pushdown (Iceberg Data File Pruning)
- **Problem**: Case II loaded entire latest_summary table, then filtered
- **Solution**: SQL subquery pushes filter to Iceberg, enabling data file pruning
- **Impact**: Reads only data files containing affected accounts

```python
# Before (v9.2): Load all, then join
latest_summary = spark.read.table(config.latest_summary_table)
latest_for_affected = latest_summary.join(affected_keys, pk, "inner")

# After (v9.3): SQL subquery for filter pushdown
latest_for_affected = spark.sql(f"""
    SELECT ... FROM {config.latest_summary_table} s
    WHERE s.{pk} IN (SELECT {pk} FROM case_ii_affected_keys)
""")
```

### Expected Performance Improvement

| Optimization | Estimated Impact |
|--------------|------------------|
| Case III partition pruning | 50-90% I/O reduction for backfill |
| Single MERGE | 4x less write amplification |
| Case II filter pushdown | Variable (depends on affected account ratio) |
| **Total** | **30-50% faster for mixed workloads** |

### Files Modified
- `pipeline/summary_pipeline.py`: 
  - Lines 655-675: Case II filter pushdown optimization
  - Lines 779-867: Case III partition pruning
  - Lines 1450-1540: Single consolidated MERGE

---

## v9.2.1 (January 2026)

### Bug Fix: Case IV Gap Handling
- **CRITICAL**: Fixed gap handling in Case IV (bulk historical load) processing
- Previous v9.2 used `COLLECT_LIST` which **skipped** gaps entirely, resulting in incorrect array positions
- Now uses `MAP_FROM_ENTRIES` + `TRANSFORM` approach for correct gap handling

#### Problem (v9.2 - BROKEN)
```
Account uploads: Jan 2025, Mar 2025 (Feb missing)

COLLECT_LIST returned: [6000, 5400]  (only existing values)
After reverse: [5400, 6000, NULL, NULL, ...]

RESULT: bal[1] = 6000 (WRONG! Should be NULL for Feb gap)
        bal[2] = NULL (WRONG! Should be 6000 for Jan)
```

#### Solution (v9.2.1 - FIXED)
```sql
-- Step 1: Build MAP of month_int -> value
MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(month_int, value))) as value_map

-- Step 2: Look up each position by month_int
TRANSFORM(
    SEQUENCE(0, 35),
    pos -> CAST(value_map[current_month_int - pos] AS INT)
) as history_array
```

#### Result (v9.2.1 - CORRECT)
```
Account 5001 with gaps:
Mar 2025: bal[0]=5400, bal[1]=NULL (Feb gap!), bal[2]=6000 (Jan) ✓
```

### Files Modified
- `pipeline/summary_pipeline.py`: Lines 1133-1216 (process_case_iv function)

---

## v9.2 (January 2026)

### New Feature: Case IV - Bulk Historical Load
- **CRITICAL**: Added Case IV to handle new accounts uploading multiple months at once
- Previously, if a new account uploaded 72 months of historical data, ALL records were classified as Case I
- This resulted in INCORRECT arrays - each month had only position 0 filled, no rolling history!
- Now correctly detects bulk historical loads and builds proper rolling arrays using SQL window functions

### Case IV Details
- Detects when a new account (not in summary) has multiple months in the batch
- First month → Case I (initialize with single value)
- Subsequent months → Case IV (build rolling history from prior months in batch)
- Uses `MAP_FROM_ENTRIES` + `TRANSFORM` for gap-aware array building (fixed in v9.2.1)

### Processing Order (Updated)
1. **Case III (Backfill)** - Highest priority
2. **Case I (New Accounts)** - First month for new accounts
3. **Case IV (Bulk Historical)** - Subsequent months for new accounts with multi-month uploads
4. **Case II (Forward)** - Lowest priority

### Tests Added
- `tests/test_bulk_historical_load.py` - Tests 72-month bulk upload scenario
- `tests/test_all_scenarios.py` - Comprehensive 4-case test with gap handling (21 tests)
- `tests/test_comprehensive_edge_cases.py` - Extended edge case coverage (40+ tests)
- `tests/test_performance_benchmark.py` - Performance benchmarking at various scales

---

## v9.1-fixed (January 2026)

### Bug Fix
- **CRITICAL**: Fixed Case III (backfill) logic that was not creating summary rows for backfill months
- Original v9 only updated future summaries when backfill data arrived
- Now correctly creates a new summary row for the backfill month itself, inheriting historical data from the closest prior summary

### New Features
- Full column mapping support (36 source → destination mappings)
- Column transformations (13 sentinel value handlers)
- Inferred columns (orig_loan_am, payment_rating_cd)
- Date column validation (invalid years → NULL)
- Separate column lists for summary vs latest_summary tables
- Configurable Spark settings from JSON
- Dynamic allocation support

### Performance
- ~1.85x faster than v8 for backfill scenarios
- Reduced memory usage (no COLLECT_LIST)
- Configurable broadcast threshold

---

## v9.0 (December 2025)

### Features
- SQL-based array updates using `transform()`
- Broadcast optimization for small tables
- MEMORY_AND_DISK caching
- Single-pass array updates
- PipelineConfig class

### Bug
- **Case III backfill only updates future summaries, does NOT create backfill month row**

---

## v8.0 (November 2025)

### Features
- Single-file, function-only implementation
- 7-CTE SQL approach for backfill
- Dynamic allocation support
- Iceberg table optimization (rewrite_data_files, expire_snapshots)
- Bucketing support

### Performance
- ~220 min for mixed workload (50B table)
- High memory usage for COLLECT_LIST

---

## v5.0 (October 2025)

### Features
- Class-based architecture (RollingColumnBuilder, etc.)
- Column mappers and transformations
- Incremental and initial modes

---

## v4.0 (September 2025)

### Features
- Initial production implementation
- 36-month rolling history arrays
- Case I/II/III classification
- MERGE to Iceberg tables

---

## Migration Guide: v9.0 → v9.1-fixed

1. **Replace pipeline file**: `summary_pipeline.py`
2. **Update config**: Add new sections (see below)
3. **Test backfill**: Run `tests/run_backfill_test.py`

### Config Changes Required

Add these new sections to your config:

```json
{
  "columns": { /* 36 column mappings */ },
  "column_transformations": [ /* 13 transforms */ ],
  "inferred_columns": [ /* 2 derived columns */ ],
  "date_columns": [ /* 7 date columns */ ],
  "summary_columns": [ /* 47 output columns */ ],
  "latest_summary_columns": [ /* 34 output columns */ ]
}
```

See `config/pipeline_config.json` for complete example.
