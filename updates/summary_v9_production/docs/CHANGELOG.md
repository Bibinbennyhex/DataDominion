# Changelog - Summary Pipeline

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
