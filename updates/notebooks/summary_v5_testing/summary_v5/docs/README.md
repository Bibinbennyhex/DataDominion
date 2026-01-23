# Summary Pipeline v5.0

**Unified Production Pipeline - Single File Implementation**

---

## What's New in v5

### 1. Single-File Implementation
All code is consolidated into **one file**: `summary_pipeline.py` (1,400+ lines)

No separate modules to import - everything runs from this single file.

### 2. Incremental Mode (Production Default)
New default mode that automatically handles **all scenarios in one run**:
- Backfill (corrections for historical months)
- New accounts (first-time appearances)  
- Forward entries (new month data)

### 3. Correct Processing Order
Automatically processes in the correct sequence:
1. **Backfill FIRST** - Rebuilds history with corrections
2. **New accounts SECOND** - Creates initial arrays
3. **Forward LAST** - Uses corrected history

---

## Quick Start

### Production Run (Default)
```bash
# Just run - defaults to incremental mode
spark-submit summary_pipeline.py --config summary_config.json

# Or explicit
spark-submit summary_pipeline.py --config summary_config.json --mode incremental
```

### Initial Load (First Time)
```bash
spark-submit summary_pipeline.py \
  --config summary_config.json \
  --mode initial \
  --start-month 2016-01 \
  --end-month 2025-12
```

### Forward Processing (Month Range)
```bash
spark-submit summary_pipeline.py \
  --config summary_config.json \
  --mode forward \
  --start-month 2026-01 \
  --end-month 2026-03
```

### Manual Backfill
```bash
spark-submit summary_pipeline.py \
  --config summary_config.json \
  --mode backfill \
  --backfill-filter "cons_acct_key = 123 AND rpt_as_of_mo = '2025-06'"
```

---

## File Structure

```
summary_v5/
├── summary_pipeline.py    ← Single file with ALL code (1,400+ lines)
├── summary_config.json    ← Configuration file
└── README.md              ← This file
```

---

## Processing Modes

| Mode | Default | Use Case | Command |
|------|---------|----------|---------|
| **incremental** | ✅ Yes | Production daily runs | `--mode incremental` |
| **initial** | | First-time historical load | `--mode initial --start-month 2016-01 --end-month 2025-12` |
| **forward** | | Sequential month processing | `--mode forward --start-month 2026-01` |
| **backfill** | | Manual corrections | `--mode backfill --backfill-filter "..."` |

---

## Your Production Scenario

### Existing State
```
Data in summary: 2016-01 to 2025-12
Latest base_ts: 2026-01-14 23:59:59
```

### New Data Arrives (2026-01-15)
```
Account 1, 2026-01, balance=11000  → Forward entry
Account 2, 2026-01, balance=6000   → Forward entry  
Account 1, 2025-11, balance=10700  → Backfill (correction!)
Account 9999, 2026-01, balance=50K → New account
```

### Run Incremental Mode
```bash
spark-submit summary_pipeline.py --config summary_config.json
```

### What Happens
```
================================================================================
SUMMARY PIPELINE v5.0 - INCREMENTAL MODE (PRODUCTION)
================================================================================
Latest timestamp in summary: 2026-01-14 23:59:59
Processing all new data since: 2026-01-14 23:59:59
New records found: 4

CLASSIFICATION RESULTS:
  Case I   (New accounts):            1 records
  Case II  (Forward entries):         2 records
  Case III (Backfill):                1 records

STEP 1/3: Processing 1 backfill records (Case III)
  → Rebuilding history arrays with corrected values...
  → Backfill processing completed

STEP 2/3: Processing 1 new accounts (Case I)
  → Creating initial history arrays...
  → New account processing completed

STEP 3/3: Processing 2 forward entries (Case II)
  → Shifting history arrays (using corrected history from backfill)...
  → Forward processing completed

INCREMENTAL PROCESSING COMPLETE
================================================================================
```

---

## Code Structure

### Single File Organization

```python
# summary_pipeline.py (1,400+ lines)

# IMPORTS (lines 1-50)

# LOGGING SETUP (lines 52-62)

# CONFIGURATION CLASS (lines 64-140)
class SummaryConfig:
    """Loads and validates JSON config"""

# SPARK SESSION FACTORY (lines 142-175)
def create_spark_session():
    """Creates optimized Spark session"""

# UTILITY FUNCTIONS (lines 177-220)
def calculate_optimal_partitions():
def configure_spark_partitions():
def replace_invalid_years():
def month_to_int():
def month_to_int_expr():

# ROLLING COLUMN BUILDER (lines 222-320)
class RollingColumnBuilder:
    """Builds 36-month rolling arrays"""

# GRID COLUMN GENERATOR (lines 322-360)
def generate_grid_column():
    """Converts arrays to string grids"""

# SUMMARY PIPELINE - MAIN CLASS (lines 362-1350)
class SummaryPipeline:
    # Entry Points
    run_incremental()    # ← Production default
    run_forward()
    run_backfill()
    
    # Forward Mode Helpers
    _check_initial_state()
    _process_single_month()
    _build_initial_summary()
    _build_incremental_summary()
    _write_results()
    
    # Backfill/Incremental Helpers
    _get_max_summary_timestamp()
    _prepare_source_data()
    _classify_backfill_records()
    _process_case_i()
    _process_case_ii()
    _process_case_iii()
    _write_backfill_results()
    
    # Optimization
    _optimize_tables()

# CLI ENTRY POINT (lines 1352-1400)
def parse_args():
def main():
```

---

## Key Methods

### run_incremental()
**The production default**. Automatically handles all scenarios:

```python
def run_incremental(self):
    # 1. Get latest timestamp from summary
    max_ts = self._get_max_summary_timestamp()
    
    # 2. Read all new data (base_ts > max_ts)
    batch_df = spark.sql(f"SELECT * FROM source WHERE base_ts > '{max_ts}'")
    
    # 3. Classify records
    classified = self._classify_backfill_records(batch_df)
    
    # 4. Process in correct order
    self._process_case_iii(...)  # Backfill FIRST
    self._process_case_i(...)    # New accounts SECOND
    self._process_case_ii(...)   # Forward LAST
```

### _classify_backfill_records()
**Determines case type for each record**:

```python
Case I:   New account (max_existing_month IS NULL)
Case II:  Forward (month_int > max_month_int)
Case III: Backfill (month_int <= max_month_int AND newer timestamp)
```

### _process_case_iii()
**The complex one** - Rebuilds entire history using 7-CTE SQL:

1. Validate backfill (newer timestamp wins)
2. Get all existing data for affected accounts
3. Combine backfill + existing
4. Deduplicate (backfill priority)
5. Keep winners
6. Collect all months per account
7. Rebuild 36-month arrays for each month

---

## Configuration

Same as v4 - uses `summary_config.json`:

```json
{
  "source_table": "default.default.accounts_all",
  "destination_table": "default.summary",
  "latest_history_table": "default.latest_summary",
  "partition_column": "rpt_as_of_mo",
  "primary_column": "cons_acct_key",
  "history_length": 36,
  
  "rolling_columns": [
    {
      "name": "balance_am",
      "mapper_expr": "CASE WHEN balance_am < 0 THEN NULL ELSE balance_am END",
      "mapper_column": "balance_am",
      "num_cols": 36,
      "type": "Integer"
    }
  ],
  
  "grid_columns": [
    {
      "name": "payment_history_grid",
      "mapper_rolling_column": "payment_rating_cd",
      "placeholder": "?"
    }
  ]
}
```

---

## Comparison: v4 vs v5

| Feature | v4 | v5 |
|---------|----|----|
| **File Structure** | Multiple files | Single file |
| **Default Mode** | forward | **incremental** |
| **Auto-Classification** | Manual | **Automatic** |
| **Processing Order** | Manual | **Guaranteed** |
| **Production Ready** | ✅ | ✅ (Enhanced) |

---

## Validation

After running:

```bash
# Check record counts
spark-sql -e "
SELECT 
  COUNT(*) as total_records,
  COUNT(DISTINCT cons_acct_key) as unique_accounts,
  MAX(rpt_as_of_mo) as latest_month,
  MAX(base_ts) as latest_timestamp
FROM default.summary
"

# Check for duplicates (should be 0)
spark-sql -e "
SELECT cons_acct_key, rpt_as_of_mo, COUNT(*) 
FROM default.summary 
GROUP BY 1, 2 
HAVING COUNT(*) > 1
"

# Check array lengths (should be 0)
spark-sql -e "
SELECT COUNT(*) 
FROM default.summary 
WHERE SIZE(balance_am_history) != 36
"
```

---

## Production Deployment

### EMR Command
```bash
spark-submit \
  --deploy-mode cluster \
  --master yarn \
  --conf spark.sql.shuffle.partitions=2000 \
  --conf spark.executor.memory=100g \
  --conf spark.executor.instances=50 \
  s3://your-bucket/summary_pipeline.py \
  --config s3://your-bucket/summary_config.json
```

### Airflow DAG
```python
run_incremental = EmrAddStepsOperator(
    task_id='run_summary_incremental',
    steps=[{
        'Name': 'Summary Pipeline Incremental',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                's3://bucket/summary_pipeline.py',
                '--config', 's3://bucket/summary_config.json'
                # No --mode needed, defaults to incremental
            ]
        }
    }]
)
```

---

## Summary

### What You Get
- ✅ **Single file** - Easy to deploy and maintain
- ✅ **Production default** - Just run with config
- ✅ **Auto-handles all scenarios** - Backfill, new accounts, forward
- ✅ **Correct order guaranteed** - No manual sequencing needed
- ✅ **Same config format** - Compatible with v4 configs

### Production Command
```bash
# That's all you need:
spark-submit summary_pipeline.py --config summary_config.json
```

---

**Version**: 5.0.0  
**Status**: Production Ready ✅  
**Lines of Code**: 1,400+  
**Files**: 1 (consolidated)
