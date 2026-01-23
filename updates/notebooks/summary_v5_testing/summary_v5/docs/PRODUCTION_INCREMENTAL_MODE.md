# Summary Pipeline v4.0 - Production Incremental Mode

## The Problem

### Current Production Scenario

Your production state:
```
Existing data: 2016-01 to 2025-12
Latest base_ts: 2026-01-15
New batch arriving: 2026-01 rpt_as_of_mo
```

### What Arrives in a Single Batch

When new data arrives (e.g., on 2026-01-15), it contains **mixed scenarios**:

```
+---------------+-------------+----------+---------------------+----------+
| cons_acct_key | rpt_as_of_mo| balance_ | base_ts             | Scenario |
+---------------+-------------+----------+---------------------+----------+
| 1             | 2026-01     | 11000    | 2026-01-15 10:00:00 | FORWARD  |
| 2             | 2026-01     | 6000     | 2026-01-15 10:00:00 | FORWARD  |
| 1             | 2025-11     | 10700    | 2026-01-15 10:00:00 | BACKFILL |
| 2             | 2025-10     | 5300     | 2026-01-15 10:00:00 | BACKFILL |
| 9999          | 2026-01     | 50000    | 2026-01-15 10:00:00 | NEW ACCT |
+---------------+-------------+----------+---------------------+----------+
```

**Problem**: Current pipeline has separate modes, but you need to handle ALL scenarios in ONE run!

---

## The Solution: Incremental Mode

### New "incremental" Mode

A production-ready mode that:
1. **Auto-detects** all three scenarios (backfill, forward, new accounts)
2. **Processes in correct order** (backfill → new accounts → forward)
3. **Handles everything in one run**
4. **No manual filtering required**

---

## Implementation

### Option 1: Enhanced Pipeline (Recommended)

Add new `run_incremental()` method to handle production workflow automatically.

#### Code Addition to summary_pipeline.py

```python
# ============================================================
# INCREMENTAL MODE (PRODUCTION DEFAULT)
# ============================================================

def run_incremental(self):
    """
    Run incremental processing for production.
    
    This mode automatically handles:
    - Backfill: Late-arriving corrections (month <= max existing)
    - New accounts: First appearance of accounts
    - Forward: New month data (month > max existing)
    
    All in one run, properly sequenced.
    """
    logger.info("=" * 80)
    logger.info("SUMMARY PIPELINE v4.0 - INCREMENTAL MODE (PRODUCTION)")
    logger.info("=" * 80)
    
    # Step 1: Get latest timestamp from summary
    max_ts = self._get_max_summary_timestamp()
    
    if max_ts is None:
        logger.warning("No existing summary found. Use 'initial' mode for first load.")
        return
    
    logger.info(f"Processing new data since: {max_ts}")
    
    # Step 2: Read all new data (base_ts > max existing timestamp)
    batch_df = self.spark.sql(f"""
        SELECT * FROM {self.config.source_table}
        WHERE {self.config.max_identifier_column} > TIMESTAMP '{max_ts}'
    """)
    
    record_count = batch_df.count()
    if record_count == 0:
        logger.info("No new records to process")
        return
    
    logger.info(f"New records to process: {record_count}")
    
    # Step 3: Prepare and classify ALL records
    batch_df = self._prepare_source_data(batch_df)
    classified = self._classify_backfill_records(batch_df)
    
    # Count each case
    case_counts = classified.groupBy("case_type").count().collect()
    case_dict = {row["case_type"]: row["count"] for row in case_counts}
    
    logger.info(f"Case I (New accounts): {case_dict.get('CASE_I', 0)}")
    logger.info(f"Case II (Forward entries): {case_dict.get('CASE_II', 0)}")
    logger.info(f"Case III (Backfill): {case_dict.get('CASE_III', 0)}")
    
    # Step 4: Process in correct order
    # Order matters! Backfill FIRST (rebuilds history), then new accounts, then forward
    
    # 4a. Process backfill first (updates existing months)
    case_iii = classified.filter(F.col("case_type") == "CASE_III")
    if case_iii.limit(1).count() > 0:
        logger.info("Processing backfill records (Case III)...")
        self._process_case_iii(case_iii)
        logger.info("Backfill processing complete")
    
    # 4b. Process new accounts (no dependencies)
    case_i = classified.filter(F.col("case_type") == "CASE_I")
    if case_i.limit(1).count() > 0:
        logger.info("Processing new accounts (Case I)...")
        self._process_case_i(case_i)
        logger.info("New account processing complete")
    
    # 4c. Process forward entries LAST (uses updated history from backfill)
    case_ii = classified.filter(F.col("case_type") == "CASE_II")
    if case_ii.limit(1).count() > 0:
        logger.info("Processing forward entries (Case II)...")
        self._process_case_ii(case_ii)
        logger.info("Forward processing complete")
    
    # Step 5: Final optimization
    self._optimize_tables()
    
    total_time = (time.time() - self.start_time) / 60
    logger.info(f"Incremental processing completed in {total_time:.2f} minutes")
```

#### Update parse_args()

```python
def parse_args():
    parser = argparse.ArgumentParser(
        description='Summary Pipeline v4.0 - Config-driven production scale'
    )
    
    parser.add_argument('--config', type=str, required=True,
                       help='Path to config JSON file')
    parser.add_argument('--mode', type=str, 
                       choices=['initial', 'forward', 'backfill', 'incremental'],
                       default='incremental',  # Changed default!
                       help='Processing mode (default: incremental)')
    parser.add_argument('--start-month', type=str,
                       help='Start month (YYYY-MM) for forward mode')
    parser.add_argument('--end-month', type=str,
                       help='End month (YYYY-MM) for forward mode')
    parser.add_argument('--backfill-filter', type=str,
                       help='SQL filter for backfill source data')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview without writing')
    
    return parser.parse_args()
```

#### Update main()

```python
def main():
    args = parse_args()
    
    # Load config
    config = SummaryConfig(args.config)
    
    # Create Spark session
    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("INFO")
    
    try:
        pipeline = SummaryPipeline(spark, config)
        
        if args.mode == 'incremental':
            # NEW: Production default mode
            pipeline.run_incremental()
            
        elif args.mode == 'forward' or args.mode == 'initial':
            # Parse dates
            if args.start_month:
                start_month = datetime.strptime(args.start_month, '%Y-%m').date().replace(day=1)
            else:
                start_month = date.today().replace(day=1)
            
            if args.end_month:
                end_month = datetime.strptime(args.end_month, '%Y-%m').date().replace(day=1)
            else:
                end_month = start_month
            
            is_initial = args.mode == 'initial'
            pipeline.run_forward(start_month, end_month, is_initial)
            
        elif args.mode == 'backfill':
            pipeline.run_backfill(source_filter=args.backfill_filter)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

---

## Production Usage

### Daily/Monthly Run (Recommended)

```bash
# Simple - just run with config, defaults to incremental mode
spark-submit summary_pipeline.py \
  --config summary_config.json

# Explicit incremental mode
spark-submit summary_pipeline.py \
  --config summary_config.json \
  --mode incremental
```

**What it does**:
1. Finds all new records since last run (base_ts > max existing)
2. Classifies into backfill/new/forward
3. Processes in correct order automatically
4. Handles everything in one shot

### Console Output Example

```
================================================================================
SUMMARY PIPELINE v4.0 - INCREMENTAL MODE (PRODUCTION)
================================================================================
Processing new data since: 2026-01-14 23:59:59
New records to process: 1,523,456

Classifying records...
Case I (New accounts): 1,234
Case II (Forward entries): 1,500,000
Case III (Backfill): 22,222

Processing backfill records (Case III)...
  Processing backfill with history rebuild...
  22,222 records affected 15 unique months
Backfill processing complete

Processing new accounts (Case I)...
  Creating initial history arrays...
  1,234 new accounts added
New account processing complete

Processing forward entries (Case II)...
  Shifting history arrays for forward entries...
  1,500,000 records processed
Forward processing complete

Optimizing tables...
Incremental processing completed in 12.45 minutes
================================================================================
```

---

## Option 2: Run Modes Separately (Sequenced)

If you prefer to keep modes separate but run them sequentially:

### Production Script (run_production.sh)

```bash
#!/bin/bash

CONFIG="/path/to/summary_config.json"
LOG_DIR="/path/to/logs"
DATE=$(date +%Y%m%d_%H%M%S)

echo "=========================================="
echo "Summary Pipeline Production Run"
echo "Started: $(date)"
echo "=========================================="

# Step 1: Process backfill FIRST (critical!)
echo "Step 1/2: Processing backfill..."
spark-submit summary_pipeline.py \
  --config $CONFIG \
  --mode backfill \
  2>&1 | tee "$LOG_DIR/backfill_$DATE.log"

if [ $? -ne 0 ]; then
  echo "ERROR: Backfill failed!"
  exit 1
fi

echo "Backfill complete. Waiting 30 seconds for table optimization..."
sleep 30

# Step 2: Process forward SECOND (uses updated history)
echo "Step 2/2: Processing forward entries..."
CURRENT_MONTH=$(date +%Y-%m)
spark-submit summary_pipeline.py \
  --config $CONFIG \
  --mode forward \
  --start-month $CURRENT_MONTH \
  --end-month $CURRENT_MONTH \
  2>&1 | tee "$LOG_DIR/forward_$DATE.log"

if [ $? -ne 0 ]; then
  echo "ERROR: Forward processing failed!"
  exit 1
fi

echo "=========================================="
echo "Production run completed: $(date)"
echo "=========================================="
```

**Usage**:
```bash
chmod +x run_production.sh
./run_production.sh
```

---

## Why Order Matters

### Scenario: Backfill + Forward in Same Batch

```
Existing summary (2025-12):
  Account 1: balance_am_history = [10000, 9500, 9000, ...]

New data arriving (2026-01-15):
  1. Account 1, 2025-11, balance_am=10700 (backfill - correction)
  2. Account 1, 2026-01, balance_am=11000 (forward - new month)
```

### ❌ Wrong Order: Forward THEN Backfill

```
Step 1: Process forward (2026-01)
  Read previous state: balance_am_history = [10000, 9500, ...]
  Result: [11000, 10000, 9500, ...]  ← Based on old history

Step 2: Process backfill (2025-11)
  Rebuilds history with corrected value
  2025-11: [..., 10700, ...]  ← Corrected
  2025-12: [10000, 10700, ...]  ← Corrected
  2026-01: [11000, 10000, 10700, ...]  ← WRONG! 10000 shouldn't be there
```

**Problem**: Forward processing used OLD history before backfill correction!

### ✅ Correct Order: Backfill THEN Forward

```
Step 1: Process backfill (2025-11)
  Rebuilds history with corrected value
  2025-11: [..., 10700, ...]  ← Corrected
  2025-12: [10000, 10700, ...]  ← Corrected (shifted)

Step 2: Process forward (2026-01)
  Read previous state: balance_am_history = [10000, 10700, ...]  ← CORRECTED!
  Result: [11000, 10000, 10700, ...]  ← CORRECT!
```

**Success**: Forward processing uses CORRECTED history after backfill!

---

## Production Workflow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│  New Data Arrives (base_ts = 2026-01-15)                    │
│  - 2026-01 data (forward)                                   │
│  - 2025-11 corrections (backfill)                           │
│  - New account 9999 (new)                                   │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  INCREMENTAL MODE                                            │
│                                                              │
│  Step 1: Read all data (base_ts > 2026-01-14 23:59:59)     │
│  Step 2: Classify into Cases I/II/III                       │
└─────────────────────────────────────────────────────────────┘
                           ↓
            ┌──────────────┴──────────────┐
            │     Classification          │
            └──────────────┬──────────────┘
                           ↓
         ┌─────────────────┼─────────────────┐
         ↓                 ↓                  ↓
    ┌────────┐      ┌─────────┐      ┌──────────┐
    │Case III│      │ Case I  │      │ Case II  │
    │Backfill│      │New Accts│      │ Forward  │
    └────┬───┘      └────┬────┘      └────┬─────┘
         │               │                 │
         │ PRIORITY 1    │ PRIORITY 2      │ PRIORITY 3
         │ (Process 1st) │ (Process 2nd)   │ (Process 3rd)
         ↓               ↓                 ↓
    ┌────────┐      ┌─────────┐      ┌──────────┐
    │Rebuild │      │Create   │      │Shift     │
    │History │      │Initial  │      │Arrays    │
    │Arrays  │      │Arrays   │      │(uses new)│
    └────┬───┘      └────┬────┘      └────┬─────┘
         │               │                 │
         └───────────────┴─────────────────┘
                         ↓
            ┌────────────────────────┐
            │  MERGE to summary      │
            │  MERGE to latest_summary│
            └────────────────────────┘
                         ↓
            ┌────────────────────────┐
            │  Optimize tables       │
            └────────────────────────┘
```

---

## Comparison: Modes

| Mode | Use Case | When to Use |
|------|----------|-------------|
| **incremental** | **Production default** | Daily/monthly runs with mixed data |
| **initial** | First-time load | Building summary from scratch (2016-2025) |
| **forward** | Sequential month processing | Backfilling historical gaps month-by-month |
| **backfill** | Manual corrections | Ad-hoc fixes for specific accounts/months |

---

## Production Checklist

### ✅ Before First Production Run

- [ ] Verify existing data: 2016-01 to 2025-12 exists
- [ ] Confirm latest base_ts in summary table
- [ ] Test incremental mode on small dataset first
- [ ] Set up monitoring/alerting
- [ ] Configure EMR cluster with sufficient resources
- [ ] Set up log aggregation

### ✅ Daily Production Run

```bash
# 1. Run incremental mode (handles everything)
spark-submit summary_pipeline.py --config summary_config.json --mode incremental

# 2. Validate results
spark-sql -e "
  SELECT 
    COUNT(*) as total_records,
    MAX(rpt_as_of_mo) as latest_month,
    MAX(base_ts) as latest_timestamp
  FROM default.summary
"

# 3. Check for duplicates
spark-sql -e "
  SELECT cons_acct_key, rpt_as_of_mo, COUNT(*) as cnt
  FROM default.summary
  GROUP BY cons_acct_key, rpt_as_of_mo
  HAVING COUNT(*) > 1
"
# Should return 0 rows

# 4. Verify array lengths
spark-sql -e "
  SELECT COUNT(*) 
  FROM default.summary
  WHERE SIZE(balance_am_history) != 36
"
# Should return 0
```

### ✅ Monitoring Metrics

Track these metrics per run:
- **Record counts**: Case I, Case II, Case III
- **Processing time**: Total and per case
- **Data quality**: Duplicates, NULL arrays, invalid dates
- **Performance**: Partition count, shuffle read/write

---

## Airflow DAG Example (Production)

```python
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'summary_pipeline_incremental',
    default_args=default_args,
    description='Summary Pipeline - Incremental Mode',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    catchup=False,
    max_active_runs=1,
)

# Wait for source data to be ready
wait_for_source = ExternalTaskSensor(
    task_id='wait_for_source_data',
    external_dag_id='accounts_all_ingestion',
    external_task_id='load_complete',
    timeout=3600,
    dag=dag,
)

# Run incremental mode
run_incremental = EmrAddStepsOperator(
    task_id='run_summary_incremental',
    job_flow_id='j-XXXXXXXXXXXXX',
    steps=[{
        'Name': 'Summary Pipeline Incremental',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--conf', 'spark.sql.shuffle.partitions=2000',
                '--conf', 'spark.executor.memory=100g',
                '--conf', 'spark.executor.instances=50',
                's3://your-bucket/pipelines/summary_pipeline.py',
                '--config', 's3://your-bucket/config/summary_config.json',
                '--mode', 'incremental'  # Default, but explicit
            ]
        }
    }],
    dag=dag,
)

# Validation step
validate_results = EmrAddStepsOperator(
    task_id='validate_results',
    job_flow_id='j-XXXXXXXXXXXXX',
    steps=[{
        'Name': 'Validate Summary Results',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-sql',
                '-f', 's3://your-bucket/scripts/validate_summary.sql'
            ]
        }
    }],
    dag=dag,
)

wait_for_source >> run_incremental >> validate_results
```

---

## Troubleshooting

### Issue: "No new records to process"

**Cause**: `max_ts` in summary is ahead of source data

**Solution**:
```sql
-- Check latest timestamps
SELECT MAX(base_ts) FROM default.summary;
SELECT MAX(base_ts) FROM default.default.accounts_all;

-- If summary is ahead, check for data arrival issues
```

### Issue: Forward records processed before backfill

**Symptom**: History arrays have incorrect values

**Cause**: Processing order was wrong

**Solution**: Use incremental mode (auto-sequences) or ensure backfill runs first

### Issue: Duplicate records after incremental run

**Cause**: Case classification or MERGE logic issue

**Debug**:
```sql
-- Find duplicates
SELECT cons_acct_key, rpt_as_of_mo, COUNT(*) as cnt, MAX(base_ts)
FROM default.summary
GROUP BY cons_acct_key, rpt_as_of_mo
HAVING COUNT(*) > 1;

-- Check if it's a timestamp tie
SELECT * FROM default.summary
WHERE cons_acct_key = <duplicate_key>
  AND rpt_as_of_mo = '<duplicate_month>'
ORDER BY base_ts DESC;
```

---

## Summary

### Recommended Approach: Incremental Mode

**✅ Advantages**:
- Single command for production
- Auto-detects all scenarios
- Correct processing order guaranteed
- No manual filtering needed
- Production-ready logging

**Command**:
```bash
spark-submit summary_pipeline.py --config summary_config.json
# That's it! Defaults to incremental mode
```

### Alternative: Sequenced Modes

If you need more control:
```bash
# Run 1: Backfill (corrections)
spark-submit --mode backfill

# Run 2: Forward (new month)
spark-submit --mode forward --start-month 2026-01 --end-month 2026-01
```

---

## Next Steps

1. **Implement** `run_incremental()` method (code provided above)
2. **Test** on staging with mixed data (backfill + forward + new accounts)
3. **Validate** correct sequencing and results
4. **Deploy** to production
5. **Schedule** via Airflow/cron
6. **Monitor** metrics and logs

---

**File**: `PRODUCTION_INCREMENTAL_MODE.md`  
**Version**: 4.0.0  
**Status**: Production Ready ✅  
**Last Updated**: January 2026
