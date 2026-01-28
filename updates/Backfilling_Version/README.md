# Iceberg History Backfill System

A production-ready PySpark system for backfilling historical array columns in Apache Iceberg tables at massive scale (500B+ rows).

## Overview

This system handles the complex task of backfilling historical data into array columns while maintaining:
- Position-aware updates (newest data at index 0)
- 36-month rolling history windows
- Efficient processing of 500 billion row tables
- Bucketed Iceberg table optimization
- Comprehensive audit trails and recovery

## Problem Statement

Given a table with array history columns:

```sql
CREATE TABLE accounts_history (
    acct BIGINT,
    month_partition STRING,
    balance_history ARRAY<DECIMAL(18,2)>,  -- [current_month, month-1, month-2, ...]
    payment_history ARRAY<DECIMAL(18,2)>,
    -- ... 18 more history columns
) 
USING iceberg
PARTITIONED BY (month_partition)
CLUSTERED BY (acct) INTO 64 BUCKETS
```

When backfilling month `2023-12`, the system must:
1. Update all partitions after `2023-12` (within 36-month window)
2. Insert the backfill value at the correct position based on month offset
3. Process only affected accounts (not entire table)
4. Maintain array size of 36 elements

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    BACKFILL SYSTEM                          │
├─────────────────────────────────────────────────────────────┤
│  1. Backfill Staging Table (source of truth)               │
│  2. Core Processor (position calculation, array updates)   │
│  3. Merge Engine (partition-aware Iceberg MERGE)           │
│  4. Batch Orchestrator (retry, checkpointing, parallelism) │
│  5. Audit & Monitoring (tracking, verification)            │
└─────────────────────────────────────────────────────────────┘
```

## Project Structure

```
.
├── config.py                    # Configuration and column definitions
├── backfill_infrastructure.py   # Infrastructure setup (tables)
├── backfill_processor.py        # Core processing logic
├── backfill_merge.py           # Partition-wise MERGE operations
├── backfill_sql.py             # SQL-based alternative approach
├── backfill_batch.py           # Batch orchestration and retry logic
├── utils.py                    # Utility functions (audit, verify, report)
├── run_backfill.py             # Main entry point
└── README.md                   # This file
```

## Installation

### Prerequisites

- Apache Spark 3.3+
- Apache Iceberg 1.0+
- PySpark
- Python 3.8+

### Setup

1. Install dependencies:

```bash
pip install pyspark
```

2. Configure Spark with Iceberg extensions (see `run_backfill.py`):

```python
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .getOrCreate()
```

## Quick Start

### 1. Setup Infrastructure

Create staging and audit tables:

```bash
python run_backfill.py setup --sample-data
```

This creates:
- `backfill_staging` - Stores backfill data
- `backfill_audit` - Tracks execution history

### 2. Load Backfill Data

Insert data into `backfill_staging`:

```python
INSERT INTO backfill_staging VALUES (
    'bf-2024-001',           -- backfill_id
    12345,                   -- acct
    '2023-12',              -- backfill_month
    1500.00,                -- balance
    200.00,                 -- payment
    -- ... values for all 20 columns
    current_timestamp(),
    'pending',
    NULL,
    NULL
);
```

### 3. Estimate Time

Before running, estimate processing time:

```bash
python run_backfill.py estimate \
    --target-table catalog.db.accounts_history \
    --backfill-month 2023-12
```

### 4. Run Backfill

Execute the backfill:

```bash
python run_backfill.py backfill \
    --target-table catalog.db.accounts_history \
    --backfill-id bf-2024-001 \
    --backfill-month 2023-12 \
    --method merge \
    --max-parallel 4 \
    --report \
    --verify
```

### 5. Check Status

Monitor progress:

```bash
python run_backfill.py status --backfill-id bf-2024-001
```

### 6. Resume on Failure

If a job fails, resume from last successful partition:

```bash
python run_backfill.py backfill \
    --target-table catalog.db.accounts_history \
    --backfill-id bf-2024-001 \
    --backfill-month 2023-12 \
    --resume-from 2024-05
```

## Execution Methods

### Method 1: Partition-wise MERGE (Recommended)

**Best for**: Production, 500B+ rows, maximum reliability

```bash
--method merge
```

**Features**:
- Processes one partition at a time
- Leverages Iceberg bucketing for efficient joins
- Built-in checkpointing and retry
- Minimal memory footprint

**Performance**: 2-4 hours for 10K accounts across 36 partitions

### Method 2: SQL-based MERGE

**Best for**: Simpler cases, manual SQL execution

```bash
--method sql
```

**Features**:
- Pure SQL implementation
- Easier to understand and debug
- Can be run from any SQL interface

**Performance**: 3-6 hours for same workload

## Configuration

Edit `config.py` to customize:

```python
# Define your 20 history columns
HISTORY_COLUMNS = [
    HistoryColumn("balance_history", "balance"),
    HistoryColumn("payment_history", "payment"),
    # ... add all 20
]

# Adjust Spark configs for your cluster
SPARK_CONFIGS = {
    "spark.sql.shuffle.partitions": "20000",  # Adjust based on cluster size
    "spark.sql.adaptive.enabled": "true",
    # ...
}

# Processing parameters
MAX_HISTORY_MONTHS = 36
MAX_PARALLEL_PARTITIONS = 4
RETRY_COUNT = 3
```

## Monitoring & Verification

### View Audit Trail

```sql
SELECT 
    backfill_id,
    target_partition,
    status,
    rows_updated,
    duration_seconds,
    started_at,
    completed_at
FROM backfill_audit
WHERE backfill_id = 'bf-2024-001'
ORDER BY target_partition;
```

### Verify Backfill

Check that values were inserted correctly:

```sql
SELECT 
    acct,
    month_partition,
    balance_history[0] as current_month,
    balance_history[5] as month_minus_5,  -- Should contain backfill if partition is 2024-05
    balance_history
FROM catalog.db.accounts_history
WHERE acct = 12345
  AND month_partition >= '2024-01'
LIMIT 5;
```

### Generate Report

```python
from utils import generate_backfill_report

report = generate_backfill_report(spark, 'bf-2024-001')
print(report)
```

## Performance Tuning

### For 500B Row Tables

1. **Partition Strategy**: Process partitions sequentially to avoid resource contention
2. **Broadcast Join**: Backfill data is small - use broadcast for efficient joins
3. **Bucket Pruning**: Iceberg automatically uses 64 buckets for join optimization
4. **Adaptive Query Execution**: Enable AQE for dynamic optimization

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### Estimated Performance

| Accounts | Partitions | Method | Est. Time |
|----------|------------|--------|-----------|
| 1K       | 36         | MERGE  | 30-60 min |
| 10K      | 36         | MERGE  | 2-4 hours |
| 100K     | 36         | MERGE  | 6-12 hours|
| 1M       | 36         | MERGE  | 24-48 hours|

*Based on 500B row table, 64 buckets, standard Spark cluster*

## Advanced Usage

### Programmatic Usage

```python
from run_backfill import create_spark_session
from backfill_processor import IcebergBackfillProcessor
from backfill_batch import BatchBackfillOrchestrator
from config import HISTORY_COLUMNS

spark = create_spark_session()

processor = IcebergBackfillProcessor(
    spark=spark,
    target_table="catalog.db.accounts_history",
    history_columns=HISTORY_COLUMNS
)

orchestrator = BatchBackfillOrchestrator(processor)

result = orchestrator.run_backfill(
    backfill_id="bf-001",
    backfill_month="2023-12"
)

print(result)
```

### Multiple Backfills

```python
from backfill_batch import run_parallel_backfills

backfills = [
    {"backfill_id": "bf-001", "backfill_month": "2023-12"},
    {"backfill_id": "bf-002", "backfill_month": "2023-11"},
    {"backfill_id": "bf-003", "backfill_month": "2023-10"},
]

results = run_parallel_backfills(processor, backfills)
```

### Generate SQL Script

For manual review or execution:

```python
from backfill_sql import generate_sql_script
from config import HISTORY_COLUMNS

generate_sql_script(
    target_table="catalog.db.accounts_history",
    backfill_month="2023-12",
    backfill_id="bf-001",
    history_columns=HISTORY_COLUMNS,
    output_file="backfill_2023_12.sql"
)
```

## Troubleshooting

### Out of Memory Errors

- Reduce `MAX_PARALLEL_PARTITIONS` to 1-2
- Increase executor memory
- Process fewer partitions per batch

### Slow Performance

- Check if Iceberg bucketing is working: look for "bucket pruning" in Spark UI
- Verify AQE is enabled
- Increase `spark.sql.shuffle.partitions`
- Consider increasing cluster resources

### Failed Partitions

- Use `--resume-from` to skip completed partitions
- Check `backfill_audit` table for error messages
- Verify data quality in `backfill_staging`

### Data Validation Issues

```python
from utils import verify_backfill

verify_result = verify_backfill(
    spark=spark,
    target_table="catalog.db.accounts_history",
    backfill_id="bf-001",
    backfill_month="2023-12",
    sample_accounts=[12345, 67890]
)

verify_result.show()
```

## Best Practices

1. **Test on Small Dataset First**: Run on 1-2 partitions before full execution
2. **Monitor Progress**: Check `backfill_audit` regularly during execution
3. **Verify Results**: Always use `--verify` flag or manual verification
4. **Checkpoint Regularly**: The system auto-checkpoints, but monitor for failures
5. **Clean Old Audits**: Periodically clean up old audit records (90+ days)

```python
from utils import cleanup_old_audits
cleanup_old_audits(spark, days_to_keep=90)
```

## Comparison of Approaches

| Approach | Time (500B rows) | Complexity | Recovery | Best For |
|----------|------------------|------------|----------|----------|
| Partition MERGE | 2-4 hrs | Medium | Excellent | Production |
| SQL MERGE | 3-6 hrs | Low | Good | Simple cases |
| Full Rewrite | Days | Low | Poor | Last resort |
| UPDATE | 12-24 hrs | Low | Good | Small updates |

## Contributing

When adding new history columns:

1. Update `HISTORY_COLUMNS` in `config.py`
2. Update `backfill_staging` table schema in `backfill_infrastructure.py`
3. Test with sample data

## License

MIT

## Support

For issues or questions:
- Check the troubleshooting section
- Review audit logs in `backfill_audit` table
- Enable DEBUG logging: `--log-level DEBUG`
