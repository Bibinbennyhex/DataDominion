# Summary Pipeline v4.0

A production-grade, config-driven pipeline for maintaining account summary tables with 36-month rolling history arrays.

## Features

- **Config-driven**: All columns, transformations, and rolling arrays defined in JSON
- **Pure Spark SQL + DataFrame API**: No UDFs for maximum scale (500B+ records)
- **Three processing modes**:
  - `initial`: First-time load from scratch
  - `forward`: Month-by-month sequential processing (production default)
  - `backfill`: Late-arriving data with history rebuild
- **Two-table architecture**: `summary` + `latest_summary` for optimal query performance
- **Grid column generation**: Payment history grid from rolling arrays

## Files

| File | Description |
|------|-------------|
| `summary_config.json` | Configuration file (columns, transformations, rolling arrays) |
| `summary_pipeline.py` | Main pipeline script |
| `setup_tables.py` | Table creation script |
| `generate_test_data.py` | Test data generator |
| `run_test.py` | End-to-end test runner |

## Quick Start

### 1. Setup Tables

```bash
docker exec spark-iceberg spark-submit \
  /home/iceberg/notebooks/notebooks/summary_v4/setup_tables.py \
  --config /home/iceberg/notebooks/notebooks/summary_v4/summary_config.json \
  --drop-existing
```

### 2. Generate Test Data

```bash
docker exec spark-iceberg spark-submit \
  /home/iceberg/notebooks/notebooks/summary_v4/generate_test_data.py \
  --config /home/iceberg/notebooks/notebooks/summary_v4/summary_config.json \
  --accounts 10000 \
  --months 12 \
  --start-month 2024-01
```

### 3. Run Pipeline (Forward Mode)

```bash
docker exec spark-iceberg spark-submit \
  /home/iceberg/notebooks/notebooks/summary_v4/summary_pipeline.py \
  --config /home/iceberg/notebooks/notebooks/summary_v4/summary_config.json \
  --mode forward \
  --start-month 2024-01 \
  --end-month 2024-12
```

### 4. Run Pipeline (Backfill Mode)

```bash
docker exec spark-iceberg spark-submit \
  /home/iceberg/notebooks/notebooks/summary_v4/summary_pipeline.py \
  --config /home/iceberg/notebooks/notebooks/summary_v4/summary_config.json \
  --mode backfill
```

## Configuration

The `summary_config.json` file defines:

### Core Settings
```json
{
  "source_table": "default.default.accounts_all",
  "destination_table": "default.summary",
  "latest_history_table": "default.latest_summary",
  "partition_column": "rpt_as_of_mo",
  "primary_column": "cons_acct_key",
  "history_length": 36
}
```

### Column Mappings
Maps source column names to destination names:
```json
{
  "columns": {
    "cons_acct_key": "cons_acct_key",
    "acct_bal_am": "balance_am",
    "days_past_due_ct_4in": "days_past_due"
  }
}
```

### Rolling Columns
Defines rolling history arrays:
```json
{
  "rolling_columns": [
    {
      "name": "balance_am",
      "mapper_expr": "CASE WHEN balance_am < 0 THEN NULL ELSE balance_am END",
      "mapper_column": "balance_am",
      "num_cols": 36,
      "type": "Integer"
    }
  ]
}
```

### Grid Columns
Defines string grids from rolling arrays:
```json
{
  "grid_columns": [
    {
      "name": "payment_history_grid",
      "mapper_rolling_column": "payment_rating_cd",
      "placeholder": "?",
      "separator": ""
    }
  ]
}
```

## Architecture

### Tables

1. **accounts_all** (Source)
   - Raw account data with monthly snapshots
   - Partitioned by `rpt_as_of_mo`

2. **summary** (Main)
   - Full summary with rolling history arrays
   - One row per account per month
   - Partitioned by `rpt_as_of_mo`

3. **latest_summary** (Lookup)
   - Only latest state per account
   - Used for fast joins during processing
   - Not partitioned

### Processing Cases

| Case | Condition | Action |
|------|-----------|--------|
| I | New account | Create row with `[value, NULL, NULL, ...]` |
| II | Forward entry (`new_month > max_existing`) | Shift: `[new, gap_nulls, prev[0:n]]` |
| III | Backfill (`new_month <= max_existing` and newer timestamp) | Rebuild entire history |

## Production Deployment

For production on AWS EMR/Glue:

1. Update config with production table names:
```json
{
  "source_table": "spark_catalog.edf_gold.ivaps_consumer_accounts_all",
  "destination_table": "ascend_iceberg.ascenddb.summary",
  "latest_history_table": "ascend_iceberg.ascenddb.latest_summary"
}
```

2. Store config in S3 and update pipeline to load from S3

3. Submit job:
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.shuffle.partitions=2000 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.maxExecutors=200 \
  summary_pipeline.py \
  --config s3://bucket/config/summary_config.json \
  --mode forward \
  --start-month 2024-01 \
  --end-month 2024-12
```

## Performance Tuning

### Key Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `shuffle_partitions` | 2000 | For 500B scale |
| `target_records_per_partition` | 10M | Records per partition |
| `max_file_size_mb` | 256 | Target Parquet file size |
| `broadcast_threshold_mb` | 50 | Broadcast join threshold |

### Optimization

- Uses Adaptive Query Execution (AQE)
- Dynamic partition pruning
- Skew join handling
- Periodic table optimization (rewrite_data_files, expire_snapshots)

## Comparison with Production Code

| Feature | Production (summary.py) | v4 (this) |
|---------|------------------------|-----------|
| Config-driven | Yes | Yes |
| Rolling columns | Yes | Yes |
| Grid generation | Yes | Yes |
| Two-table architecture | Yes | Yes |
| Backfill support | No | Yes (Case III) |
| Gap handling | Yes | Yes |
| Iceberg optimization | Yes | Yes |
