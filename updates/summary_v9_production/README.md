# Summary Pipeline v9.1 Production

Production-ready DPD Summary Pipeline with fixed backfill logic.

## Version

**v9.1-fixed** (January 2026)

## Bug Fixed

The original v9 pipeline had a critical bug in Case III (backfill) processing:
- **Bug**: When late-arriving data came in, it only updated future summary rows but did NOT create a summary row for the backfill month itself
- **Fix**: Now correctly creates a new summary row for the backfill month with inherited historical data from the closest prior summary

## Folder Structure

```
summary_v9_production/
├── README.md                  # This file
├── pipeline/
│   └── summary_pipeline.py    # Main pipeline code (1241 lines)
├── config/
│   └── pipeline_config.json   # Full production configuration
├── tests/
│   ├── run_backfill_test.py   # Backfill cascade test
│   └── test_config.json       # Test configuration
├── deployment/
│   ├── emr_cluster.json       # EMR cluster configuration
│   ├── spark_submit.sh        # spark-submit script
│   └── spark_defaults.conf    # Spark configuration
└── docs/
    ├── PERFORMANCE.md         # Performance analysis
    └── CHANGELOG.md           # Version history
```

## Quick Start

### 1. Run on EMR

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.yarn.maxAppAttempts=2 \
  pipeline/summary_pipeline.py \
  --config config/pipeline_config.json \
  --mode incremental
```

### 2. Run Locally (Docker)

```bash
# Copy files to container
docker cp pipeline/summary_pipeline.py spark-iceberg:/home/iceberg/pipeline/
docker cp config/pipeline_config.json spark-iceberg:/home/iceberg/pipeline/

# Run pipeline
docker exec spark-iceberg python3 /home/iceberg/pipeline/summary_pipeline.py \
  --config /home/iceberg/pipeline/pipeline_config.json
```

### 3. Run Tests

```bash
docker exec spark-iceberg python3 /home/iceberg/test/run_backfill_test.py
```

## Features

| Feature | Description |
|---------|-------------|
| **Case I** | New accounts - creates initial 36-element arrays |
| **Case II** | Forward entries - shifts arrays, fills gaps with NULL |
| **Case III** | Backfill - creates backfill month row + updates future summaries |
| **Column Mappings** | 36 source-to-destination column mappings |
| **Transformations** | 13 sentinel value transformations (-2147483647 → NULL) |
| **Inferred Columns** | 2 derived columns (orig_loan_am, payment_rating_cd) |
| **Rolling Arrays** | 7 history arrays (36 months each) |
| **Grid Columns** | Payment history grid generation |
| **Separate Outputs** | Different columns for summary vs latest_summary |

## Processing Order

**Critical for correctness:**

1. **Backfill (Case III)** - FIRST - rebuilds history with corrections
2. **New Accounts (Case I)** - SECOND - no dependencies  
3. **Forward (Case II)** - LAST - uses corrected history from backfill

## Configuration

See `config/pipeline_config.json` for full configuration including:
- Table names
- Column mappings
- Transformations
- Rolling column definitions
- Performance tuning
- Spark settings

## Performance

| Scenario | Records | Estimated Time |
|----------|---------|----------------|
| 50M new accounts | 50M | ~10 min |
| 750M forward entries | 750M | ~30 min |
| 200M backfill | 200M | ~75 min |
| **Total mixed workload** | 1B | **~2 hours** |

See `docs/PERFORMANCE.md` for detailed analysis.

## EMR Cluster Recommendation

| Component | Specification |
|-----------|---------------|
| Master | 1x r6i.4xlarge |
| Core (On-Demand) | 20x r6i.8xlarge |
| Task (Spot) | 30x r6i.8xlarge |
| Total vCPU | 1,600 |
| Total RAM | 12.8 TB |
| Cost/run | ~$130 |

See `deployment/emr_cluster.json` for full configuration.
