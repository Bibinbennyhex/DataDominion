# Summary Pipeline v9.4 Production

Production-ready DPD Summary Pipeline with full support for all 4 case types, gap handling, temp table processing, and optimized I/O.

## Version

**v9.4.1** (February 2026) - *Production Stable*

> **Note**: A candidate release **v9.4.2** is available in `pipeline/summary_pipeline_v9_4_2.py`.

## What's New in v9.4.2 (Candidate)

### Features & Optimizations
- **Chained Backfill Support**: Correctly handles consecutive missing months arriving in a single batch (e.g., April, May, June).
- **Map-Lookup Optimization**: Replaces expensive self-joins with a high-performance Window Map lookup strategy.
    - **Logic**: Collects all backfill values for an account into a `MAP<Month, Struct>` using a Window function.
    - **Projection**: Uses `transform` + `peer_map[key]` to fill gaps instantly during row creation.
    - **Performance**: ~35% faster than Join-based patching for backfill batches.
- **Non-Continuous Support**: robustly handles batches with internal gaps (e.g., Feb, April, June).

## What's New in v9.4.1

### Critical Fixes
- **Multiple Backfill Merge**: Fixed issue where multiple backfills for the same account in the same batch were not all propagated to future months.
- **Type-Aware Aggregation**: Fixed SQL generation for STRING columns in backfill processing.

### New Tests
- **Duplicate Record Handling**: Added `test_duplicate_records.py` to verify that latest `base_ts` wins.
- **Consecutive Backfill Test**: Added `test_consecutive_backfill.py` to verify chaining logic.

## What's New in v9.4

### Temp Table Processing Architecture
- **Temp Table Approach**: Each case writes to temporary Iceberg table, breaking DAG lineage
- **Unique Batch ID**: Format `{schema}.pipeline_batch_{timestamp}_{uuid}` prevents collisions
- **Auto Cleanup**: `try/finally` ensures temp table dropped on success/failure
- **Orphan Cleanup**: `cleanup_orphaned_temp_tables()` for crashed runs
- **Legacy Mode**: `--legacy` flag for v9.3-style in-memory union

### Benefits
- Significantly reduced driver memory pressure
- Improved reliability for large batches (1B+ records)
- No more driver OOM errors

### Previous Versions
- **v9.3**: Partition pruning, single MERGE, filter pushdown (50-90% I/O reduction)
- **v9.2.1**: Fixed Case IV gap handling (MAP+TRANSFORM approach)
- **v9.2**: Added Case IV for bulk historical loads
- **v9.1**: Fixed Case III backfill row creation

## Case Processing

| Case | Scenario | Example |
|------|----------|---------|
| **Case I** | New account, single month | Brand new customer, first report |
| **Case II** | Existing account, forward month | Monthly report for existing customer |
| **Case III** | Backfill (late-arriving data) | Historical correction for existing customer |
| **Case IV** | Bulk historical load | New customer with 72 months of history |

## Bug Fixes

| Version | Bug Fixed |
|---------|-----------|
| v9.1 | Case III didn't create row for backfill month itself |
| v9.2 | Case IV not handled (all bulk records were Case I) |
| v9.2.1 | Case IV gaps not preserved (COLLECT_LIST skipped gaps) |
| v9.4.1 | Multiple backfills in same batch not fully propagated |
| v9.4.1 | Type mismatch for STRING columns in backfill aggregation |

## Folder Structure

```
summary_v9_production/
├── README.md                           # This file
├── pipeline/
│   └── summary_pipeline.py             # v9.4.1 - Temp table optimized (~1500 lines)
├── config/
│   └── pipeline_config.json            # Full production configuration
├── tests/
│   ├── run_backfill_test.py            # Case III backfill test
│   ├── test_bulk_historical_load.py    # Case IV 72-month test
│   ├── test_all_scenarios.py           # All 4 cases + gaps (25 tests)
│   ├── test_comprehensive_50_cases.py  # Extended scenarios (52 tests)
│   ├── test_comprehensive_edge_cases.py # Extended edge cases (34 tests)
│   ├── test_duplicate_records.py       # Duplicate handling (14 tests)
│   ├── test_performance_benchmark.py   # Performance benchmarking
│   └── test_config.json                # Test configuration
├── deployment/
│   ├── emr_cluster.json                # EMR cluster configuration
│   ├── spark_submit.sh                 # spark-submit script
│   ├── spark_defaults.conf             # Spark configuration
│   └── bootstrap.sh                    # EMR bootstrap script
└── docs/
    ├── DESIGN_DOCUMENT.md              # Technical design (v9.4)
    ├── PERFORMANCE.md                  # Performance analysis
    ├── CHANGELOG.md                    # Version history
    ├── CASE_IV_BULK_HISTORICAL_LOAD.md # Case IV documentation
    └── TEST_CASES.md                   # Test case documentation
```

## Quick Start

### 1. Run on EMR (Default - Temp Table)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.yarn.maxAppAttempts=2 \
  pipeline/summary_pipeline.py \
  --config config/pipeline_config.json \
  --mode incremental
```

### 2. Run on EMR (Legacy - In-Memory Union)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  pipeline/summary_pipeline.py \
  --config config/pipeline_config.json \
  --mode incremental \
  --legacy
```

### 3. Cleanup Orphaned Temp Tables

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  pipeline/summary_pipeline.py \
  --config config/pipeline_config.json \
  --cleanup-orphans \
  --max-age-hours 24
```

### 4. Run Locally (Docker)

```bash
# Copy files to container
cat pipeline/summary_pipeline.py | docker exec -i spark-iceberg tee /home/iceberg/summary_v9_production/pipeline/summary_pipeline.py > /dev/null

# Run comprehensive test
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_all_scenarios.py
```

### 5. Run Tests

```bash
# All scenarios (25 tests)
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_all_scenarios.py

# Extended 52-test suite
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_comprehensive_50_cases.py

# Edge cases (40+ tests)
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_comprehensive_edge_cases.py

# Performance benchmark
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_performance_benchmark.py --scale SMALL
```

## Features

| Feature | Description |
|---------|-------------|
| **Case I** | New accounts - creates initial 36-element arrays |
| **Case II** | Forward entries - shifts arrays, fills gaps with NULL |
| **Case III** | Backfill - creates backfill month row + updates future summaries |
| **Case IV** | Bulk historical - builds rolling arrays for multi-month uploads |
| **Gap Handling** | Non-reporting months correctly show as NULL in arrays |
| **Column Mappings** | 36 source-to-destination column mappings |
| **Transformations** | 13 sentinel value transformations (-2147483647 → NULL) |
| **Inferred Columns** | 2 derived columns (orig_loan_am, payment_rating_cd) |
| **Rolling Arrays** | 7 history arrays (36 months each) |
| **Grid Columns** | Payment history grid generation |
| **Separate Outputs** | Different columns for summary vs latest_summary |

## Processing Order

**Critical for correctness:**

1. **Case III (Backfill)** - FIRST - rebuilds history with corrections
2. **Case I (New Accounts)** - SECOND - no dependencies  
3. **Case IV (Bulk Historical)** - THIRD - uses Case I first months
4. **Case II (Forward)** - LAST - uses corrected history

## Gap Handling Example

```
Account 5001 uploads: Jan 2025, Mar 2025 (Feb MISSING)

Array for Mar 2025:
  bal[0] = 5400  (Mar - current month)
  bal[1] = NULL  (Feb - gap, correctly NULL!)
  bal[2] = 6000  (Jan - 2 months ago)
```

## Performance

### v9.3 Optimizations

| Optimization | Impact |
|--------------|--------|
| Case III Partition Pruning | 50-90% I/O reduction for backfill |
| Single Consolidated MERGE | 4x less write amplification |
| Case II Filter Pushdown | Iceberg data file pruning |

### Estimated Processing Times (50 nodes)

| Scenario | Records | Estimated Time |
|----------|---------|----------------|
| 50M new accounts | 50M | ~10 min |
| 750M forward entries | 750M | ~25 min |
| 200M backfill | 200M | ~50 min |
| 100M bulk historical | 100M | ~45 min |
| **Total mixed workload** | 1.1B | **~2.5 hours** |

See `docs/PERFORMANCE.md` for detailed analysis and benchmarks.

## EMR Cluster Recommendation

| Component | Specification |
|-----------|---------------|
| Master | 1x r6i.4xlarge |
| Core (On-Demand) | 20x r6i.8xlarge |
| Task (Spot) | 30x r6i.8xlarge |
| Total vCPU | 1,600 |
| Total RAM | 12.8 TB |
| Cost/run | ~$150 |

See `deployment/emr_cluster.json` for full configuration.

## Configuration

See `config/pipeline_config.json` for full configuration including:
- Table names
- Column mappings
- Transformations
- Rolling column definitions
- Performance tuning
- Spark settings

## Test Results (v9.4 - All Pass)

```
test_all_scenarios.py:              25/25 PASSED
test_comprehensive_50_cases.py:     52/52 PASSED
test_comprehensive_edge_cases.py:   40+/40+ PASSED
test_bulk_historical_load.py:       15/15 PASSED
test_performance_benchmark.py:      Completes successfully at all scales
```

### v9.4 Temp Table Verification

Log evidence from test runs:
- `Created temp table: demo.temp.pipeline_batch_20260131_143052_a1b2c3d4` (Temp table created)
- `Wrote 500 Case I records to temp table` (Case I materialized)
- `Wrote 250 Case II records to temp table` (Case II materialized)
- `Dropped temp table: demo.temp.pipeline_batch_...` (Cleanup successful)

### v9.3 Optimization Verification (Retained)

Log evidence from test runs:
- `Applied partition filter: rpt_as_of_mo BETWEEN '2022-11' AND '2028-11'` (Case III partition pruning)
- `Using SQL subquery for filter pushdown (Iceberg optimization)` (Case II filter pushdown)
- `WRITING ALL RESULTS (single MERGE operation)` (Single consolidated MERGE)

## Documentation

- [DESIGN_DOCUMENT.md](docs/DESIGN_DOCUMENT.md) - Technical design document (v9.4)
- [CHANGELOG.md](docs/CHANGELOG.md) - Version history and changes
- [PERFORMANCE.md](docs/PERFORMANCE.md) - Performance analysis and benchmarks
- [CASE_IV_BULK_HISTORICAL_LOAD.md](docs/CASE_IV_BULK_HISTORICAL_LOAD.md) - Case IV details
- [TEST_CASES.md](docs/TEST_CASES.md) - Complete test case documentation
