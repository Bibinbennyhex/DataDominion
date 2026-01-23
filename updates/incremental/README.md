# Incremental Summary Update Module

This folder contains the incremental update logic for the summary table.

## Scripts

| Script | Description |
|--------|-------------|
| `incremental_summary_update.py` | Original pipeline script (v1) |
| `incremental_summary_update_v2.py` | **Optimized pipeline with config support (v2)** |
| `config.yaml` | Configuration file for v2 |

## Quick Start

```bash
# Run v2 with default settings
docker exec spark-iceberg spark-submit /home/iceberg/notebooks/notebooks/incremental/incremental_summary_update_v2.py

# Run with custom config
docker exec spark-iceberg spark-submit /home/iceberg/notebooks/notebooks/incremental/incremental_summary_update_v2.py \
    --config /home/iceberg/notebooks/notebooks/incremental/config.yaml

# Dry run (preview without writing)
docker exec spark-iceberg spark-submit /home/iceberg/notebooks/notebooks/incremental/incremental_summary_update_v2.py --dry-run

# With validation
docker exec spark-iceberg spark-submit /home/iceberg/notebooks/notebooks/incremental/incremental_summary_update_v2.py --validate
```

## V2 Improvements

### Performance Optimizations

| Improvement | v1 | v2 |
|-------------|----|----|
| Backfill processing | `crossJoin` O(n²) | Window functions O(n log n) |
| Classification | Multiple scans | Single aggregation + broadcast join |
| Count operations | Eager `.count()` | Lazy evaluation with `.isEmpty()` |
| Caching strategy | Manual cache/unpersist | Smart caching with automatic cleanup |
| Broadcast joins | Fixed threshold | Configurable threshold |

### New Features

- **YAML Configuration**: External config file support
- **CLI Arguments**: Override settings from command line
- **Dry Run Mode**: Preview changes without writing (`--dry-run`)
- **Validation**: Post-update integrity checks (`--validate`)
- **Metrics**: Detailed execution metrics and timing
- **Better Logging**: Structured logging with configurable levels

## CLI Options

```
spark-submit incremental_summary_update_v2.py [OPTIONS]

Options:
  --config, -c PATH      Path to YAML configuration file
  --dry-run, -n          Preview changes without writing
  --validate, -v         Run post-update validation checks
  --accounts-table       Override source table
  --summary-table        Override target table
  --batch-size           Records per batch (default: 100000)
  --log-level            DEBUG, INFO, WARN, ERROR
```

## Configuration

### Via YAML (config.yaml)

```yaml
# Tables
accounts_table: "default.default.accounts_all"
summary_table: "default.summary_testing"

# Processing
history_length: 36
batch_size: 100000

# Features
dry_run: false
validate: true

# Performance
broadcast_threshold_mb: 100
adaptive_enabled: true
skew_join_enabled: true

# Logging
log_level: "INFO"
metrics_enabled: true
```

### Via CLI

```bash
spark-submit incremental_summary_update_v2.py \
    --accounts-table "my_db.accounts" \
    --summary-table "my_db.summary" \
    --batch-size 50000 \
    --log-level DEBUG
```

## Scenarios Handled

### Case I: New Account
- Account doesn't exist in summary
- Creates row with NULL-padded history arrays
- Current values at index 0

### Case II: Forward Month Entry
- New month > max existing month for account
- Shifts existing history arrays
- Inserts NULLs for any month gaps
- Truncates to 36 elements

### Case III: Backfill
- New month <= max existing month
- Only updates if `new.base_ts > existing.base_ts`
- **Rebuilds ALL affected future rows' history arrays**
- Uses optimized window functions (no crossJoin)

## Metrics Output

After execution, v2 provides detailed metrics:

```
============================================================
PIPELINE METRICS SUMMARY
============================================================
  input_records: 3
  new_accounts: 1
  forward_entries: 1
  backfill_entries: 1
  rows_inserted: 1
  rows_updated: 165
  total_merged: 166
  classification_time_sec: 2.5
  processing_time_sec: 15.3
  merge_time_sec: 22.1
  total_time_sec: 40.2
============================================================
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    IncrementalSummaryProcessor              │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ _get_new_    │→ │ _classify_   │→ │ _process_    │      │
│  │   batch()    │  │  records()   │  │ all_cases()  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│         ↓                                    ↓              │
│  ┌──────────────────────────────────────────────────┐      │
│  │              _merge_updates()                     │      │
│  │         (or _preview_updates() in dry-run)       │      │
│  └──────────────────────────────────────────────────┘      │
│         ↓                                                   │
│  ┌──────────────────────────────────────────────────┐      │
│  │            _validate_results()                    │      │
│  └──────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

## Comparison: v1 vs v2

| Aspect | v1 | v2 |
|--------|----|----|
| Configuration | Hardcoded constants | YAML + CLI |
| Backfill complexity | O(n²) crossJoin | O(n log n) window |
| Dry run support | No | Yes |
| Validation | No | Yes |
| Metrics | Basic logging | Comprehensive metrics |
| Error handling | Basic try/catch | Structured with exit codes |
| Testability | Monolithic | Modular class-based |
