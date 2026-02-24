# Summary Pipeline — `main`

Incremental PySpark pipeline that maintains **36-month rolling history arrays** for ~2 billion consumer credit accounts. Processes monthly batches from a source Iceberg table and writes to a denormalized summary table optimized for downstream credit bureau queries.

---

## Quick Start

### Production (EMR)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.primary_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.primary_catalog.type=glue \
  summary_inc.py --config config.json
```

### Local Docker (Testing)

```bash
# Start environment
docker compose -f docker_test/docker-compose.yml up -d

# Run all tests
docker compose -f docker_test/docker-compose.yml exec spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/run_all_tests.py

# Run a specific test
docker compose -f docker_test/docker-compose.yml exec spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/test_main_all_cases.py
```

---

## How It Works

The pipeline classifies each incoming record into one of four cases, processes them independently, and merges results back into Iceberg tables.

### Case Classification

| Case | Condition | Frequency | I/O Pattern |
|------|-----------|-----------|-------------|
| **Case I** | New account, single month | ~5% | Write only |
| **Case II** | Existing account, month > latest | ~90% | Join latest_summary |
| **Case III** | Existing account, month ≤ latest (backfill) | ~4% | Read full history |
| **Case IV** | New account, multiple months in batch | ~1% | Window on batch only |

### Processing Order

```
cleanup() → load_and_classify_accounts()
  → process_case_iii()
  → process_case_i()
  → process_case_iv()       (if any)
  → write_backfill_results()
  → process_case_ii()
  → write_forward_results()
  → refresh_watermark_tracker()
```

> ⚠️ **Processing order is critical.** Violating it corrupts history arrays.

### Idempotency

The pipeline is **safe to re-run**. A watermark tracker table records the last successfully committed run. On failure, the watermark is not advanced — the next run picks up from the same point with no double-processing.

---

## Project Structure

```
main/
├── summary_inc.py              # Pipeline entry point (~80 KB, all processing logic)
├── config.json                 # Full configuration (tables, columns, transforms, Spark)
├── docker_test/
│   ├── docker-compose.yml      # Local Spark + Iceberg environment
│   ├── tests/                  # All test suites
│   │   ├── test_main_all_cases.py
│   │   ├── test_idempotency.py
│   │   ├── test_main_base_ts_propagation.py
│   │   ├── test_latest_summary_consistency.py
│   │   ├── test_aggressive_idempotency.py
│   │   ├── test_case3_current_max_month.py
│   │   ├── test_consecutive_backfill.py
│   │   ├── test_non_continuous_backfill.py
│   │   ├── test_long_backfill_gaps.py
│   │   ├── test_null_update_case_iii.py
│   │   ├── test_null_update_other_cases.py
│   │   ├── test_performance_benchmark.py
│   │   ├── scenario_suite.py           # 50+ shared scenarios
│   │   ├── test_utils.py               # Helpers (build_source_row, history, etc.)
│   │   └── run_all_tests.py            # Sequential runner
│   ├── airflow/                # DAG definitions
│   └── notebooks/              # Exploratory notebooks
├── production_docs/
│   ├── DESIGN_DOCUMENT.md      # Architecture, case logic, diagrams
│   ├── PERFORMANCE.md          # Benchmarks, bottleneck analysis, optimization plan
│   ├── TEST_CASES.md           # Per-case test scenarios with examples
│   └── CHANGELOG.md            # Version history and patch notes
└── .gitignore
```

---

## Tables

| Table | Purpose | Size | Partitioning |
|-------|---------|------|-------------|
| `accounts_all` | Raw monthly account snapshots (source) | ~220 GB/month | `rpt_as_of_mo` |
| `summary` | Denormalized with 36-month rolling arrays | **8 TB** | `rpt_as_of_mo`, 64 buckets by `cons_acct_key` |
| `latest_summary` | Latest row per account | ~800 GB | Bucketed |
| `watermark_tracker` | Pipeline run state & idempotency | 3 rows | None |

### Rolling History Arrays (7 columns)

Each summary record contains arrays of length 36:

| Column | Type |
|--------|------|
| `actual_payment_am_history` | Integer[36] |
| `balance_am_history` | Integer[36] |
| `credit_limit_am_history` | Integer[36] |
| `past_due_am_history` | Integer[36] |
| `payment_rating_cd_history` | String[36] |
| `days_past_due_history` | Integer[36] |
| `asset_class_cd_4in_history` | String[36] |

```
balance_am_history = [current, prev_1, prev_2, ..., prev_35]
                      ▲ Position 0 = current month
                      Position 35 = 35 months ago
```

---

## Configuration

All configuration lives in `config.json`:

| Section | Key Fields |
|---------|-----------|
| Tables | `source_table`, `destination_table`, `latest_history_table` |
| Schema | `primary_column` (cons_acct_key), `partition_column` (rpt_as_of_mo), `history_length` (36) |
| Mapping | `columns` — source → destination rename map (~35 entries) |
| Transforms | `column_transformations` — sentinel → NULL rules |
| Derived | `inferred_columns` — calculated columns (orig_loan_am, payment_rating_cd) |
| Arrays | `rolling_columns` — 7 array definitions with mapper_expr + type |
| Spark | `spark.*` — full Spark/Iceberg/S3 config block |

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| Processing Engine | Apache Spark 3.5+ |
| Table Format | Apache Iceberg (MOR) |
| Storage | AWS S3 |
| Catalog | AWS Glue |
| Orchestration | AWS EMR / Airflow |

---

## Key Design Decisions

1. **Case-wise processing** — minimizes I/O at 500B+ record scale. A unified approach would require loading existing data for all records (terabytes of unnecessary reads).

2. **Per-case temp tables** — each case writes to `temp_catalog.checkpointdb.case_*`, breaking Spark DAG lineage and enabling selective reads during the write phase.

3. **Chunked Case III merge** — backfill merges are split into balanced month-chunks via greedy bin-packing, preventing oversized Iceberg commits.

4. **Watermark tracker** — committed watermark = `MIN(summary.max_ts, latest_summary.max_ts)`. Advances only after a clean end-to-end run.

5. **Case III-B explicit merge** — only `base_ts`, `*_history`, and grid columns are updated. Scalar columns are never overwritten by backfill data.

---

## Documentation

| Document | Contents |
|----------|----------|
| [DESIGN_DOCUMENT.md](production_docs/DESIGN_DOCUMENT.md) | Architecture, Mermaid diagrams, case logic, watermark lifecycle |
| [PERFORMANCE.md](production_docs/PERFORMANCE.md) | Benchmarks, bottleneck analysis, cluster recommendations, optimization plan |
| [TEST_CASES.md](production_docs/TEST_CASES.md) | Per-case test scenarios with examples, assertions, edge case coverage |
| [CHANGELOG.md](production_docs/CHANGELOG.md) | Version history from v9.1 through main, all patches |

---

## Version History

| Version | Key Addition |
|---------|-------------|
| v9.1 | Case III backfill row creation |
| v9.2 | Case IV detection + MAP_FROM_ENTRIES |
| v9.3 | Partition pruning, filter pushdown |
| v9.4 | Chained backfill (peer_map), multi-forward |
| v9.4.5 | NULL handling fixes |
| v9.4.8 | Stability patches |
| **main** | **Watermark tracker, per-case temp tables, chunked merge, source window filter** |

See [CHANGELOG.md](production_docs/CHANGELOG.md) for full details.
