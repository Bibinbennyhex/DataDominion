# Summary Incremental Pipeline - Performance

**Document Version**: 1.0  
**Baseline**: `main/summary_inc.py`  
**Last Updated**: February 13, 2026

---

## 1. Performance Drivers

Pipeline performance is dominated by:

- Case mix in the incoming batch (Case III is most expensive).
- Backfill month spread and affected account count.
- Size of `summary` partitions touched for Case III.
- Shuffle and merge behavior in Iceberg write paths.
- Cluster sizing and Spark partition configuration.

---

## 2. Case-wise Cost Profile

| Case | Relative Cost | Dominant Operations |
|---|---|---|
| Case I | Low | transform + merge insert/update |
| Case II | Medium | latest join + history shift + merge |
| Case III | High | range reads + future row patch + multiple merges |
| Case IV | Medium-High | ordered window build + merge |

Case III remains the primary hotspot because it modifies historical and future rows for affected accounts.

---

## 3. Partition and Shuffle Strategy

The pipeline uses dynamic partition sizing through `get_write_partitions()` with bounded controls:

- `MIN_PARTITIONS` = 16
- `MAX_PARTITIONS` = 8192
- optional config overrides:
  - `write_partitions`
  - `min_write_partitions`
  - `max_write_partitions`
  - `target_rows_per_partition`
  - `min_parallelism_factor`

When expected row counts are known, partition count blends row-based sizing with runtime baseline parallelism.

---

## 4. Watermark Effect on Runtime

Using committed ingestion watermark reduces unnecessary source scanning on reruns and supports stable incremental behavior:

- Source filter uses `base_ts > committed_watermark`.
- No new data reruns become near no-op at classification stage.
- Tracker consistency checks in test harness guard against stale watermark state during repeated tests.

---

## 5. Test Runtime Observations (Retest 2026-02-13)

From `RETEST_RESULTS_2026-02-13.md`:

- Total tests executed: 22
- Passed: 22
- Total wall time: 1940.92 sec
- Average per test: 88.22 sec
- P95: 120.39 sec

Longest runs observed:

| Test | Duration (sec) |
|---|---:|
| `test_aggressive_idempotency.py --scale TINY --cycles 2 --reruns 2` | 238.60 |
| `test_comprehensive_edge_cases.py` | 120.39 |
| `test_all_scenarios_v942.py` | 113.44 |
| `test_idempotency.py` | 110.82 |
| `test_complex_scenarios.py` | 110.92 |

These are test-suite timings in local docker context and include Spark startup overhead.

---

## 6. Benchmark Harness

Performance benchmark entrypoint:

- `main/docker_test/tests/test_performance_benchmark.py`
- Delegates to `scenario_suite.run_performance_benchmark`

Available scales:

| Scale | New | Forward | Backfill | Bulk Accounts | Bulk Months |
|---|---:|---:|---:|---:|---:|
| TINY | 20 | 10 | 8 | 5 | 8 |
| SMALL | 200 | 100 | 80 | 40 | 12 |
| MEDIUM | 1000 | 500 | 350 | 150 | 18 |
| LARGE | 4000 | 2000 | 1200 | 600 | 24 |

Run command:

```bash
docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/test_performance_benchmark.py --scale SMALL
```

Output includes rows processed, elapsed seconds, and throughput (rows/sec).

---

## 7. Tuning Guidance

- If Case III dominates:
  - increase executor memory and shuffle partitions,
  - validate partition pruning by month range,
  - reduce skew in affected-account distribution.

- If merge stage dominates:
  - review write partition counts for case outputs,
  - verify Iceberg table maintenance (compaction, snapshot retention) externally.

- If reruns are unexpectedly expensive:
  - inspect committed watermark movement,
  - verify source ingestion timestamps are monotonic and trustworthy.

- If throughput is unstable:
  - pin cluster sizing for benchmark comparisons,
  - benchmark each scale at least 3 times and compare median.

---

## 8. Operational Performance Checklist

Before production release:

1. Run `run_all_tests.py` without performance and ensure all pass.
2. Run benchmark at agreed scale (`SMALL` or higher) and record throughput baseline.
3. Compare run-time and throughput against previous baseline release.
4. Validate no regression in idempotency and latest-summary consistency suites.
