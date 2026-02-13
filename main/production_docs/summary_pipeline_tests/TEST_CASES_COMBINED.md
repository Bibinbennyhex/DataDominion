# Test Cases Documentation - Combined

**Scope**: production pipeline in `main/summary_inc.py`  
**Execution harness**: `main/docker_test/tests`

---

## 1. Test Inventory

### 1.1 Standard Suite (`run_all_tests.py`)

| Order | Test File | Entrypoint | Primary Focus |
|---:|---|---|---|
| 1 | `simple_test.py` | `run_simple_test()` | Spark smoke sanity |
| 2 | `test_main_all_cases.py` | standalone `run_test()` | Case I/II/III/IV integrated |
| 3 | `test_main_base_ts_propagation.py` | standalone `run_test()` | Case III `base_ts` propagation |
| 4 | `run_backfill_test.py` | `run_backfill_test()` | Backfill cascade |
| 5 | `test_all_scenarios.py` | `run_all_scenarios_test()` | Mixed scenarios |
| 6 | `test_all_scenarios_v942.py` | `run_all_scenarios_test()` | Legacy wrapper parity |
| 7 | `test_bulk_historical_load.py` | `run_bulk_historical_load_test()` | Long history window behavior |
| 8 | `test_complex_scenarios.py` | `run_all_scenarios_test()` | Mixed scenario alias |
| 9 | `test_comprehensive_50_cases.py` | `run_comprehensive_50_cases_test()` | Higher-volume mixed batch |
| 10 | `test_comprehensive_edge_cases.py` | `run_comprehensive_edge_cases_test()` | Null + duplicate + backfill bundle |
| 11 | `test_consecutive_backfill.py` | `run_backfill_test()` | Backfill alias |
| 12 | `test_duplicate_records.py` | `run_duplicate_records_test()` | Duplicate dedupe by latest `base_ts` |
| 13 | `test_full_46_columns.py` | `run_full_46_columns_test()` | Output shape + inferred fields |
| 14 | `test_long_backfill_gaps.py` | `run_backfill_test()` | Backfill alias |
| 15 | `test_non_continuous_backfill.py` | `run_backfill_test()` | Backfill alias |
| 16 | `test_null_update_case_iii.py` | `run_null_update_test()` | Null sentinel alias |
| 17 | `test_null_update_other_cases.py` | `run_null_update_test()` | Null sentinel alias |
| 18 | `test_null_update.py` | `run_null_update_test()` | Sentinel-to-NULL canonical checks |
| 19 | `test_idempotency.py` | standalone `run_test()` | Rerun no-drift validation |
| 20 | `test_latest_summary_consistency.py` | standalone `run_test()` | latest-summary consistency |
| 21 | `test_recovery.py` | `run_recovery_test()` | Recovery + rerun path |

### 1.2 Optional Stress/Performance

| File | Purpose | Typical Command |
|---|---|---|
| `test_aggressive_idempotency.py` | Multi-cycle idempotency stress | `... test_aggressive_idempotency.py --scale TINY --cycles 2 --reruns 2` |
| `test_performance_benchmark.py` | Throughput benchmark (TINY/SMALL/MEDIUM/LARGE) | `... test_performance_benchmark.py --scale SMALL` |

---

## 2. Representative Data Examples

### 2.1 Case I input (new account)
```json
{
  "cons_acct_key": 1001,
  "rpt_as_of_mo": "2026-01",
  "base_ts": "2026-02-01T12:00:00",
  "acct_bal_am": 3000,
  "actual_pymt_am": 300
}
```

### 2.2 Case II input (forward)
```json
{
  "cons_acct_key": 2001,
  "rpt_as_of_mo": "2026-01",
  "base_ts": "2026-02-01T12:00:00",
  "acct_bal_am": 5200,
  "actual_pymt_am": 520
}
```

### 2.3 Case III input (backfill)
```json
{
  "cons_acct_key": 3001,
  "rpt_as_of_mo": "2025-12",
  "base_ts": "2026-02-01T12:00:00",
  "acct_bal_am": 4800,
  "actual_pymt_am": 480
}
```

### 2.4 Case IV input (new account, multi-month same batch)
```json
[
  {
    "cons_acct_key": 4001,
    "rpt_as_of_mo": "2025-12",
    "base_ts": "2026-02-01T12:00:00",
    "acct_bal_am": 7000,
    "actual_pymt_am": 700
  },
  {
    "cons_acct_key": 4001,
    "rpt_as_of_mo": "2026-01",
    "base_ts": "2026-02-01T12:00:00",
    "acct_bal_am": 6800,
    "actual_pymt_am": 680
  }
]
```

### 2.5 Duplicate input sample
```json
[
  {"cons_acct_key": 12001, "rpt_as_of_mo": "2026-01", "base_ts": "2026-02-01T10:00:00", "acct_bal_am": 5000},
  {"cons_acct_key": 12001, "rpt_as_of_mo": "2026-01", "base_ts": "2026-02-01T11:00:00", "acct_bal_am": 5500},
  {"cons_acct_key": 12001, "rpt_as_of_mo": "2026-01", "base_ts": "2026-02-01T12:00:00", "acct_bal_am": 5600}
]
```

### 2.6 Null sentinel sample
```json
{
  "cons_acct_key": 14001,
  "rpt_as_of_mo": "2026-01",
  "base_ts": "2026-02-01T00:00:00",
  "acct_bal_am": -1,
  "actual_pymt_am": -1
}
```

### 2.7 Expected output shape sample
```json
{
  "cons_acct_key": 2001,
  "rpt_as_of_mo": "2026-01",
  "base_ts": "2026-02-01T12:00:00",
  "balance_am_history": [5200, 5000, null, null],
  "actual_payment_am_history": [520, 500, null, null]
}
```

---

## 3. Assertion Families

- Functional correctness:
  - case classification and history shifting/chaining.
- Backfill correctness:
  - inserted historical month and propagation to future rows.
- Dedupe correctness:
  - latest `base_ts` wins for same account-month duplicates.
- Null transform correctness:
  - sentinel numeric values transformed to `NULL`.
- Consistency correctness:
  - `latest_summary` matches latest row derivable from `summary`.
- Idempotency correctness:
  - reruns do not change counts or values.
- Tracker correctness:
  - watermark tracker is consistent after each run in shared harness.

---

## 4. Execution Commands

```bash
# Standard suite
docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/run_all_tests.py

# Include performance benchmark
docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/run_all_tests.py --include-performance --performance-scale SMALL

# Aggressive idempotency stress
docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/test_aggressive_idempotency.py --scale TINY --cycles 2 --reruns 2
```

---

## 5. Latest Retest Reference

See `RETEST_RESULTS_2026-02-13.md` for the latest complete rerun table (22/22 pass).
