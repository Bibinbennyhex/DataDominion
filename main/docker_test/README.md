# Main Pipeline Docker Tests

This folder provides a docker-executable test harness for `main/summary_inc.py`, aligned with the `summary_v` local testing style.

## Included

- `docker-compose.yml`: Local Spark + Iceberg REST + MinIO stack
- `airflow/dags/main_summary_stepwise_dag.py`: Stepwise Airflow DAG for orchestrated test execution
- `run_test.ps1`: One-command runner for all tests
- `tests/test_main_all_cases.py`: End-to-end Case I/II/III/IV coverage
- `tests/test_main_base_ts_propagation.py`: Focused Case III `base_ts` propagation regression
- `tests/scenario_suite.py`: Shared scenario library used by all legacy-equivalent test entries
- `tests/run_all_tests.py`: Sequential test runner (includes all legacy-equivalent entries)
- Legacy-equivalent entries matching `updates/summary_v9_production/tests` filenames:
  - `simple_test.py`
  - `run_backfill_test.py`
  - `test_all_scenarios.py`
  - `test_all_scenarios_v942.py`
  - `test_bulk_historical_load.py`
  - `test_complex_scenarios.py`
  - `test_comprehensive_50_cases.py`
  - `test_comprehensive_edge_cases.py`
  - `test_consecutive_backfill.py`
  - `test_duplicate_records.py`
  - `test_full_46_columns.py`
  - `test_long_backfill_gaps.py`
  - `test_non_continuous_backfill.py`
  - `test_null_update_case_iii.py`
  - `test_null_update_other_cases.py`
  - `test_null_update.py`
  - `test_performance_benchmark.py`
  - `test_recovery.py`
  - `test_config.json`

## Prerequisites

- Docker Desktop running
- `docker compose` available

## Quick Start (PowerShell)

```powershell
cd main/docker_test
./run_test.ps1
```

Keep the environment running after tests:

```powershell
./run_test.ps1 -KeepRunning
```

Include performance benchmark:

```powershell
./run_test.ps1 -IncludePerformance -PerformanceScale TINY
```

## Manual Run

```powershell
cd main/docker_test
docker compose up -d
docker compose exec -T spark-iceberg-main python3 /workspace/main/docker_test/tests/run_all_tests.py
docker compose down
```

## Airflow DAG Run

Airflow is included in `docker-compose.yml` as `airflow-main` and exposed at `http://localhost:8085`.

For proper per-task logs in Airflow UI, trigger a normal DAG run (scheduler-managed):

```powershell
cd main/docker_test
docker compose up -d
docker compose exec -T airflow airflow dags list
docker compose exec -T airflow airflow dags trigger main_summary_stepwise_dag -e 2026-02-12T12:00:00+00:00 -r manual_trigger_20260212_120000
```

This DAG runs these scripts step-by-step inside `spark-iceberg-main`:
- `simple_test.py`
- `test_main_all_cases.py`
- `test_main_base_ts_propagation.py`
- `run_backfill_test.py`
- `test_performance_benchmark.py --scale TINY`

Optional (CLI-only smoke): you can still use `airflow dags test ...`, but those task logs can include dependency-check messages like `Task is not able to be run` and are less clean for UI review.

## Notes

- Tests use `main/config.json` as baseline and override only table names/catalog settings for local execution.
- Pipeline execution path is `run_pipeline()` from `main/summary_inc.py` (not the `updates/summary_v*` code).
- Temporary checkpoint tables are created under `temp_catalog.checkpointdb`.
