# Agent Context (Saved)

Last updated: 2026-02-12 (UTC)

## 1) Project Understanding

- Current production logic is `main/summary_inc.py`.
- Historical/dev iterations are under `updates/summary_v*`.
- Latest historical reference mentioned by user: `updates/summary_v9_production/pipeline/summary_pipeline_v9_4_8.py`.
- Canonical config for main pipeline: `main/config.json`.

## 2) Business Logic Notes

- Classification covers Case I/II/III/IV.
- User confirmed invariant: Case IV cannot occur without Case I for the same batch (either Case I only, or Case I + Case IV).
- Core pipeline order in `run_pipeline()`:
  1. Case III (backfill)
  2. Case I (new)
  3. Case IV (bulk historical for new multi-month accounts)
  4. Backfill writes/merges
  5. Case II (forward)

## 3) Main Fixes Applied in `main/summary_inc.py`

### 3.1 Case III-B timestamp propagation
- Backfill-to-future update now carries timestamp as:
  - `GREATEST(existing_summary_ts, new_base_ts)` in Case III-B generation path.

### 3.2 Case III-B merge correctness fix
- Replaced wildcard merge update (`UPDATE SET *`) for `case_3b` with explicit updates.
- Update scope now targets:
  - `base_ts` (`GREATEST(s.base_ts, c.base_ts)`)
  - rolling `*_history` columns
  - configured grid columns
- Added safe behavior if `case_3a` temp does not exist (no forced dependency).

## 4) Docker Test Harness for Main

Folder: `main/docker_test`

Key files:
- `docker-compose.yml`
- `run_test.ps1`
- `tests/run_all_tests.py`
- `tests/scenario_suite.py`
- Legacy-equivalent test entry files aligned with `updates/summary_v9_production/tests` names.

Test runner supports:
- full run
- optional performance benchmark
- scale flag (`TINY|SMALL|MEDIUM|LARGE`)

## 5) Airflow Integration Added

### Files added/updated
- Added custom Airflow image:
  - `main/docker_test/airflow/Dockerfile`
- Added stepwise DAG:
  - `main/docker_test/airflow/dags/main_summary_stepwise_dag.py`
- Updated compose to include Airflow service:
  - `main/docker_test/docker-compose.yml`
- Updated usage docs:
  - `main/docker_test/README.md`

### DAG behavior
- Runs existing scripts in `spark-iceberg-main` container in sequence:
  1. `simple_test.py`
  2. `test_main_all_cases.py`
  3. `test_main_base_ts_propagation.py`
  4. `run_backfill_test.py`
  5. `test_performance_benchmark.py --scale TINY`

### Logging behavior
- DAG helper streams container stdout/stderr live into Airflow task logs.
- For clean UI logs, use `airflow dags trigger` (scheduler-managed run), not `airflow dags test` with future execution dates.
- Test Spark sessions now suppress stage progress bars:
  - `main/docker_test/tests/test_utils.py`
    - `spark.ui.showConsoleProgress=false`
    - `spark.sparkContext.setLogLevel("ERROR")`

## 6) Validated Commands

### Full main test run in spark container
```powershell
docker compose exec -T spark-iceberg-main python3 /workspace/main/docker_test/tests/run_all_tests.py --include-performance --performance-scale TINY
```

### Airflow DAG list
```powershell
docker compose exec -T airflow airflow dags list
```

### Trigger Airflow DAG for clean task UI logs
```powershell
docker compose exec -T airflow airflow dags trigger main_summary_stepwise_dag -e 2026-02-12T12:00:00+00:00 -r manual_trigger_20260212_120000
```

### Task states for a run
```powershell
docker compose exec -T airflow airflow tasks states-for-dag-run main_summary_stepwise_dag manual_trigger_20260212_120000
```

### Direct log file for one task attempt
```powershell
docker compose exec -T airflow bash -lc 'sed -n "1,200p" /opt/airflow/logs/dag_id=main_summary_stepwise_dag/run_id=manual_trigger_20260212_120000/task_id=integration_all_cases/attempt=1.log'
```

## 7) Observed Runtime Notes

- `dags test` can show `Task is not able to be run` when dependency checks fail (notably future execution date checks). This is expected in that mode.
- Scheduler logs may show queued/state mismatch noise in standalone mode; manual trigger runs still executed successfully.
- Occasional flaky data-state behavior observed in one earlier run; immediate reruns passed.

## 8) Suggested Improvements (Open)

P0/P1 suggestions discussed:
- make temp table writes more rerun-safe (`DROP IF EXISTS` before `.create()` or run-scoped temp names)
- harden `case_i_result` lifecycle handling in `run_pipeline()`
- avoid expensive `count()` checks for existence when not needed
- add explicit performance thresholds in benchmark tests

## 9) Security/Access Notes

- Airflow UI exposed at `http://localhost:8085`.
- Username typically `admin` for standalone.
- Password is generated/stored inside container at:
  - `/opt/airflow/standalone_admin_password.txt`
- Do not commit plaintext credentials to repo.

## 10) How to Keep This File Fresh

After major changes, update:
- what changed
- why it changed
- exact commands used to validate
- pass/fail outcomes and known caveats
