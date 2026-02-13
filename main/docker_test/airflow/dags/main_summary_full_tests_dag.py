"""
Full test DAG for main summary docker tests.

Each test script is executed as a separate Airflow task inside the Spark
container, giving task-level visibility and rerun control.
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import List

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator


SPARK_CONTAINER_ENV = "SPARK_CONTAINER_NAME"
DEFAULT_SPARK_CONTAINER = "spark-iceberg-main"
TEST_ROOT = "/workspace/main/docker_test/tests"
SUMMARY_LOG_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} (INFO|WARNING|ERROR|DEBUG):")
SPARK_TASK_LOG_FILTER_ENV = "SPARK_TASK_LOG_FILTER"

TEST_SCRIPTS: List[str] = [
    "simple_test.py",
    "test_main_all_cases.py",
    "test_main_base_ts_propagation.py",
    "run_backfill_test.py",
    "test_all_scenarios.py",
    "test_all_scenarios_v942.py",
    "test_bulk_historical_load.py",
    "test_complex_scenarios.py",
    "test_comprehensive_50_cases.py",
    "test_comprehensive_edge_cases.py",
    "test_consecutive_backfill.py",
    "test_duplicate_records.py",
    "test_full_46_columns.py",
    "test_long_backfill_gaps.py",
    "test_non_continuous_backfill.py",
    "test_null_update_case_iii.py",
    "test_null_update_other_cases.py",
    "test_null_update.py",
    "test_idempotency.py",
    "test_latest_summary_consistency.py",
    "test_recovery.py",
]


def run_in_spark(command: str, **_: object) -> None:
    import docker

    container_name = os.environ.get(SPARK_CONTAINER_ENV, DEFAULT_SPARK_CONTAINER)
    client = docker.from_env()
    try:
        container = client.containers.get(container_name)
        print(f"[RUN] {container_name}: {command}")

        exec_id = client.api.exec_create(
            container.id,
            ["bash", "-lc", command],
            stdout=True,
            stderr=True,
        )["Id"]

        pending = ""
        for chunk in client.api.exec_start(exec_id, stream=True, demux=False):
            if not chunk:
                continue
            pending += chunk.decode("utf-8", errors="replace")
            while "\n" in pending:
                line, pending = pending.split("\n", 1)
                if _should_emit_line(line):
                    print(line)
        if pending and _should_emit_line(pending):
            print(pending)

        exec_result = client.api.exec_inspect(exec_id)
        exit_code = int(exec_result.get("ExitCode", 1))
        if exit_code != 0:
            raise AirflowException(
                f"Command failed in container '{container_name}' "
                f"(exit_code={exit_code}): {command}"
            )
    finally:
        client.close()


def _should_emit_line(line: str) -> bool:
    mode = os.environ.get(SPARK_TASK_LOG_FILTER_ENV, "summary_only").strip().lower()
    if mode in {"off", "all", "none"}:
        return True
    if SUMMARY_LOG_RE.match(line):
        return True
    if line.startswith("Traceback") or "AssertionError" in line or "Exception" in line:
        return True
    return False


def _task_id_for(script_name: str) -> str:
    base = os.path.splitext(script_name)[0].lower()
    normalized = re.sub(r"[^a-z0-9_]+", "_", base).strip("_")
    if not normalized:
        normalized = "test_task"
    if normalized[0].isdigit():
        normalized = f"t_{normalized}"
    return normalized


with DAG(
    dag_id="main_summary_full_tests_dag",
    description="Full main/docker_test suite with one Airflow task per test script",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["main", "summary", "docker", "tests", "full"],
) as dag:
    previous_task = None

    for script in TEST_SCRIPTS:
        task = PythonOperator(
            task_id=_task_id_for(script),
            python_callable=run_in_spark,
            op_kwargs={"command": f"python3 {TEST_ROOT}/{script}"},
        )
        if previous_task is not None:
            previous_task >> task
        previous_task = task
