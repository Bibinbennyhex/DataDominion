"""
Stepwise Airflow DAG for main summary docker tests.

This DAG executes existing test scripts inside the spark container so we keep
the same runtime stack (Spark + Iceberg + MinIO) used by local docker runs.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator


SPARK_CONTAINER_ENV = "SPARK_CONTAINER_NAME"
DEFAULT_SPARK_CONTAINER = "spark-iceberg-main"


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

        for chunk in client.api.exec_start(exec_id, stream=True, demux=False):
            if not chunk:
                continue
            print(chunk.decode("utf-8", errors="replace"), end="")

        exec_result = client.api.exec_inspect(exec_id)
        exit_code = int(exec_result.get("ExitCode", 1))

        if exit_code != 0:
            raise AirflowException(
                f"Command failed in container '{container_name}' "
                f"(exit_code={exit_code}): {command}"
            )
    finally:
        client.close()


with DAG(
    dag_id="main_summary_stepwise_dag",
    description="Stepwise validation for main/summary_inc.py in docker stack",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["main", "summary", "docker", "tests"],
) as dag:
    simple_smoke = PythonOperator(
        task_id="simple_smoke",
        python_callable=run_in_spark,
        op_kwargs={"command": "python3 /workspace/main/docker_test/tests/simple_test.py"},
    )

    integration_all_cases = PythonOperator(
        task_id="integration_all_cases",
        python_callable=run_in_spark,
        op_kwargs={"command": "python3 /workspace/main/docker_test/tests/test_main_all_cases.py"},
    )

    regression_base_ts = PythonOperator(
        task_id="regression_base_ts",
        python_callable=run_in_spark,
        op_kwargs={"command": "python3 /workspace/main/docker_test/tests/test_main_base_ts_propagation.py"},
    )

    backfill_validation = PythonOperator(
        task_id="backfill_validation",
        python_callable=run_in_spark,
        op_kwargs={"command": "python3 /workspace/main/docker_test/tests/run_backfill_test.py"},
    )

    performance_tiny = PythonOperator(
        task_id="performance_tiny",
        python_callable=run_in_spark,
        op_kwargs={
            "command": "python3 /workspace/main/docker_test/tests/test_performance_benchmark.py --scale TINY"
        },
    )

    simple_smoke >> integration_all_cases >> regression_base_ts >> backfill_validation >> performance_tiny
