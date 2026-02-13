"""
Staged Airflow DAG for main/summary_inc.py.

This DAG runs the pipeline in explicit stages inside the Spark container:
1) precheck config/dependencies
2) cleanup checkpoint tables
3) run pipeline
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator


SPARK_CONTAINER_ENV = "SPARK_CONTAINER_NAME"
DEFAULT_SPARK_CONTAINER = "spark-iceberg-main"
SUMMARY_LOG_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} (INFO|WARNING|ERROR|DEBUG):")
SPARK_TASK_LOG_FILTER_ENV = "SPARK_TASK_LOG_FILTER"

CONFIG_BUCKET_ENV = "MAIN_SUMMARY_CONFIG_BUCKET"
CONFIG_KEY_ENV = "MAIN_SUMMARY_CONFIG_KEY"
MODE_ENV = "MAIN_SUMMARY_MODE"
DEFAULT_CONFIG_BUCKET = "warehouse"
DEFAULT_CONFIG_KEY = "main/docker_test/tests/test_config.json"
DEFAULT_MODE = "incremental"


def _should_emit_line(line: str) -> bool:
    mode = os.environ.get(SPARK_TASK_LOG_FILTER_ENV, "summary_only").strip().lower()
    if mode in {"off", "all", "none"}:
        return True
    if SUMMARY_LOG_RE.match(line):
        return True
    if line.startswith("Traceback") or "AssertionError" in line or "Exception" in line:
        return True
    if line.lstrip().startswith('File "'):
        return True
    if "Error:" in line or "Exception:" in line:
        return True
    return False


def _resolve_runtime_inputs() -> tuple[str, str, str]:
    config_bucket = os.environ.get(CONFIG_BUCKET_ENV, DEFAULT_CONFIG_BUCKET).strip()
    config_key = os.environ.get(CONFIG_KEY_ENV, DEFAULT_CONFIG_KEY).strip()
    run_mode = os.environ.get(MODE_ENV, DEFAULT_MODE).strip().lower()

    if run_mode not in {"incremental", "full"}:
        raise AirflowException(
            f"Invalid {MODE_ENV}='{run_mode}'. Expected 'incremental' or 'full'."
        )

    return config_bucket, config_key, run_mode


def _build_bootstrap_prefix() -> str:
    return (
        "python3 -m pip show boto3 >/dev/null 2>&1 || "
        "python3 -m pip install -q boto3 python-dateutil; "
        "AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL:-http://minio:9000} "
        "PYTHONPATH=/workspace/main:${PYTHONPATH} "
    )


def _build_precheck_command() -> str:
    config_bucket, config_key, run_mode = _resolve_runtime_inputs()
    config_payload = json.dumps(
        {"bucket_name": config_bucket, "key": config_key, "mode": run_mode},
        separators=(",", ":"),
    )
    return (
        _build_bootstrap_prefix()
        + "python3 - <<'PY'\n"
          "import json\n"
          "import boto3\n"
          f"payload = {config_payload!r}\n"
          "cfg_ref = json.loads(payload)\n"
          "s3 = boto3.client('s3')\n"
          "obj = s3.get_object(Bucket=cfg_ref['bucket_name'], Key=cfg_ref['key'])\n"
          "cfg = json.loads(obj['Body'].read().decode('utf-8'))\n"
          "required = ['source_table', 'destination_table', 'latest_history_table', 'spark']\n"
          "missing = [k for k in required if k not in cfg]\n"
          "if missing:\n"
          "    raise SystemExit(f'Missing required config keys: {missing}')\n"
          "print(f\"CONFIG_OK s3://{cfg_ref['bucket_name']}/{cfg_ref['key']}\")\n"
          "print(f\"MODE_OK {cfg_ref['mode']}\")\n"
          "PY"
    )


def _build_cleanup_command() -> str:
    config_bucket, config_key, _ = _resolve_runtime_inputs()
    config_payload = json.dumps(
        {
            "bucket_name": config_bucket,
            "key": config_key,
        },
        separators=(",", ":"),
    )
    return (
        _build_bootstrap_prefix()
        + "python3 - <<'PY'\n"
          "import json\n"
          "import boto3\n"
          "import summary_inc as s\n"
          f"payload = {config_payload!r}\n"
          "cfg_ref = json.loads(payload)\n"
          "cfg = json.loads(\n"
          "    boto3.client('s3').get_object(\n"
          "        Bucket=cfg_ref['bucket_name'],\n"
          "        Key=cfg_ref['key']\n"
          "    )['Body'].read().decode('utf-8')\n"
          ")\n"
          "spark = s.create_spark_session(cfg['spark']['app_name'], cfg['spark'])\n"
          "spark.sparkContext.setLogLevel('WARN')\n"
          "try:\n"
          "    s.cleanup(spark)\n"
          "finally:\n"
          "    spark.stop()\n"
          "print('CLEANUP_DONE')\n"
          "PY"
    )


def _build_pipeline_command() -> str:
    config_bucket, config_key, run_mode = _resolve_runtime_inputs()
    config_payload = json.dumps(
        {
            "bucket_name": config_bucket,
            "key": config_key,
            "mode": run_mode,
        },
        separators=(",", ":"),
    )
    return (
        _build_bootstrap_prefix()
        + "python3 - <<'PY'\n"
          "import json\n"
          "import boto3\n"
          "import summary_inc as s\n"
          f"payload = {config_payload!r}\n"
          "cfg_ref = json.loads(payload)\n"
          "cfg = json.loads(\n"
          "    boto3.client('s3').get_object(\n"
          "        Bucket=cfg_ref['bucket_name'],\n"
          "        Key=cfg_ref['key']\n"
          "    )['Body'].read().decode('utf-8')\n"
          ")\n"
          "if not s.validate_config(cfg):\n"
          "    raise SystemExit('Config validation failed')\n"
          "spark = s.create_spark_session(cfg['spark']['app_name'], cfg['spark'])\n"
          "spark.sparkContext.setLogLevel('WARN')\n"
          "try:\n"
          "    print(f\"RUN_MODE {cfg_ref['mode']}\")\n"
          "    s.run_pipeline(spark, cfg)\n"
          "finally:\n"
          "    spark.stop()\n"
          "print('PIPELINE_DONE')\n"
          "PY"
    )


def _run_in_spark(command: str) -> None:
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
                f"(exit_code={exit_code})"
            )
    finally:
        client.close()


def run_stage_precheck(**_: object) -> None:
    _run_in_spark(_build_precheck_command())


def run_stage_cleanup(**_: object) -> None:
    _run_in_spark(_build_cleanup_command())


def run_stage_pipeline(**_: object) -> None:
    _run_in_spark(_build_pipeline_command())


with DAG(
    dag_id="main_summary_run_dag",
    description="Run main/summary_inc.py with staged Airflow tasks",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["main", "summary", "docker", "pipeline", "staged"],
) as dag:
    precheck_config = PythonOperator(
        task_id="precheck_config",
        python_callable=run_stage_precheck,
    )

    cleanup_checkpoint_tables = PythonOperator(
        task_id="cleanup_checkpoint_tables",
        python_callable=run_stage_cleanup,
    )

    run_pipeline = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_stage_pipeline,
    )

    precheck_config >> cleanup_checkpoint_tables >> run_pipeline
