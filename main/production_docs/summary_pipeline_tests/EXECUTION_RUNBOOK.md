# Execution Runbook

## Prerequisites
- Docker Desktop running
- Services defined in main/docker_test/docker-compose.yml

## Start Services
~~~bash
docker compose -f main/docker_test/docker-compose.yml up -d
~~~

## Run Standard Suite
~~~bash
docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/run_all_tests.py
~~~

## Run Stress Idempotency
~~~bash
docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/test_aggressive_idempotency.py --scale TINY --cycles 2 --reruns 2
~~~

## Run Performance Benchmark (Optional)
~~~bash
docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/test_performance_benchmark.py --scale SMALL
~~~

## Stop Services
~~~bash
docker compose -f main/docker_test/docker-compose.yml down
~~~

## Artifacts
- Detailed log: main/docker_test/tests/_retest_run.log
- Structured result JSON: main/docker_test/tests/_retest_results.json
