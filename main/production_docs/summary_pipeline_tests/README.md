# Summary Incremental Pipeline - Production Documentation

This folder contains production-facing documentation for the current summary incremental pipeline implemented in `main/summary_inc.py`.

## Current Baseline

- Pipeline code: `main/summary_inc.py`
- Runtime harness: `main/docker_test`
- Test suites: `main/docker_test/tests`
- Last consolidated retest: February 13, 2026 (22/22 pass)

## Document Set

- `DESIGN_DOCUMENT.md`
  Technical design, case processing logic, watermark tracker model, and operational assumptions.

- `PERFORMANCE.md`
  Performance profile, scaling behavior, benchmark approach, and tuning guidance.

- `TEST_CASES_COMBINED.md`
  Combined test-case catalog with representative input/output examples.

- `EXECUTION_RUNBOOK.md`
  Exact commands to run full tests, stress idempotency, and optional performance benchmark.

- `RETEST_RESULTS_2026-02-13.md`
  Latest executed results with per-test duration and status table.

## Quick Start

```bash
docker compose -f main/docker_test/docker-compose.yml up -d

docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/run_all_tests.py

# Optional benchmark
docker compose -f main/docker_test/docker-compose.yml exec -T spark-iceberg-main \
  python3 /workspace/main/docker_test/tests/test_performance_benchmark.py --scale SMALL

docker compose -f main/docker_test/docker-compose.yml down
```

## Scope Notes

- These docs describe the current production logic in `main/summary_inc.py`, not historical v9 implementation files under `updates/`.
- Case naming remains consistent: Case I (new), Case II (forward), Case III (backfill), Case IV (new account with multi-month batch).
- Idempotency behavior is documented with current merge-based writes and committed ingestion watermark tracking.
