"""
Runs all main docker tests sequentially.
"""

import argparse
import os
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description="Run all main docker tests")
    parser.add_argument(
        "--include-performance",
        action="store_true",
        help="Include performance benchmark (defaults to off)",
    )
    parser.add_argument(
        "--performance-scale",
        default="TINY",
        choices=["TINY", "SMALL", "MEDIUM", "LARGE"],
        help="Scale for performance benchmark when included",
    )
    args = parser.parse_args()

    test_dir = os.path.dirname(os.path.abspath(__file__))
    tests = [
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
        "test_recovery.py",
    ]

    failures = []

    for test_file in tests:
        test_path = os.path.join(test_dir, test_file)
        print("=" * 80)
        print(f"RUNNING {test_file}")
        print("=" * 80)

        result = subprocess.run([sys.executable, test_path], check=False)
        if result.returncode != 0:
            failures.append(test_file)

    if args.include_performance:
        perf_script = os.path.join(test_dir, "test_performance_benchmark.py")
        print("=" * 80)
        print(f"RUNNING test_performance_benchmark.py --scale {args.performance_scale}")
        print("=" * 80)
        result = subprocess.run(
            [sys.executable, perf_script, "--scale", args.performance_scale],
            check=False,
        )
        if result.returncode != 0:
            failures.append(f"test_performance_benchmark.py --scale {args.performance_scale}")

    if failures:
        print("=" * 80)
        print("FAILED TESTS:")
        for failed in failures:
            print(f"- {failed}")
        print("=" * 80)
        sys.exit(1)

    print("=" * 80)
    print("ALL TESTS PASSED")
    print("=" * 80)


if __name__ == "__main__":
    main()
