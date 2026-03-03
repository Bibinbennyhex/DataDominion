"""
Run all tests and capture summary/latest_summary before/after snapshots
around each main_pipeline.run_pipeline invocation.
"""

import json
import os
import runpy
import traceback
from datetime import datetime

from pyspark.sql import functions as F

import test_utils


TEST_FILES = [
    "simple_test.py",
    "test_main_all_cases.py",
    "test_main_base_ts_propagation.py",
    "run_backfill_test.py",
    "test_all_scenarios.py",
    "test_all_scenarios_v942.py",
    "test_bulk_historical_load.py",
    "test_complex_scenarios.py",
    "test_case3_current_max_month.py",
    "test_soft_delete_case_iii.py",
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
    # Additional high-value test not in run_all_tests.py
    "test_aggressive_idempotency.py",
]


CURRENT_TEST = {"name": None}
PIPELINE_CALLS = []
TEST_RESULTS = []


def _snapshot_table(spark, table_name: str):
    if not spark.catalog.tableExists(table_name):
        return {
            "exists": False,
            "rows": 0,
            "accounts": 0,
            "max_month": None,
            "sample": None,
        }

    df = spark.table(table_name)
    rows = df.count()
    accounts = df.select("cons_acct_key").distinct().count() if "cons_acct_key" in df.columns else None
    max_month = (
        df.agg(F.max("rpt_as_of_mo").alias("max_month")).first()["max_month"]
        if "rpt_as_of_mo" in df.columns
        else None
    )

    sample = None
    if rows > 0 and "cons_acct_key" in df.columns:
        sample_cols = [c for c in [
            "cons_acct_key",
            "rpt_as_of_mo",
            "balance_am_history",
            "payment_rating_cd_history",
        ] if c in df.columns]
        row = df.select(*sample_cols).orderBy("cons_acct_key", "rpt_as_of_mo").limit(1).collect()[0]
        sample = row.asDict()
        for arr_col in ["balance_am_history", "payment_rating_cd_history"]:
            if arr_col in sample and sample[arr_col] is not None:
                sample[arr_col] = sample[arr_col][:6]

    return {
        "exists": True,
        "rows": rows,
        "accounts": accounts,
        "max_month": max_month,
        "sample": sample,
    }


def _snapshot_pair(spark, config):
    return {
        "summary": _snapshot_table(spark, config["destination_table"]),
        "latest_summary": _snapshot_table(spark, config["latest_history_table"]),
        "destination_table": config["destination_table"],
        "latest_table": config["latest_history_table"],
    }


ORIG_RUN_PIPELINE = test_utils.main_pipeline.run_pipeline


def _wrapped_run_pipeline(spark, config, *args, **kwargs):
    test_name = CURRENT_TEST["name"] or "UNKNOWN_TEST"
    before = _snapshot_pair(spark, config)
    err = None
    started = datetime.utcnow().isoformat()
    try:
        return ORIG_RUN_PIPELINE(spark, config, *args, **kwargs)
    except Exception as exc:
        err = str(exc).split("\n")[0]
        raise
    finally:
        after = _snapshot_pair(spark, config)
        PIPELINE_CALLS.append(
            {
                "test": test_name,
                "started_utc": started,
                "before": before,
                "after": after,
                "error": err,
            }
        )


def _aggregate_test_result(test_file: str):
    calls = [c for c in PIPELINE_CALLS if c["test"] == test_file]
    if not calls:
        return {
            "test": test_file,
            "pipeline_calls": 0,
            "summary_rows_before": None,
            "summary_rows_after": None,
            "latest_rows_before": None,
            "latest_rows_after": None,
            "summary_max_month_after": None,
            "latest_max_month_after": None,
            "call_errors": 0,
        }

    first = calls[0]
    last = calls[-1]
    return {
        "test": test_file,
        "pipeline_calls": len(calls),
        "summary_rows_before": first["before"]["summary"]["rows"],
        "summary_rows_after": last["after"]["summary"]["rows"],
        "latest_rows_before": first["before"]["latest_summary"]["rows"],
        "latest_rows_after": last["after"]["latest_summary"]["rows"],
        "summary_max_month_after": last["after"]["summary"]["max_month"],
        "latest_max_month_after": last["after"]["latest_summary"]["max_month"],
        "call_errors": sum(1 for c in calls if c["error"]),
    }


def main():
    tests_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(tests_dir)

    test_utils.main_pipeline.run_pipeline = _wrapped_run_pipeline
    try:
        for test_file in TEST_FILES:
            CURRENT_TEST["name"] = test_file
            path = os.path.join(tests_dir, test_file)
            status = "PASS"
            error = ""
            try:
                runpy.run_path(path, run_name="__main__")
            except Exception as exc:
                status = "FAIL"
                error = str(exc).split("\n")[0]
                traceback.print_exc()

            agg = _aggregate_test_result(test_file)
            agg["status"] = status
            agg["error"] = error
            TEST_RESULTS.append(agg)
            print(f"[AUDIT] {test_file} -> {status}")
    finally:
        test_utils.main_pipeline.run_pipeline = ORIG_RUN_PIPELINE

    payload = {
        "generated_utc": datetime.utcnow().isoformat(),
        "results": TEST_RESULTS,
        "calls": PIPELINE_CALLS,
    }
    out_json = os.path.join(tests_dir, "_audit_before_after_results.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    print("\n=== AUDIT SUMMARY ===")
    print("test | status | calls | summary_before->after | latest_before->after | call_errors")
    for r in TEST_RESULTS:
        print(
            f"{r['test']} | {r['status']} | {r['pipeline_calls']} | "
            f"{r['summary_rows_before']}->{r['summary_rows_after']} | "
            f"{r['latest_rows_before']}->{r['latest_rows_after']} | "
            f"{r['call_errors']}"
        )
    print(f"\nSaved: {out_json}")


if __name__ == "__main__":
    main()
