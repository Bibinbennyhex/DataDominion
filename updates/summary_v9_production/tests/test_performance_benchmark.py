"""
PERFORMANCE BENCHMARK TEST - Summary Pipeline v9.2.1
=====================================================

Generates synthetic data at various scales and measures:
1. Case classification time
2. Case I processing time
3. Case II processing time  
4. Case III processing time
5. Case IV processing time (with gap handling)
6. Total pipeline throughput (records/second)

Scales tested (configurable):
- TINY:   100 accounts, 1-12 months each
- SMALL:  1,000 accounts, 1-36 months each
- MEDIUM: 10,000 accounts, 1-36 months each
- LARGE:  100,000 accounts, 1-36 months each

Usage:
    docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_performance_benchmark.py [--scale TINY|SMALL|MEDIUM|LARGE]
"""

import sys
import os
import time
import random
import argparse
from datetime import datetime
from typing import Dict, List, Tuple

# Add the pipeline directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
pipeline_dir = os.path.join(os.path.dirname(script_dir), 'pipeline')
sys.path.insert(0, pipeline_dir)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================================
# CONFIGURATION
# ============================================================================

SCALES = {
    "TINY": {
        "new_accounts": 50,           # Case I
        "forward_accounts": 25,       # Case II
        "backfill_accounts": 15,      # Case III
        "bulk_accounts": 10,          # Case IV
        "bulk_months_range": (6, 12), # min/max months per bulk account
        "gap_probability": 0.2,       # 20% chance of gap in bulk
        "description": "TINY: ~200 total records"
    },
    "SMALL": {
        "new_accounts": 500,
        "forward_accounts": 250,
        "backfill_accounts": 150,
        "bulk_accounts": 100,
        "bulk_months_range": (12, 36),
        "gap_probability": 0.15,
        "description": "SMALL: ~5K total records"
    },
    "MEDIUM": {
        "new_accounts": 5000,
        "forward_accounts": 2500,
        "backfill_accounts": 1500,
        "bulk_accounts": 1000,
        "bulk_months_range": (24, 48),
        "gap_probability": 0.1,
        "description": "MEDIUM: ~100K total records"
    },
    "LARGE": {
        "new_accounts": 50000,
        "forward_accounts": 25000,
        "backfill_accounts": 15000,
        "bulk_accounts": 10000,
        "bulk_months_range": (36, 72),
        "gap_probability": 0.1,
        "description": "LARGE: ~1M total records"
    }
}


class Timer:
    """Simple timer context manager"""
    def __init__(self, name: str):
        self.name = name
        self.elapsed = 0.0
    
    def __enter__(self):
        self.start = time.time()
        return self
    
    def __exit__(self, *args):
        self.elapsed = time.time() - self.start


class BenchmarkResults:
    """Collects benchmark results"""
    def __init__(self):
        self.timings: Dict[str, float] = {}
        self.counts: Dict[str, int] = {}
        self.throughput: Dict[str, float] = {}
    
    def record(self, name: str, elapsed: float, count: int = 0):
        self.timings[name] = elapsed
        self.counts[name] = count
        if count > 0 and elapsed > 0:
            self.throughput[name] = count / elapsed
    
    def report(self):
        print("\n" + "=" * 80)
        print("PERFORMANCE BENCHMARK RESULTS")
        print("=" * 80)
        print(f"{'Phase':<30} {'Time (s)':<12} {'Records':<12} {'Throughput':<15}")
        print("-" * 80)
        
        total_time = 0
        total_records = 0
        
        for name in self.timings:
            t = self.timings[name]
            c = self.counts.get(name, 0)
            tp = self.throughput.get(name, 0)
            
            if c > 0:
                print(f"{name:<30} {t:>10.2f}s {c:>10,} {tp:>12,.1f}/s")
            else:
                print(f"{name:<30} {t:>10.2f}s {'N/A':<12} {'N/A':<15}")
            
            if name.startswith("Case") or name == "Write Results":
                total_time += t
                total_records += c
        
        print("-" * 80)
        if total_records > 0 and total_time > 0:
            overall_tp = total_records / total_time
            print(f"{'TOTAL (processing)':<30} {total_time:>10.2f}s {total_records:>10,} {overall_tp:>12,.1f}/s")
        
        return {
            "total_time": total_time,
            "total_records": total_records,
            "throughput": total_records / total_time if total_time > 0 else 0
        }


def create_spark_session():
    """Create Spark session configured for Iceberg"""
    spark = SparkSession.builder \
        .appName("performance_benchmark") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "rest") \
        .config("spark.sql.catalog.demo.uri", "http://rest:8181") \
        .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/") \
        .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.defaultCatalog", "demo") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def setup_tables(spark):
    """Create test tables"""
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.perf_test")
    
    # Drop existing tables
    spark.sql("DROP TABLE IF EXISTS demo.perf_test.accounts")
    spark.sql("DROP TABLE IF EXISTS demo.perf_test.summary")
    spark.sql("DROP TABLE IF EXISTS demo.perf_test.latest_summary")
    
    # Create accounts table
    spark.sql("""
        CREATE TABLE demo.perf_test.accounts (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_am INT,
            actual_payment_am INT,
            credit_limit_am INT,
            past_due_am INT,
            days_past_due INT,
            asset_class_cd STRING,
            payment_rating_cd STRING
        )
        USING iceberg
    """)
    
    # Create summary table
    spark.sql("""
        CREATE TABLE demo.perf_test.summary (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_am INT,
            actual_payment_am INT,
            credit_limit_am INT,
            past_due_am INT,
            days_past_due INT,
            asset_class_cd STRING,
            payment_rating_cd STRING,
            balance_am_history ARRAY<INT>,
            actual_payment_am_history ARRAY<INT>,
            credit_limit_am_history ARRAY<INT>,
            past_due_am_history ARRAY<INT>,
            days_past_due_history ARRAY<INT>,
            asset_class_cd_history ARRAY<STRING>,
            payment_rating_cd_history ARRAY<STRING>,
            payment_history_grid STRING
        )
        USING iceberg
    """)
    
    # Create latest_summary table
    spark.sql("""
        CREATE TABLE demo.perf_test.latest_summary (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_am INT,
            actual_payment_am INT,
            credit_limit_am INT,
            past_due_am INT,
            days_past_due INT,
            asset_class_cd STRING,
            payment_rating_cd STRING,
            balance_am_history ARRAY<INT>,
            actual_payment_am_history ARRAY<INT>,
            credit_limit_am_history ARRAY<INT>,
            past_due_am_history ARRAY<INT>,
            days_past_due_history ARRAY<INT>,
            asset_class_cd_history ARRAY<STRING>,
            payment_rating_cd_history ARRAY<STRING>,
            payment_history_grid STRING
        )
        USING iceberg
    """)


def build_int_array(positions_values: Dict[int, int], length: int = 36) -> str:
    """Build SQL array with specific integer values at positions"""
    arr = ["CAST(NULL AS INT)"] * length
    for pos, val in positions_values.items():
        arr[pos] = str(val)
    return "array(" + ", ".join(arr) + ")"


def build_str_array(positions_values: Dict[int, str], length: int = 36) -> str:
    """Build SQL array of strings with specific values at positions"""
    arr = ["CAST(NULL AS STRING)"] * length
    for pos, val in positions_values.items():
        arr[pos] = f"'{val}'"
    return "array(" + ", ".join(arr) + ")"


def generate_test_data(spark, scale_config: dict, results: BenchmarkResults):
    """Generate synthetic test data at the given scale"""
    print("\n" + "=" * 80)
    print(f"GENERATING TEST DATA: {scale_config['description']}")
    print("=" * 80)
    
    upload_ts = "2026-01-30 10:00:00"
    account_id = 1000
    
    total_records = 0
    
    with Timer("Data Generation") as t:
        # ====================
        # Case II: Forward accounts (need existing summary)
        # ====================
        print(f"\n1. Generating {scale_config['forward_accounts']} existing accounts for Case II (forward)...")
        for i in range(scale_config['forward_accounts']):
            acct = account_id + i
            # Create existing summary for Feb 2026
            spark.sql(f"""
                INSERT INTO demo.perf_test.summary VALUES
                ({acct}, '2026-02', TIMESTAMP '2026-02-15 10:00:00',
                 {5000 + i}, {500 + i}, {10000 + i}, 0, 0, 'A', '0',
                 {build_int_array({0: 5000 + i, 1: 4900 + i})},
                 {build_int_array({0: 500 + i, 1: 490 + i})},
                 {build_int_array({0: 10000 + i, 1: 10000 + i})},
                 {build_int_array({0: 0, 1: 0})},
                 {build_int_array({0: 0, 1: 0})},
                 {build_str_array({0: 'A', 1: 'A'})},
                 {build_str_array({0: '0', 1: '0'})},
                 '00??????????????????????????????????')
            """)
            spark.sql(f"""
                INSERT INTO demo.perf_test.latest_summary VALUES
                ({acct}, '2026-02', TIMESTAMP '2026-02-15 10:00:00',
                 {5000 + i}, {500 + i}, {10000 + i}, 0, 0, 'A', '0',
                 {build_int_array({0: 5000 + i, 1: 4900 + i})},
                 {build_int_array({0: 500 + i, 1: 490 + i})},
                 {build_int_array({0: 10000 + i, 1: 10000 + i})},
                 {build_int_array({0: 0, 1: 0})},
                 {build_int_array({0: 0, 1: 0})},
                 {build_str_array({0: 'A', 1: 'A'})},
                 {build_str_array({0: '0', 1: '0'})},
                 '00??????????????????????????????????')
            """)
            # Insert forward entry (Mar 2026)
            spark.sql(f"""
                INSERT INTO demo.perf_test.accounts VALUES
                ({acct}, '2026-03', TIMESTAMP '{upload_ts}',
                 {5100 + i}, {510 + i}, {10000 + i}, 0, 0, 'A', '0')
            """)
            total_records += 1
        
        account_id += scale_config['forward_accounts']
        
        # ====================
        # Case III: Backfill accounts (need existing summary)
        # ====================
        print(f"2. Generating {scale_config['backfill_accounts']} existing accounts for Case III (backfill)...")
        for i in range(scale_config['backfill_accounts']):
            acct = account_id + i
            # Create existing summary for Jan 2026
            spark.sql(f"""
                INSERT INTO demo.perf_test.summary VALUES
                ({acct}, '2026-01', TIMESTAMP '2026-01-15 10:00:00',
                 {8000 + i}, {800 + i}, {15000 + i}, 0, 0, 'A', '0',
                 {build_int_array({0: 8000 + i})},
                 {build_int_array({0: 800 + i})},
                 {build_int_array({0: 15000 + i})},
                 {build_int_array({0: 0})},
                 {build_int_array({0: 0})},
                 {build_str_array({0: 'A'})},
                 {build_str_array({0: '0'})},
                 '0???????????????????????????????????')
            """)
            spark.sql(f"""
                INSERT INTO demo.perf_test.latest_summary VALUES
                ({acct}, '2026-01', TIMESTAMP '2026-01-15 10:00:00',
                 {8000 + i}, {800 + i}, {15000 + i}, 0, 0, 'A', '0',
                 {build_int_array({0: 8000 + i})},
                 {build_int_array({0: 800 + i})},
                 {build_int_array({0: 15000 + i})},
                 {build_int_array({0: 0})},
                 {build_int_array({0: 0})},
                 {build_str_array({0: 'A'})},
                 {build_str_array({0: '0'})},
                 '0???????????????????????????????????')
            """)
            # Insert backfill (Dec 2025)
            spark.sql(f"""
                INSERT INTO demo.perf_test.accounts VALUES
                ({acct}, '2025-12', TIMESTAMP '{upload_ts}',
                 {8200 + i}, {820 + i}, {15000 + i}, 0, 0, 'A', '0')
            """)
            total_records += 1
        
        account_id += scale_config['backfill_accounts']
        
        # ====================
        # Case I: New single-month accounts
        # ====================
        print(f"3. Generating {scale_config['new_accounts']} new accounts for Case I (single month)...")
        for i in range(scale_config['new_accounts']):
            acct = account_id + i
            spark.sql(f"""
                INSERT INTO demo.perf_test.accounts VALUES
                ({acct}, '2026-01', TIMESTAMP '{upload_ts}',
                 {3000 + i}, {300 + i}, {8000 + i}, 0, 0, 'A', '0')
            """)
            total_records += 1
        
        account_id += scale_config['new_accounts']
        
        # ====================
        # Case IV: Bulk historical accounts
        # ====================
        print(f"4. Generating {scale_config['bulk_accounts']} bulk historical accounts for Case IV...")
        min_months, max_months = scale_config['bulk_months_range']
        gap_prob = scale_config['gap_probability']
        
        bulk_records = 0
        for i in range(scale_config['bulk_accounts']):
            acct = account_id + i
            num_months = random.randint(min_months, max_months)
            
            # Generate months with possible gaps
            months_to_insert = []
            for m in range(num_months):
                if random.random() > gap_prob:  # Include month (no gap)
                    months_to_insert.append(m)
            
            # Ensure at least 2 months (otherwise it's Case I)
            if len(months_to_insert) < 2:
                months_to_insert = [0, 1]
            
            for m in months_to_insert:
                year = 2025 - (m // 12)
                month = 12 - (m % 12)
                if month <= 0:
                    month += 12
                    year -= 1
                month_str = f"{year}-{month:02d}"
                balance = 10000 - (m * 50)
                
                spark.sql(f"""
                    INSERT INTO demo.perf_test.accounts VALUES
                    ({acct}, '{month_str}', TIMESTAMP '{upload_ts}',
                     {balance}, {100 + m * 5}, 12000, 0, 0, 'A', '0')
                """)
                bulk_records += 1
                total_records += 1
        
        print(f"   - Generated {bulk_records} bulk historical records")
    
    results.record("Data Generation", t.elapsed, total_records)
    
    print(f"\nTotal records in accounts table: {total_records}")
    print(f"Data generation time: {t.elapsed:.2f}s")
    
    return total_records


def run_pipeline(spark, results: BenchmarkResults):
    """Run the pipeline with timing"""
    print("\n" + "=" * 80)
    print("RUNNING PIPELINE v9.2.1")
    print("=" * 80)
    
    try:
        from summary_pipeline import run_pipeline as pipeline_run, PipelineConfig
    except ImportError as e:
        print(f"Could not import pipeline: {e}")
        return None
    
    config = PipelineConfig()
    config.accounts_table = "demo.perf_test.accounts"
    config.summary_table = "demo.perf_test.summary"
    config.latest_summary_table = "demo.perf_test.latest_summary"
    config.primary_key = "cons_acct_key"
    config.partition_key = "rpt_as_of_mo"
    config.timestamp_key = "base_ts"
    config.history_length = 36
    
    config.rolling_columns = [
        {"name": "balance_am", "source_column": "balance_am", "mapper_expr": "balance_am", "data_type": "INT"},
        {"name": "actual_payment_am", "source_column": "actual_payment_am", "mapper_expr": "actual_payment_am", "data_type": "INT"},
        {"name": "credit_limit_am", "source_column": "credit_limit_am", "mapper_expr": "credit_limit_am", "data_type": "INT"},
        {"name": "past_due_am", "source_column": "past_due_am", "mapper_expr": "past_due_am", "data_type": "INT"},
        {"name": "days_past_due", "source_column": "days_past_due", "mapper_expr": "days_past_due", "data_type": "INT"},
        {"name": "asset_class_cd", "source_column": "asset_class_cd", "mapper_expr": "asset_class_cd", "data_type": "STRING"},
        {"name": "payment_rating_cd", "source_column": "payment_rating_cd", "mapper_expr": "payment_rating_cd", "data_type": "STRING"}
    ]
    
    config.grid_columns = [
        {"name": "payment_history_grid", "source_history": "payment_rating_cd_history", "placeholder": "?", "separator": ""}
    ]
    
    config.column_mappings = {}
    config.column_transformations = []
    config.inferred_columns = []
    config.date_columns = []
    config.summary_columns = []
    config.latest_summary_columns = []
    
    with Timer("Total Pipeline") as total_timer:
        stats = pipeline_run(spark, config)
    
    if stats:
        # Record timing by case
        results.record("Case I (New)", stats.get('case_i_time', 0), stats.get('case_i_records', 0))
        results.record("Case II (Forward)", stats.get('case_ii_time', 0), stats.get('case_ii_records', 0))
        results.record("Case III (Backfill)", stats.get('case_iii_time', 0), stats.get('case_iii_records', 0))
        results.record("Case IV (Bulk)", stats.get('case_iv_time', 0), stats.get('case_iv_records', 0))
        results.record("Write Results", stats.get('write_time', 0), stats.get('records_written', 0))
        results.record("Classification", stats.get('classification_time', 0), stats.get('total_records', 0))
        results.record("Total Pipeline", total_timer.elapsed, stats.get('records_written', 0))
    
    return stats


def verify_results(spark, expected_records: int) -> bool:
    """Quick verification that results are correct"""
    print("\n" + "=" * 80)
    print("VERIFICATION")
    print("=" * 80)
    
    summary_count = spark.sql("SELECT COUNT(*) FROM demo.perf_test.summary").first()[0]
    latest_count = spark.sql("SELECT COUNT(*) FROM demo.perf_test.latest_summary").first()[0]
    
    print(f"Summary table rows: {summary_count:,}")
    print(f"Latest summary rows: {latest_count:,}")
    
    # Quick spot check - verify arrays are not all NULL
    sample = spark.sql("""
        SELECT cons_acct_key, rpt_as_of_mo,
               balance_am_history[0] as bal_0,
               size(filter(balance_am_history, x -> x IS NOT NULL)) as non_null_count
        FROM demo.perf_test.summary
        LIMIT 5
    """).collect()
    
    print("\nSample rows (verifying arrays populated):")
    for row in sample:
        print(f"  Account {row['cons_acct_key']} ({row['rpt_as_of_mo']}): bal[0]={row['bal_0']}, non_null={row['non_null_count']}")
    
    # Verify Case IV accounts have proper history
    bulk_check = spark.sql("""
        SELECT cons_acct_key, 
               COUNT(*) as rows,
               MAX(size(filter(balance_am_history, x -> x IS NOT NULL))) as max_history
        FROM demo.perf_test.summary
        GROUP BY cons_acct_key
        HAVING COUNT(*) > 2
        ORDER BY max_history DESC
        LIMIT 3
    """).collect()
    
    print("\nBulk historical accounts (Case IV) verification:")
    for row in bulk_check:
        print(f"  Account {row['cons_acct_key']}: {row['rows']} rows, max history depth: {row['max_history']}")
    
    return summary_count > 0


def main():
    parser = argparse.ArgumentParser(description='Performance Benchmark for Summary Pipeline')
    parser.add_argument('--scale', choices=['TINY', 'SMALL', 'MEDIUM', 'LARGE'], default='SMALL',
                       help='Scale of test data to generate')
    args = parser.parse_args()
    
    scale = args.scale
    scale_config = SCALES[scale]
    
    print("=" * 80)
    print(f"PERFORMANCE BENCHMARK - Summary Pipeline v9.2.1")
    print(f"Scale: {scale} - {scale_config['description']}")
    print("=" * 80)
    print(f"Start time: {datetime.now()}")
    
    spark = create_spark_session()
    print(f"Spark version: {spark.version}")
    
    results = BenchmarkResults()
    
    try:
        # Setup
        with Timer("Table Setup") as t:
            setup_tables(spark)
        results.record("Table Setup", t.elapsed)
        print(f"Table setup: {t.elapsed:.2f}s")
        
        # Generate data
        total_records = generate_test_data(spark, scale_config, results)
        
        # Run pipeline
        stats = run_pipeline(spark, results)
        
        if stats:
            # Verify
            verify_results(spark, total_records)
            
            # Report
            summary = results.report()
            
            print("\n" + "=" * 80)
            print("BENCHMARK SUMMARY")
            print("=" * 80)
            print(f"Scale:           {scale}")
            print(f"Total Records:   {summary['total_records']:,}")
            print(f"Total Time:      {summary['total_time']:.2f}s")
            print(f"Throughput:      {summary['throughput']:,.1f} records/second")
            
            # Extrapolation for production
            if summary['throughput'] > 0:
                print("\n" + "-" * 40)
                print("PRODUCTION EXTRAPOLATION (single node)")
                print("-" * 40)
                prod_scenarios = [
                    ("50M new accounts", 50_000_000),
                    ("200M backfill", 200_000_000),
                    ("750M forward", 750_000_000),
                    ("1B mixed", 1_000_000_000)
                ]
                for name, count in prod_scenarios:
                    est_time = count / summary['throughput']
                    print(f"  {name}: ~{est_time/60:.1f} minutes")
            
            return 0
        else:
            print("\n*** PIPELINE FAILED ***")
            return 1
    
    except Exception as e:
        print(f"\nBenchmark failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        print(f"\nEnd time: {datetime.now()}")
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
