"""
Quick Test Runner for Summary Pipeline v9
==========================================

Runs pipeline with test data in the Docker environment.
"""

import sys
sys.path.insert(0, '/home/iceberg/pipeline')
sys.path.insert(0, '/home/iceberg/test')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime


def create_spark_session():
    """Create Spark session configured for Iceberg"""
    spark = SparkSession.builder \
        .appName("v9_test_runner") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "rest") \
        .config("spark.sql.catalog.demo.uri", "http://rest:8181") \
        .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/") \
        .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.defaultCatalog", "demo") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def setup_tables(spark):
    """Create test tables"""
    print("Creating test database and tables...")
    
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.test_db")
    
    # Drop existing tables
    spark.sql("DROP TABLE IF EXISTS demo.test_db.accounts")
    spark.sql("DROP TABLE IF EXISTS demo.test_db.summary")
    spark.sql("DROP TABLE IF EXISTS demo.test_db.latest_summary")
    
    # Create accounts table
    spark.sql("""
        CREATE TABLE demo.test_db.accounts (
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
        CREATE TABLE demo.test_db.summary (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_history ARRAY<INT>,
            payment_history ARRAY<INT>,
            credit_limit_history ARRAY<INT>,
            past_due_history ARRAY<INT>,
            days_past_due_history ARRAY<INT>,
            payment_rating_history ARRAY<STRING>,
            asset_class_history ARRAY<STRING>,
            payment_history_grid STRING
        )
        USING iceberg
    """)
    
    # Create latest_summary table
    spark.sql("""
        CREATE TABLE demo.test_db.latest_summary (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_history ARRAY<INT>,
            payment_history ARRAY<INT>,
            credit_limit_history ARRAY<INT>,
            past_due_history ARRAY<INT>,
            days_past_due_history ARRAY<INT>,
            payment_rating_history ARRAY<STRING>,
            asset_class_history ARRAY<STRING>,
            payment_history_grid STRING
        )
        USING iceberg
    """)
    
    print("Tables created successfully!")


def insert_test_data(spark):
    """Insert test data for all 3 cases"""
    print("\nInserting test data...")
    
    # Case I: New accounts (IDs 1001-1003)
    spark.sql("""
        INSERT INTO demo.test_db.accounts VALUES
        (1001, '2025-12', TIMESTAMP '2025-12-15 10:00:00', 5000, 500, 10000, 0, 0, 'A', '0'),
        (1002, '2025-12', TIMESTAMP '2025-12-15 10:00:00', 7000, 700, 15000, 0, 0, 'A', '0'),
        (1003, '2025-12', TIMESTAMP '2025-12-15 10:00:00', 3000, 300, 8000, 100, 30, 'B', '1')
    """)
    
    # Case II: Forward accounts (IDs 2001-2002) - need existing summary first
    # Insert existing summary for Nov 2025
    null_array_int = ", ".join(["NULL"] * 35)
    null_array_str = ", ".join(["NULL"] * 35)
    
    spark.sql(f"""
        INSERT INTO demo.test_db.summary VALUES
        (2001, '2025-11', TIMESTAMP '2025-11-15 10:00:00', 
         array(4000, {null_array_int}), array(400, {null_array_int}),
         array(10000, {null_array_int}), array(0, {null_array_int}),
         array(0, {null_array_int}), array('0', {null_array_str}),
         array('A', {null_array_str}), '0???????????????????????????????????'),
        (2002, '2025-11', TIMESTAMP '2025-11-15 10:00:00',
         array(6000, {null_array_int}), array(600, {null_array_int}),
         array(15000, {null_array_int}), array(0, {null_array_int}),
         array(0, {null_array_int}), array('0', {null_array_str}),
         array('A', {null_array_str}), '0???????????????????????????????????')
    """)
    
    spark.sql(f"""
        INSERT INTO demo.test_db.latest_summary VALUES
        (2001, '2025-11', TIMESTAMP '2025-11-15 10:00:00', 
         array(4000, {null_array_int}), array(400, {null_array_int}),
         array(10000, {null_array_int}), array(0, {null_array_int}),
         array(0, {null_array_int}), array('0', {null_array_str}),
         array('A', {null_array_str}), '0???????????????????????????????????'),
        (2002, '2025-11', TIMESTAMP '2025-11-15 10:00:00',
         array(6000, {null_array_int}), array(600, {null_array_int}),
         array(15000, {null_array_int}), array(0, {null_array_int}),
         array(0, {null_array_int}), array('0', {null_array_str}),
         array('A', {null_array_str}), '0???????????????????????????????????')
    """)
    
    # Now add Dec 2025 accounts for forward processing
    spark.sql("""
        INSERT INTO demo.test_db.accounts VALUES
        (2001, '2025-12', TIMESTAMP '2025-12-15 10:00:00', 4500, 450, 10000, 0, 0, 'A', '0'),
        (2002, '2025-12', TIMESTAMP '2025-12-15 10:00:00', 6500, 650, 15000, 0, 0, 'A', '0')
    """)
    
    # Case III: Backfill accounts (IDs 3001-3002) - need existing summary for Dec first
    spark.sql(f"""
        INSERT INTO demo.test_db.summary VALUES
        (3001, '2025-12', TIMESTAMP '2025-12-15 10:00:00', 
         array(8000, NULL, 7000, {", ".join(["NULL"] * 33)}), 
         array(800, NULL, 700, {", ".join(["NULL"] * 33)}),
         array(20000, {null_array_int}), array(0, {null_array_int}),
         array(0, {null_array_int}), array('0', {null_array_str}),
         array('A', {null_array_str}), '0?0?????????????????????????????????')
    """)
    
    spark.sql(f"""
        INSERT INTO demo.test_db.latest_summary VALUES
        (3001, '2025-12', TIMESTAMP '2025-12-15 10:00:00', 
         array(8000, NULL, 7000, {", ".join(["NULL"] * 33)}), 
         array(800, NULL, 700, {", ".join(["NULL"] * 33)}),
         array(20000, {null_array_int}), array(0, {null_array_int}),
         array(0, {null_array_int}), array('0', {null_array_str}),
         array('A', {null_array_str}), '0?0?????????????????????????????????')
    """)
    
    # Add backfill data for Nov 2025 (with newer timestamp)
    spark.sql("""
        INSERT INTO demo.test_db.accounts VALUES
        (3001, '2025-11', TIMESTAMP '2025-12-20 10:00:00', 7500, 750, 20000, 0, 0, 'A', '0')
    """)
    
    print("Test data inserted!")
    
    # Show counts
    print("\nData counts:")
    print(f"  Accounts: {spark.sql('SELECT COUNT(*) FROM demo.test_db.accounts').first()[0]}")
    print(f"  Summary: {spark.sql('SELECT COUNT(*) FROM demo.test_db.summary').first()[0]}")
    print(f"  Latest Summary: {spark.sql('SELECT COUNT(*) FROM demo.test_db.latest_summary').first()[0]}")


def run_pipeline_test(spark):
    """Run the pipeline and verify results"""
    print("\n" + "=" * 80)
    print("RUNNING PIPELINE")
    print("=" * 80)
    
    # Import pipeline
    from summary_pipeline import run_pipeline, PipelineConfig
    
    # Configure for test tables
    config = PipelineConfig()
    config.accounts_table = "demo.test_db.accounts"
    config.summary_table = "demo.test_db.summary"
    config.latest_summary_table = "demo.test_db.latest_summary"
    config.primary_key = "cons_acct_key"
    config.partition_key = "rpt_as_of_mo"
    config.timestamp_key = "base_ts"
    
    # Set history arrays
    config.history_arrays = [
        ("balance_history", "balance_am"),
        ("payment_history", "actual_payment_am"),
        ("credit_limit_history", "credit_limit_am"),
        ("past_due_history", "past_due_am"),
        ("days_past_due_history", "days_past_due"),
        ("payment_rating_history", "payment_rating_cd"),
        ("asset_class_history", "asset_class_cd")
    ]
    
    config.grid_columns = [
        {
            "name": "payment_history_grid",
            "source_history": "payment_rating_history",
            "placeholder": "?",
            "separator": ""
        }
    ]
    
    # Run pipeline
    try:
        stats = run_pipeline(spark, config)
        print("\n" + "=" * 80)
        print("PIPELINE RESULTS")
        print("=" * 80)
        print(f"Total records:     {stats.get('total_records', 0)}")
        print(f"Case I (new):      {stats.get('case_i_records', 0)}")
        print(f"Case II (forward): {stats.get('case_ii_records', 0)}")
        print(f"Case III (backfill): {stats.get('case_iii_records', 0)}")
        print(f"Records written:   {stats.get('records_written', 0)}")
        return stats
    except Exception as e:
        print(f"Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return None


def verify_results(spark):
    """Verify pipeline results"""
    print("\n" + "=" * 80)
    print("VERIFYING RESULTS")
    print("=" * 80)
    
    # Check summary table
    print("\nSummary table contents:")
    spark.sql("""
        SELECT cons_acct_key, rpt_as_of_mo, 
               balance_history[0] as bal_0,
               balance_history[1] as bal_1,
               balance_history[2] as bal_2
        FROM demo.test_db.summary
        ORDER BY cons_acct_key, rpt_as_of_mo
    """).show(20, False)
    
    # Check latest_summary
    print("\nLatest summary contents:")
    spark.sql("""
        SELECT cons_acct_key, rpt_as_of_mo,
               balance_history[0] as bal_0,
               balance_history[1] as bal_1
        FROM demo.test_db.latest_summary
        ORDER BY cons_acct_key
    """).show(20, False)
    
    # Verify specific cases
    passed = 0
    failed = 0
    
    # Test Case I: New account 1001 should have balance 5000 at position 0
    result = spark.sql("""
        SELECT balance_history[0] as bal 
        FROM demo.test_db.latest_summary 
        WHERE cons_acct_key = 1001
    """).first()
    if result and result['bal'] == 5000:
        print("[PASS] Case I: New account 1001 has correct balance")
        passed += 1
    else:
        print(f"[FAIL] Case I: New account 1001 expected 5000, got {result}")
        failed += 1
    
    # Test Case II: Forward account 2001 should have 4500 at pos 0, 4000 at pos 1
    result = spark.sql("""
        SELECT balance_history[0] as bal_0, balance_history[1] as bal_1
        FROM demo.test_db.latest_summary 
        WHERE cons_acct_key = 2001
    """).first()
    if result and result['bal_0'] == 4500 and result['bal_1'] == 4000:
        print("[PASS] Case II: Forward account 2001 has shifted array")
        passed += 1
    else:
        print(f"[FAIL] Case II: Forward account 2001 expected [4500, 4000], got {result}")
        failed += 1
    
    # Test Case III: Backfill account 3001 should have 7500 at position 1
    result = spark.sql("""
        SELECT balance_history[0] as bal_0, balance_history[1] as bal_1, balance_history[2] as bal_2
        FROM demo.test_db.summary 
        WHERE cons_acct_key = 3001 AND rpt_as_of_mo = '2025-12'
    """).first()
    if result and result['bal_1'] == 7500:
        print("[PASS] Case III: Backfill account 3001 has updated position 1")
        passed += 1
    else:
        print(f"[FAIL] Case III: Backfill account 3001 expected bal_1=7500, got {result}")
        failed += 1
    
    print("\n" + "=" * 80)
    print(f"TEST SUMMARY: {passed} passed, {failed} failed")
    print("=" * 80)
    
    return passed, failed


def main():
    print("=" * 80)
    print("SUMMARY PIPELINE V9 - TEST RUN")
    print("=" * 80)
    print(f"Start time: {datetime.now()}")
    
    # Create Spark session
    print("\nInitializing Spark...")
    spark = create_spark_session()
    print(f"Spark version: {spark.version}")
    
    try:
        # Setup
        setup_tables(spark)
        insert_test_data(spark)
        
        # Run pipeline
        stats = run_pipeline_test(spark)
        
        # Verify
        if stats:
            passed, failed = verify_results(spark)
            
            if failed == 0:
                print("\n*** ALL TESTS PASSED ***")
                return 0
            else:
                print(f"\n*** {failed} TESTS FAILED ***")
                return 1
        else:
            print("\n*** PIPELINE FAILED ***")
            return 1
    
    except Exception as e:
        print(f"\nTest run failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        print(f"\nEnd time: {datetime.now()}")
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
