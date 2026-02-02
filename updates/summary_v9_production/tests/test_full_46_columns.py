"""
Test Full 46 Production Columns - Summary Pipeline v9.4
========================================================

This test validates that all 46 production columns are correctly:
1. Mapped from source to destination
2. Transformed (sentinel values handled)
3. Inferred/derived columns created
4. Rolling history arrays built (7 arrays x 36 months)
5. Grid columns generated

Expected columns in summary table:
- 36 mapped columns (from source)
- 2 inferred columns (orig_loan_am, payment_rating_cd)
- 7 rolling history arrays
- 1 grid column
= 46 total columns
"""

import sys
import os
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

# Add pipeline directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pipeline'))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


def create_spark_session():
    """Create Spark session for testing"""
    return (SparkSession.builder
            .appName("Test_Full_46_Columns")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.demo.type", "rest")
            .config("spark.sql.catalog.demo.uri", "http://rest:8181")
            .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/")
            .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000")
            .config("spark.sql.catalog.demo.s3.path-style-access", "true")
            .config("spark.sql.defaultCatalog", "demo")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.driver.memory", "4g")
            .getOrCreate())


def get_production_config():
    """
    Production config with all 46 columns.
    Matches pipeline_config.json format exactly.
    """
    return {
        # Table names
        "source_table": "demo.full_cols_test.accounts",
        "destination_table": "demo.full_cols_test.summary",
        "latest_history_table": "demo.full_cols_test.latest_summary",
        
        # Temp table schema (v9.4)
        "temp_table_schema": "temp",
        
        # Keys
        "primary_column": "cons_acct_key",
        "partition_column": "rpt_as_of_mo",
        "max_identifier_column": "base_ts",
        
        # History length
        "history_length": 36,
        
        # Column mappings (36 source -> destination)
        "columns": {
            "cons_acct_key": "cons_acct_key",
            "bureau_mbr_id": "bureau_member_id",
            "port_type_cd": "portfolio_rating_type_cd",
            "acct_type_dtl_cd": "acct_type_dtl_cd",
            "acct_open_dt": "open_dt",
            "acct_closed_dt": "closed_dt",
            "pymt_terms_cd": "pymt_terms_cd",
            "pymt_terms_dtl_cd": "pymt_terms_dtl_cd",
            "acct_dt": "acct_dt",
            "acct_stat_cd": "acct_stat_cd",
            "acct_pymt_stat_cd": "acct_pymt_stat_cd",
            "acct_pymt_stat_dtl_cd": "acct_pymt_stat_dtl_cd",
            "acct_credit_ext_am": "credit_limit_am",
            "acct_bal_am": "balance_am",
            "past_due_am": "past_due_am",
            "actual_pymt_am": "actual_payment_am",
            "last_pymt_dt": "last_payment_dt",
            "schd_pymt_dt": "schd_pymt_dt",
            "next_schd_pymt_am": "emi_amt",
            "collateral_cd": "collateral_cd",
            "orig_pymt_due_dt": "orig_pymt_due_dt",
            "write_off_dt": "dflt_status_dt",
            "write_off_am": "write_off_am",
            "asset_class_cd_4in": "asset_class_cd",
            "days_past_due_ct_4in": "days_past_due",
            "high_credit_am_4in": "hi_credit_am",
            "cash_limit_am_4in": "cash_limit_am",
            "collateral_am_4in": "collateral_am",
            "total_write_off_am_4in": "charge_off_am",
            "principal_write_off_am_4in": "principal_write_off_am",
            "settled_am_4in": "settled_am",
            "interest_rate_4in": "interest_rate",
            "suit_filed_wilful_def_stat_cd_4in": "suit_filed_willful_dflt",
            "wo_settled_stat_cd_4in": "written_off_and_settled_status",
            "base_ts": "base_ts",
            "rpt_as_of_mo": "rpt_as_of_mo"
        },
        
        # Column transformations (sentinel value handling)
        "column_transformations": [
            {
                "name": "acct_type_dtl_cd",
                "mapper_expr": "CASE WHEN acct_type_dtl_cd = '' OR acct_type_dtl_cd IS NULL THEN '999' ELSE acct_type_dtl_cd END"
            },
            {
                "name": "charge_off_am",
                "mapper_expr": "CASE WHEN charge_off_am IN (-2147483647, -214748364, -21474836, -1) THEN NULL ELSE charge_off_am END"
            },
            {
                "name": "credit_limit_am",
                "mapper_expr": "CASE WHEN credit_limit_am IN (-2147483647, -214748364, -21474836, -1) THEN NULL ELSE credit_limit_am END"
            },
            {
                "name": "emi_amt",
                "mapper_expr": "CASE WHEN emi_amt IN (-2147483647, -214748364, -21474836, -1) THEN NULL ELSE emi_amt END"
            },
            {
                "name": "balance_am",
                "mapper_expr": "CASE WHEN balance_am IN (-2147483647, -214748364, -21474836, -1) THEN NULL ELSE balance_am END"
            },
            {
                "name": "actual_payment_am",
                "mapper_expr": "CASE WHEN actual_payment_am IN (-2147483647, -214748364, -21474836, -1) THEN NULL ELSE actual_payment_am END"
            },
            {
                "name": "past_due_am",
                "mapper_expr": "CASE WHEN past_due_am IN (-2147483647, -214748364, -21474836, -1) THEN NULL ELSE past_due_am END"
            },
            {
                "name": "hi_credit_am",
                "mapper_expr": "CASE WHEN hi_credit_am IN (-2147483647, -214748364, -21474836, -1) THEN NULL ELSE hi_credit_am END"
            },
            {
                "name": "cash_limit_am",
                "mapper_expr": "CASE WHEN cash_limit_am IN (-2147483647, -214748364, -21474836, -1) THEN NULL ELSE cash_limit_am END"
            },
            {
                "name": "collateral_am",
                "mapper_expr": "CASE WHEN collateral_am IN (-2147483647, -214748364, -21474836, -1) THEN NULL ELSE collateral_am END"
            },
            {
                "name": "principal_write_off_am",
                "mapper_expr": "CASE WHEN principal_write_off_am IN (-2147483647, -214748364, -21474836, -1) THEN NULL ELSE principal_write_off_am END"
            },
            {
                "name": "settled_am",
                "mapper_expr": "CASE WHEN settled_am IN (-2147483647, -214748364, -21474836, -1) THEN NULL ELSE settled_am END"
            },
            {
                "name": "write_off_am",
                "mapper_expr": "CASE WHEN write_off_am IN (-2147483647, -214748364, -21474836, -1) THEN NULL ELSE write_off_am END"
            }
        ],
        
        # Inferred columns (2 derived columns)
        "inferred_columns": [
            {
                "name": "orig_loan_am",
                "mapper_expr": "CASE WHEN acct_type_dtl_cd IN ('5', '214', '220', '213', '224') THEN hi_credit_am ELSE credit_limit_am END"
            },
            {
                "name": "payment_rating_cd",
                "mapper_expr": """CASE 
                    WHEN days_past_due <= -1 THEN 
                        CASE 
                            WHEN asset_class_cd IN ('1', '01', 'S') THEN 'S' 
                            WHEN asset_class_cd IN ('2', '02', 'B') THEN 'B' 
                            WHEN asset_class_cd IN ('3', '03', 'D') THEN 'D' 
                            WHEN asset_class_cd IN ('4', '04', 'D') THEN 'L' 
                            WHEN asset_class_cd IN ('5', '05', 'M') THEN 'M' 
                            ELSE asset_class_cd 
                        END 
                    WHEN days_past_due >= 0 THEN 
                        CASE 
                            WHEN days_past_due >= 0 AND days_past_due <= 29 THEN '0' 
                            WHEN days_past_due >= 30 AND days_past_due <= 59 THEN '1' 
                            WHEN days_past_due >= 60 AND days_past_due <= 89 THEN '2' 
                            WHEN days_past_due >= 90 AND days_past_due <= 119 THEN '3' 
                            WHEN days_past_due >= 120 AND days_past_due <= 149 THEN '4' 
                            WHEN days_past_due >= 150 AND days_past_due <= 179 THEN '5' 
                            WHEN days_past_due >= 180 THEN '6' 
                            ELSE NULL 
                        END 
                    ELSE NULL 
                END"""
            }
        ],
        
        # Date columns for validation
        "date_col_list": [
            "open_dt",
            "closed_dt",
            "acct_dt",
            "last_payment_dt",
            "schd_pymt_dt",
            "orig_pymt_due_dt",
            "dflt_status_dt"
        ],
        
        # Latest summary addon columns
        "latest_history_addon_cols": [
            "emi_amt",
            "bureau_member_id",
            "open_dt",
            "acct_pymt_stat_cd",
            "acct_type_dtl_cd",
            "acct_stat_cd",
            "pymt_terms_cd",
            "balance_am",
            "closed_dt",
            "dflt_status_dt",
            "last_payment_dt",
            "asset_class_cd",
            "days_past_due",
            "past_due_am",
            "charge_off_am",
            "suit_filed_willful_dflt",
            "written_off_and_settled_status",
            "portfolio_rating_type_cd",
            "actual_payment_am",
            "credit_limit_am",
            "interest_rate",
            "orig_loan_am",
            "payment_history_grid"
        ],
        
        # Rolling columns (7 arrays)
        "rolling_columns": [
            {
                "name": "actual_payment_am",
                "mapper_expr": "CASE WHEN actual_payment_am IN (-2147483647, -214748364, -21474836, -1) THEN NULL ELSE actual_payment_am END",
                "mapper_column": "actual_payment_am",
                "num_cols": 36,
                "type": "Integer"
            },
            {
                "name": "balance_am",
                "mapper_expr": "CASE WHEN balance_am IN (-2147483647, -214748364, -21474836, -1) THEN NULL ELSE balance_am END",
                "mapper_column": "balance_am",
                "num_cols": 36,
                "type": "Integer"
            },
            {
                "name": "credit_limit_am",
                "mapper_expr": "credit_limit_am",
                "mapper_column": "credit_limit_am",
                "num_cols": 36,
                "type": "Integer"
            },
            {
                "name": "past_due_am",
                "mapper_expr": "CASE WHEN past_due_am IN (-2147483647, -214748364, -21474836, -1) THEN NULL ELSE past_due_am END",
                "mapper_column": "past_due_am",
                "num_cols": 36,
                "type": "Integer"
            },
            {
                "name": "payment_rating_cd",
                "mapper_expr": "payment_rating_cd",
                "mapper_column": "payment_rating_cd",
                "num_cols": 36,
                "type": "String"
            },
            {
                "name": "days_past_due",
                "mapper_expr": "days_past_due",
                "mapper_column": "days_past_due",
                "num_cols": 36,
                "type": "Integer"
            },
            {
                "name": "asset_class_cd_4in",
                "mapper_expr": "asset_class_cd",
                "mapper_column": "asset_class_cd",
                "num_cols": 36,
                "type": "String"
            }
        ],
        
        # Grid columns
        "grid_columns": [
            {
                "name": "payment_history_grid",
                "mapper_rolling_column": "payment_rating_cd",
                "placeholder": "?",
                "seperator": ""
            }
        ],
        
        # Performance settings
        "performance": {
            "broadcast_threshold": 10000000,
            "min_partitions": 4,
            "max_partitions": 32,
            "cache_level": "MEMORY_AND_DISK"
        },
        
        # Spark settings
        "spark": {
            "app_name": "SummaryPipeline_v9.4_test_46cols",
            "adaptive_enabled": True,
            "adaptive_coalesce": True,
            "adaptive_skew_join": True,
            "broadcast_timeout": 300,
            "auto_broadcast_threshold": "100m",
            "shuffle_partitions": 200
        }
    }


def create_source_schema():
    """Create schema matching production source table with all 36 source columns"""
    return StructType([
        # Primary key
        StructField("cons_acct_key", LongType(), False),
        
        # Account identifiers
        StructField("bureau_mbr_id", StringType(), True),
        StructField("port_type_cd", StringType(), True),
        StructField("acct_type_dtl_cd", StringType(), True),
        
        # Dates
        StructField("acct_open_dt", DateType(), True),
        StructField("acct_closed_dt", DateType(), True),
        StructField("acct_dt", DateType(), True),
        StructField("last_pymt_dt", DateType(), True),
        StructField("schd_pymt_dt", DateType(), True),
        StructField("orig_pymt_due_dt", DateType(), True),
        StructField("write_off_dt", DateType(), True),
        
        # Payment terms
        StructField("pymt_terms_cd", StringType(), True),
        StructField("pymt_terms_dtl_cd", StringType(), True),
        
        # Status codes
        StructField("acct_stat_cd", StringType(), True),
        StructField("acct_pymt_stat_cd", StringType(), True),
        StructField("acct_pymt_stat_dtl_cd", StringType(), True),
        
        # Amounts (will be transformed for sentinel values)
        StructField("acct_credit_ext_am", IntegerType(), True),
        StructField("acct_bal_am", IntegerType(), True),
        StructField("past_due_am", IntegerType(), True),
        StructField("actual_pymt_am", IntegerType(), True),
        StructField("next_schd_pymt_am", IntegerType(), True),
        StructField("write_off_am", IntegerType(), True),
        StructField("high_credit_am_4in", IntegerType(), True),
        StructField("cash_limit_am_4in", IntegerType(), True),
        StructField("collateral_am_4in", IntegerType(), True),
        StructField("total_write_off_am_4in", IntegerType(), True),
        StructField("principal_write_off_am_4in", IntegerType(), True),
        StructField("settled_am_4in", IntegerType(), True),
        
        # Other fields
        StructField("collateral_cd", StringType(), True),
        StructField("asset_class_cd_4in", StringType(), True),
        StructField("days_past_due_ct_4in", IntegerType(), True),
        StructField("interest_rate_4in", DoubleType(), True),
        StructField("suit_filed_wilful_def_stat_cd_4in", StringType(), True),
        StructField("wo_settled_stat_cd_4in", StringType(), True),
        
        # Partition and timestamp
        StructField("rpt_as_of_mo", StringType(), False),
        StructField("base_ts", TimestampType(), False),
    ])


def create_empty_summary_tables(spark):
    """
    Create empty summary and latest_summary tables with full 46-column schema.
    This is required because the pipeline's temp table creation clones from summary.
    """
    print("\nCreating empty summary tables with 46-column schema...")
    
    # Create summary table with all 46 columns using SQL
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.full_cols_test.summary (
            -- Mapped columns (36)
            cons_acct_key BIGINT,
            bureau_member_id STRING,
            portfolio_rating_type_cd STRING,
            acct_type_dtl_cd STRING,
            open_dt DATE,
            closed_dt DATE,
            pymt_terms_cd STRING,
            pymt_terms_dtl_cd STRING,
            acct_dt DATE,
            acct_stat_cd STRING,
            acct_pymt_stat_cd STRING,
            acct_pymt_stat_dtl_cd STRING,
            credit_limit_am INT,
            balance_am INT,
            past_due_am INT,
            actual_payment_am INT,
            last_payment_dt DATE,
            schd_pymt_dt DATE,
            emi_amt INT,
            collateral_cd STRING,
            orig_pymt_due_dt DATE,
            dflt_status_dt DATE,
            write_off_am INT,
            asset_class_cd STRING,
            days_past_due INT,
            hi_credit_am INT,
            cash_limit_am INT,
            collateral_am INT,
            charge_off_am INT,
            principal_write_off_am INT,
            settled_am INT,
            interest_rate DOUBLE,
            suit_filed_willful_dflt STRING,
            written_off_and_settled_status STRING,
            base_ts TIMESTAMP,
            rpt_as_of_mo STRING,
            
            -- Inferred columns (2)
            orig_loan_am INT,
            payment_rating_cd STRING,
            
            -- Rolling history arrays (7)
            actual_payment_am_history ARRAY<INT>,
            balance_am_history ARRAY<INT>,
            credit_limit_am_history ARRAY<INT>,
            past_due_am_history ARRAY<INT>,
            payment_rating_cd_history ARRAY<STRING>,
            days_past_due_history ARRAY<INT>,
            asset_class_cd_4in_history ARRAY<STRING>,
            
            -- Grid columns (1)
            payment_history_grid STRING,
            
            -- Backfill tracking (1)
            updated_base_ts TIMESTAMP
        ) USING iceberg
    """)
    
    # Create latest_summary with same schema
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.full_cols_test.latest_summary (
            -- Mapped columns (36)
            cons_acct_key BIGINT,
            bureau_member_id STRING,
            portfolio_rating_type_cd STRING,
            acct_type_dtl_cd STRING,
            open_dt DATE,
            closed_dt DATE,
            pymt_terms_cd STRING,
            pymt_terms_dtl_cd STRING,
            acct_dt DATE,
            acct_stat_cd STRING,
            acct_pymt_stat_cd STRING,
            acct_pymt_stat_dtl_cd STRING,
            credit_limit_am INT,
            balance_am INT,
            past_due_am INT,
            actual_payment_am INT,
            last_payment_dt DATE,
            schd_pymt_dt DATE,
            emi_amt INT,
            collateral_cd STRING,
            orig_pymt_due_dt DATE,
            dflt_status_dt DATE,
            write_off_am INT,
            asset_class_cd STRING,
            days_past_due INT,
            hi_credit_am INT,
            cash_limit_am INT,
            collateral_am INT,
            charge_off_am INT,
            principal_write_off_am INT,
            settled_am INT,
            interest_rate DOUBLE,
            suit_filed_willful_dflt STRING,
            written_off_and_settled_status STRING,
            base_ts TIMESTAMP,
            rpt_as_of_mo STRING,
            
            -- Inferred columns (2)
            orig_loan_am INT,
            payment_rating_cd STRING,
            
            -- Rolling history arrays (7)
            actual_payment_am_history ARRAY<INT>,
            balance_am_history ARRAY<INT>,
            credit_limit_am_history ARRAY<INT>,
            past_due_am_history ARRAY<INT>,
            payment_rating_cd_history ARRAY<STRING>,
            days_past_due_history ARRAY<INT>,
            asset_class_cd_4in_history ARRAY<STRING>,
            
            -- Grid columns (1)
            payment_history_grid STRING,
            
            -- Backfill tracking (1)
            updated_base_ts TIMESTAMP
        ) USING iceberg
    """)
    
    print("Created empty summary and latest_summary tables with 47 columns")


def create_test_data(spark):
    """Create test data with all 36 source columns"""
    print("\n" + "=" * 80)
    print("CREATING TEST DATA WITH ALL 36 SOURCE COLUMNS")
    print("=" * 80)
    
    schema = create_source_schema()
    
    # Current month for test
    current_month = date(2026, 1, 1)
    base_ts = datetime(2026, 1, 15, 10, 0, 0)
    
    # Create test records
    test_data = []
    
    # Account 1001 - Case I (new account, single month)
    # All columns populated with realistic values
    test_data.append((
        1001,                          # cons_acct_key
        "BMB001",                      # bureau_mbr_id
        "I",                           # port_type_cd
        "5",                           # acct_type_dtl_cd (loan type for orig_loan_am test)
        date(2020, 5, 15),             # acct_open_dt
        None,                          # acct_closed_dt
        date(2026, 1, 1),              # acct_dt
        date(2025, 12, 15),            # last_pymt_dt
        date(2026, 1, 5),              # schd_pymt_dt
        date(2020, 6, 5),              # orig_pymt_due_dt
        None,                          # write_off_dt
        "M",                           # pymt_terms_cd
        "01",                          # pymt_terms_dtl_cd
        "11",                          # acct_stat_cd
        "01",                          # acct_pymt_stat_cd
        "001",                         # acct_pymt_stat_dtl_cd
        50000,                         # acct_credit_ext_am (credit_limit_am)
        45000,                         # acct_bal_am (balance_am)
        0,                             # past_due_am
        5000,                          # actual_pymt_am
        5000,                          # next_schd_pymt_am (emi_amt)
        0,                             # write_off_am
        100000,                        # high_credit_am_4in (hi_credit_am - for orig_loan_am)
        10000,                         # cash_limit_am_4in
        0,                             # collateral_am_4in
        0,                             # total_write_off_am_4in (charge_off_am)
        0,                             # principal_write_off_am_4in
        0,                             # settled_am_4in
        "C",                           # collateral_cd
        "01",                          # asset_class_cd_4in
        0,                             # days_past_due_ct_4in (0 DPD = rating '0')
        12.5,                          # interest_rate_4in
        "N",                           # suit_filed_wilful_def_stat_cd_4in
        "N",                           # wo_settled_stat_cd_4in
        "2026-01",                     # rpt_as_of_mo
        base_ts,                       # base_ts
    ))
    
    # Account 1002 - Case I with sentinel values (should be transformed to NULL)
    test_data.append((
        1002,
        "BMB002",
        "I",
        "",                            # Empty - should become '999'
        date(2019, 3, 10),
        None,
        date(2026, 1, 1),
        date(2025, 12, 20),
        date(2026, 1, 10),
        date(2019, 4, 10),
        None,
        "M",
        "01",
        "11",
        "01",
        "001",
        -2147483647,                   # Sentinel - should become NULL
        30000,
        -2147483647,                   # Sentinel - should become NULL
        -1,                            # Sentinel - should become NULL
        -2147483647,                   # Sentinel - should become NULL
        0,
        -2147483647,                   # Sentinel - should become NULL
        0,
        0,
        0,
        0,
        0,
        "N",
        "01",
        15,                            # 15 DPD = rating '0'
        10.0,
        "N",
        "N",
        "2026-01",
        base_ts,
    ))
    
    # Account 1003 - Case I with 60 DPD (payment rating '2')
    test_data.append((
        1003,
        "BMB003",
        "I",
        "214",                         # Loan type for orig_loan_am test
        date(2018, 6, 1),
        None,
        date(2026, 1, 1),
        date(2025, 10, 15),
        date(2025, 11, 5),
        date(2018, 7, 5),
        None,
        "M",
        "01",
        "21",                          # Delinquent status
        "02",
        "002",
        80000,
        75000,
        15000,                         # Past due
        0,                             # No payment
        8000,
        0,
        120000,                        # hi_credit_am (for orig_loan_am)
        5000,
        0,
        0,
        0,
        0,
        "N",
        "02",                          # Asset class
        65,                            # 65 DPD = rating '2'
        14.5,
        "N",
        "N",
        "2026-01",
        base_ts,
    ))
    
    # Account 2001 - Existing account for Case II (forward entry)
    # First create existing data in Dec 2025
    prev_month = date(2025, 12, 1)
    prev_ts = datetime(2025, 12, 15, 10, 0, 0)
    
    test_data.append((
        2001,
        "BMB004",
        "I",
        "10",
        date(2021, 1, 15),
        None,
        date(2025, 12, 1),
        date(2025, 11, 25),
        date(2025, 12, 5),
        date(2021, 2, 15),
        None,
        "M",
        "01",
        "11",
        "01",
        "001",
        100000,
        60000,
        0,
        8000,
        8000,
        0,
        100000,
        20000,
        0,
        0,
        0,
        0,
        "N",
        "01",
        0,
        11.0,
        "N",
        "N",
        "2025-12",
        prev_ts,
    ))
    
    # Account 2001 - Jan 2026 (forward entry)
    test_data.append((
        2001,
        "BMB004",
        "I",
        "10",
        date(2021, 1, 15),
        None,
        date(2026, 1, 1),
        date(2025, 12, 28),
        date(2026, 1, 5),
        date(2021, 2, 15),
        None,
        "M",
        "01",
        "11",
        "01",
        "001",
        100000,
        52000,                         # Lower balance
        0,
        8000,
        8000,
        0,
        100000,
        20000,
        0,
        0,
        0,
        0,
        "N",
        "01",
        0,
        11.0,
        "N",
        "N",
        "2026-01",
        base_ts,
    ))
    
    # Account 3001 - Case IV (bulk historical - 3 months)
    for i, month_offset in enumerate([3, 2, 1]):  # Oct, Nov, Dec 2025
        m = current_month - relativedelta(months=month_offset)
        ts = datetime(m.year, m.month, 15, 10, 0, 0)
        balance = 40000 - (3 - month_offset) * 2000  # Decreasing balance
        
        test_data.append((
            3001,
            "BMB005",
            "I",
            "220",                     # Loan type
            date(2022, 8, 1),
            None,
            m,
            m - relativedelta(days=5),
            m + relativedelta(days=5),
            date(2022, 9, 1),
            None,
            "M",
            "01",
            "11",
            "01",
            "001",
            60000,
            balance,
            0,
            3000,
            3000,
            0,
            80000,                     # hi_credit_am for orig_loan_am
            0,
            0,
            0,
            0,
            0,
            "N",
            "01",
            0,
            9.5,
            "N",
            "N",
            m.strftime("%Y-%m"),
            ts,
        ))
    
    # Create DataFrame
    df = spark.createDataFrame(test_data, schema)
    
    # Drop existing tables and create new ones
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.full_cols_test")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.temp")
    spark.sql("DROP TABLE IF EXISTS demo.full_cols_test.accounts")
    spark.sql("DROP TABLE IF EXISTS demo.full_cols_test.summary")
    spark.sql("DROP TABLE IF EXISTS demo.full_cols_test.latest_summary")
    
    # Write accounts table
    df.writeTo("demo.full_cols_test.accounts").using("iceberg").createOrReplace()
    
    # Create empty summary and latest_summary tables with full 46-column schema
    # This is required because the pipeline's temp table creation clones from summary
    create_empty_summary_tables(spark)
    
    print(f"Created {df.count()} test records with all 36 source columns")
    print("\nSource columns:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i:2d}. {col}")
    
    return df


def setup_existing_summary(spark, config):
    """Create existing summary for account 2001 (for Case II test)"""
    print("\n" + "=" * 80)
    print("SETTING UP EXISTING SUMMARY FOR CASE II TEST")
    print("=" * 80)
    
    # Create minimal summary table schema with all expected columns
    # Get column list from a sample row
    from summary_pipeline import run_pipeline
    
    # First run pipeline with just the Dec 2025 record for account 2001
    # Filter to only Dec 2025 record
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW dec_only AS
        SELECT * FROM demo.full_cols_test.accounts
        WHERE cons_acct_key = 2001 AND rpt_as_of_mo = '2025-12'
    """)
    
    # Create summary/latest_summary tables from Dec 2025 data
    # We'll let the pipeline create the tables on first run
    
    print("Existing summary will be created by first pipeline run")


def run_pipeline_test(spark, config):
    """Run the pipeline with full 46-column config"""
    print("\n" + "=" * 80)
    print("RUNNING PIPELINE WITH FULL 46-COLUMN PRODUCTION CONFIG")
    print("=" * 80)
    
    try:
        from summary_pipeline import run_pipeline
        
        # Run pipeline
        result = run_pipeline(spark, config)
        
        print("\n" + "-" * 60)
        print("PIPELINE RESULTS")
        print("-" * 60)
        if result:
            print(f"Total records:       {result.get('total_records', 'N/A')}")
            print(f"Case I (new):        {result.get('case_i_count', 'N/A')}")
            print(f"Case II (forward):   {result.get('case_ii_count', 'N/A')}")
            print(f"Case III (backfill): {result.get('case_iii_count', 'N/A')}")
            print(f"Case IV (bulk):      {result.get('case_iv_count', 'N/A')}")
            print(f"Records written:     {result.get('records_written', 'N/A')}")
        
        return result
        
    except Exception as e:
        import traceback
        print(f"Pipeline error: {e}")
        traceback.print_exc()
        return None


def validate_46_columns(spark):
    """Validate that all 46 columns exist in the summary table"""
    print("\n" + "=" * 80)
    print("VALIDATING 46 PRODUCTION COLUMNS")
    print("=" * 80)
    
    # Expected columns (46 total)
    expected_columns = [
        # Mapped columns (36)
        "cons_acct_key",
        "bureau_member_id",
        "portfolio_rating_type_cd",
        "acct_type_dtl_cd",
        "open_dt",
        "closed_dt",
        "pymt_terms_cd",
        "pymt_terms_dtl_cd",
        "acct_dt",
        "acct_stat_cd",
        "acct_pymt_stat_cd",
        "acct_pymt_stat_dtl_cd",
        "credit_limit_am",
        "balance_am",
        "past_due_am",
        "actual_payment_am",
        "last_payment_dt",
        "schd_pymt_dt",
        "emi_amt",
        "collateral_cd",
        "orig_pymt_due_dt",
        "dflt_status_dt",
        "write_off_am",
        "asset_class_cd",
        "days_past_due",
        "hi_credit_am",
        "cash_limit_am",
        "collateral_am",
        "charge_off_am",
        "principal_write_off_am",
        "settled_am",
        "interest_rate",
        "suit_filed_willful_dflt",
        "written_off_and_settled_status",
        "base_ts",
        "rpt_as_of_mo",
        
        # Inferred columns (2)
        "orig_loan_am",
        "payment_rating_cd",
        
        # Rolling history arrays (7)
        "actual_payment_am_history",
        "balance_am_history",
        "credit_limit_am_history",
        "past_due_am_history",
        "payment_rating_cd_history",
        "days_past_due_history",
        "asset_class_cd_4in_history",
        
        # Grid columns (1)
        "payment_history_grid",
    ]
    
    # Read summary table
    df = spark.read.table("demo.full_cols_test.summary")
    actual_columns = df.columns
    
    print(f"\nExpected columns: {len(expected_columns)}")
    print(f"Actual columns:   {len(actual_columns)}")
    
    # Check for missing columns
    missing = []
    found = []
    
    for col in expected_columns:
        if col in actual_columns:
            found.append(col)
        else:
            missing.append(col)
    
    # Check for extra columns
    extra = [col for col in actual_columns if col not in expected_columns]
    
    print("\n" + "-" * 60)
    print("COLUMN VALIDATION RESULTS")
    print("-" * 60)
    
    passed = 0
    failed = 0
    
    # Report found columns
    print(f"\nFOUND ({len(found)}/{len(expected_columns)}):")
    for col in found:
        print(f"  [PASS] {col}")
        passed += 1
    
    # Report missing columns
    if missing:
        print(f"\nMISSING ({len(missing)}):")
        for col in missing:
            print(f"  [FAIL] {col}")
            failed += 1
    
    # Report extra columns
    if extra:
        print(f"\nEXTRA COLUMNS ({len(extra)}):")
        for col in extra:
            print(f"  [INFO] {col}")
    
    return passed, failed, missing, extra


def validate_column_values(spark):
    """Validate that column values are correct"""
    print("\n" + "=" * 80)
    print("VALIDATING COLUMN VALUES")
    print("=" * 80)
    
    passed = 0
    failed = 0
    
    df = spark.read.table("demo.full_cols_test.summary")
    
    # Test 1: Sentinel value transformation
    print("\n1. SENTINEL VALUE TRANSFORMATION:")
    r = df.filter("cons_acct_key = 1002").first()
    if r:
        # credit_limit_am should be NULL (was -2147483647)
        ok = r["credit_limit_am"] is None
        print(f"   credit_limit_am = {r['credit_limit_am']} (expected NULL): {'PASS' if ok else 'FAIL'}")
        passed += 1 if ok else 0
        failed += 0 if ok else 1
        
        # acct_type_dtl_cd should be '999' (was empty string)
        ok = r["acct_type_dtl_cd"] == "999"
        print(f"   acct_type_dtl_cd = '{r['acct_type_dtl_cd']}' (expected '999'): {'PASS' if ok else 'FAIL'}")
        passed += 1 if ok else 0
        failed += 0 if ok else 1
    
    # Test 2: Inferred column - orig_loan_am
    print("\n2. INFERRED COLUMN - orig_loan_am:")
    r = df.filter("cons_acct_key = 1001").first()
    if r:
        # Account 1001 has acct_type_dtl_cd = '5', so orig_loan_am = hi_credit_am (100000)
        ok = r["orig_loan_am"] == 100000
        print(f"   Account 1001 (type '5'): orig_loan_am = {r['orig_loan_am']} (expected 100000): {'PASS' if ok else 'FAIL'}")
        passed += 1 if ok else 0
        failed += 0 if ok else 1
    
    r = df.filter("cons_acct_key = 1003").first()
    if r:
        # Account 1003 has acct_type_dtl_cd = '214', so orig_loan_am = hi_credit_am (120000)
        ok = r["orig_loan_am"] == 120000
        print(f"   Account 1003 (type '214'): orig_loan_am = {r['orig_loan_am']} (expected 120000): {'PASS' if ok else 'FAIL'}")
        passed += 1 if ok else 0
        failed += 0 if ok else 1
    
    # Test 3: Inferred column - payment_rating_cd
    print("\n3. INFERRED COLUMN - payment_rating_cd:")
    r = df.filter("cons_acct_key = 1001").first()
    if r:
        # Account 1001 has 0 DPD, so payment_rating_cd = '0'
        ok = r["payment_rating_cd"] == "0"
        print(f"   Account 1001 (0 DPD): payment_rating_cd = '{r['payment_rating_cd']}' (expected '0'): {'PASS' if ok else 'FAIL'}")
        passed += 1 if ok else 0
        failed += 0 if ok else 1
    
    r = df.filter("cons_acct_key = 1003").first()
    if r:
        # Account 1003 has 65 DPD, so payment_rating_cd = '2'
        ok = r["payment_rating_cd"] == "2"
        print(f"   Account 1003 (65 DPD): payment_rating_cd = '{r['payment_rating_cd']}' (expected '2'): {'PASS' if ok else 'FAIL'}")
        passed += 1 if ok else 0
        failed += 0 if ok else 1
    
    # Test 4: Rolling history arrays
    print("\n4. ROLLING HISTORY ARRAYS:")
    r = df.filter("cons_acct_key = 1001").first()
    if r:
        # Check balance_am_history
        bal_hist = r["balance_am_history"]
        ok = bal_hist[0] == 45000 and len(bal_hist) == 36
        print(f"   balance_am_history[0] = {bal_hist[0]}, len = {len(bal_hist)} (expected 45000, 36): {'PASS' if ok else 'FAIL'}")
        passed += 1 if ok else 0
        failed += 0 if ok else 1
        
        # Check payment_rating_cd_history
        pr_hist = r["payment_rating_cd_history"]
        ok = pr_hist[0] == "0" and len(pr_hist) == 36
        print(f"   payment_rating_cd_history[0] = '{pr_hist[0]}', len = {len(pr_hist)} (expected '0', 36): {'PASS' if ok else 'FAIL'}")
        passed += 1 if ok else 0
        failed += 0 if ok else 1
    
    # Test 5: Grid column
    print("\n5. GRID COLUMN:")
    r = df.filter("cons_acct_key = 1001").first()
    if r:
        grid = r["payment_history_grid"]
        ok = grid is not None and len(grid) == 36 and grid[0] == "0"
        print(f"   payment_history_grid[0] = '{grid[0] if grid else None}', len = {len(grid) if grid else 0} (expected '0', 36): {'PASS' if ok else 'FAIL'}")
        passed += 1 if ok else 0
        failed += 0 if ok else 1
    
    # Test 6: Case IV (bulk historical)
    print("\n6. CASE IV - BULK HISTORICAL:")
    rows = df.filter("cons_acct_key = 3001").orderBy("rpt_as_of_mo").collect()
    ok = len(rows) == 3
    print(f"   Account 3001 rows = {len(rows)} (expected 3): {'PASS' if ok else 'FAIL'}")
    passed += 1 if ok else 0
    failed += 0 if ok else 1
    
    if len(rows) >= 3:
        # Dec 2025 should have Oct and Nov in history
        dec_row = rows[2]
        bal_hist = dec_row["balance_am_history"]
        ok = bal_hist[0] == 38000 and bal_hist[1] == 36000 and bal_hist[2] == 34000
        print(f"   Dec 2025: bal[0]={bal_hist[0]}, bal[1]={bal_hist[1]}, bal[2]={bal_hist[2]} (expected 38000, 36000, 34000): {'PASS' if ok else 'FAIL'}")
        passed += 1 if ok else 0
        failed += 0 if ok else 1
    
    return passed, failed


def main():
    """Main test function"""
    print("=" * 80)
    print("TEST FULL 46 PRODUCTION COLUMNS - SUMMARY PIPELINE v9.4")
    print("=" * 80)
    print(f"Start time: {datetime.now()}")
    
    spark = create_spark_session()
    config = get_production_config()
    
    total_passed = 0
    total_failed = 0
    
    try:
        # Step 1: Create test data
        create_test_data(spark)
        
        # Step 2: Run pipeline
        run_pipeline_test(spark, config)
        
        # Step 3: Validate 46 columns exist
        passed, failed, missing, extra = validate_46_columns(spark)
        total_passed += passed
        total_failed += failed
        
        # Step 4: Validate column values
        passed, failed = validate_column_values(spark)
        total_passed += passed
        total_failed += failed
        
        # Final summary
        print("\n" + "=" * 80)
        print(f"FINAL RESULTS: {total_passed} passed, {total_failed} failed")
        print("=" * 80)
        
        if total_failed == 0 and not missing:
            print("\n*** ALL 46 COLUMNS VALIDATED SUCCESSFULLY ***")
        else:
            if missing:
                print(f"\n*** MISSING COLUMNS: {missing} ***")
            print("\n*** SOME TESTS FAILED ***")
        
    except Exception as e:
        import traceback
        print(f"\nTest error: {e}")
        traceback.print_exc()
        total_failed += 1
    
    finally:
        print(f"\nEnd time: {datetime.now()}")
        spark.stop()
    
    return total_failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
