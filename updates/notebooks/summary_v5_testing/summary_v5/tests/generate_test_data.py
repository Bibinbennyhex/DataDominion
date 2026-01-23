#!/usr/bin/env python3
"""
Test Data Generator for Summary Pipeline v4.0
==============================================

Generates realistic test data for accounts_all table with production-like
characteristics for testing the summary pipeline.

Features:
- Configurable number of accounts and months
- Realistic data patterns (DPD distribution, balances, payments)
- Gap scenarios for testing month gaps
- Backfill scenarios for testing late-arriving data
- Scale simulation support

Usage:
    spark-submit generate_test_data.py --config summary_config.json \
        --accounts 100000 --months 12 --start-month 2024-01

Author: DataDominion Team
"""

import json
import argparse
import logging
import sys
import random
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType,
    DateType, TimestampType, DecimalType, Row
)
import pyspark.sql.functions as F

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================
# Configuration
# ============================================================

# DPD distribution (realistic bell curve with some delinquency)
DPD_DISTRIBUTION = [
    (0, 0.70),      # 70% current
    (1, 0.05),      # 5% 1-29 DPD
    (30, 0.08),     # 8% 30-59 DPD
    (60, 0.06),     # 6% 60-89 DPD
    (90, 0.05),     # 5% 90-119 DPD
    (120, 0.03),    # 3% 120-149 DPD
    (150, 0.02),    # 2% 150-179 DPD
    (180, 0.01),    # 1% 180+ DPD
]

# Account types
ACCT_TYPES = ['5', '214', '220', '213', '224', '101', '102', '103']
ACCT_TYPE_WEIGHTS = [0.25, 0.15, 0.15, 0.10, 0.10, 0.10, 0.08, 0.07]

# Portfolio types
PORT_TYPES = ['R', 'I', 'O', 'M']
PORT_TYPE_WEIGHTS = [0.40, 0.35, 0.15, 0.10]

# Asset classes
ASSET_CLASSES = ['1', '2', '3', '4', '5', '01', '02', 'S', 'B', 'D', 'L', 'M']

# Account status
ACCT_STATS = ['A', 'C', 'D', 'I', 'O']
ACCT_STAT_WEIGHTS = [0.85, 0.05, 0.03, 0.02, 0.05]


# ============================================================
# Data Generation Functions
# ============================================================

def random_choice_weighted(items: List, weights: List) -> any:
    """Choose item based on weights."""
    return random.choices(items, weights=weights, k=1)[0]


def generate_dpd() -> int:
    """Generate realistic DPD value."""
    items = [d[0] for d in DPD_DISTRIBUTION]
    weights = [d[1] for d in DPD_DISTRIBUTION]
    base_dpd = random_choice_weighted(items, weights)
    
    if base_dpd == 0:
        return 0
    elif base_dpd == 1:
        return random.randint(1, 29)
    elif base_dpd == 180:
        return random.randint(180, 365)
    else:
        return base_dpd + random.randint(0, 29)


def generate_balance(credit_limit: int, dpd: int) -> int:
    """Generate balance based on credit limit and DPD."""
    utilization = random.uniform(0.3, 0.9)
    if dpd > 90:
        utilization = random.uniform(0.7, 1.0)  # Higher utilization for delinquent
    return int(credit_limit * utilization)


def generate_payment(balance: int, dpd: int) -> int:
    """Generate payment amount."""
    if dpd == 0:
        # Current - paying 2-10% of balance
        return int(balance * random.uniform(0.02, 0.10))
    elif dpd < 60:
        # Slightly delinquent - partial payment
        return int(balance * random.uniform(0.01, 0.05))
    else:
        # Severely delinquent - minimal or no payment
        return random.choice([0, 0, 0, int(balance * 0.01)])


def generate_asset_class(dpd: int) -> str:
    """Generate asset class based on DPD."""
    if dpd == 0:
        return random.choice(['1', '01', 'S'])
    elif dpd < 30:
        return random.choice(['1', '01', 'S', '2', '02'])
    elif dpd < 90:
        return random.choice(['2', '02', 'B', '3', '03', 'D'])
    elif dpd < 180:
        return random.choice(['3', '03', 'D', '4', '04', 'L'])
    else:
        return random.choice(['4', '04', 'L', '5', '05', 'M'])


def generate_account_data(
    cons_acct_key: int,
    month: date,
    base_ts: datetime,
    prev_data: Dict = None
) -> Dict:
    """Generate data for one account-month."""
    
    # Account characteristics (stable across months)
    if prev_data:
        credit_limit = prev_data.get('credit_limit', random.randint(10000, 500000))
        acct_type = prev_data.get('acct_type', random_choice_weighted(ACCT_TYPES, ACCT_TYPE_WEIGHTS))
        port_type = prev_data.get('port_type', random_choice_weighted(PORT_TYPES, PORT_TYPE_WEIGHTS))
        open_dt = prev_data.get('open_dt')
        emi = prev_data.get('emi', random.randint(1000, 20000))
        prev_dpd = prev_data.get('dpd', 0)
    else:
        credit_limit = random.randint(10000, 500000)
        acct_type = random_choice_weighted(ACCT_TYPES, ACCT_TYPE_WEIGHTS)
        port_type = random_choice_weighted(PORT_TYPES, PORT_TYPE_WEIGHTS)
        open_dt = month - timedelta(days=random.randint(30, 1000))
        emi = random.randint(1000, 20000)
        prev_dpd = 0
    
    # Generate DPD (with some continuity from previous month)
    if prev_data and random.random() < 0.7:
        # 70% chance DPD stays similar
        dpd = max(0, prev_dpd + random.randint(-10, 30))
    else:
        dpd = generate_dpd()
    
    balance = generate_balance(credit_limit, dpd)
    payment = generate_payment(balance, dpd)
    past_due = int(balance * min(dpd / 100, 0.5)) if dpd > 0 else 0
    asset_class = generate_asset_class(dpd)
    acct_stat = 'A' if dpd < 90 else random_choice_weighted(ACCT_STATS, ACCT_STAT_WEIGHTS)
    
    # Build data dict
    return {
        'cons_acct_key': cons_acct_key,
        'bureau_mbr_id': f"BMB{cons_acct_key % 1000:05d}",
        'port_type_cd': port_type,
        'acct_type_dtl_cd': acct_type,
        'pymt_terms_cd': random.choice(['M', 'W', 'B', 'Q']),
        'pymt_terms_dtl_cd': random.choice(['1', '2', '3']),
        'acct_open_dt': open_dt,
        'acct_closed_dt': None,
        'acct_dt': month,
        'last_pymt_dt': month - timedelta(days=random.randint(1, 30)) if payment > 0 else None,
        'schd_pymt_dt': month + timedelta(days=15),
        'orig_pymt_due_dt': open_dt + timedelta(days=30),
        'write_off_dt': month if dpd >= 180 and random.random() < 0.1 else None,
        'acct_stat_cd': acct_stat,
        'acct_pymt_stat_cd': '0' if dpd == 0 else str(min(dpd // 30, 6)),
        'acct_pymt_stat_dtl_cd': '00',
        'acct_credit_ext_am': credit_limit,
        'acct_bal_am': balance,
        'past_due_am': past_due,
        'actual_pymt_am': payment,
        'next_schd_pymt_am': emi,
        'write_off_am': balance if dpd >= 180 else 0,
        'asset_class_cd_4in': asset_class,
        'days_past_due_ct_4in': dpd,
        'high_credit_am_4in': credit_limit,
        'cash_limit_am_4in': int(credit_limit * 0.3),
        'collateral_am_4in': 0,
        'total_write_off_am_4in': 0,
        'principal_write_off_am_4in': 0,
        'settled_am_4in': 0,
        'interest_rate_4in': round(random.uniform(8.0, 24.0), 4),
        'suit_filed_wilful_def_stat_cd_4in': None,
        'wo_settled_stat_cd_4in': None,
        'collateral_cd': None,
        'rpt_as_of_mo': month.strftime('%Y-%m'),
        'base_ts': base_ts,
        # Keep for next iteration
        '_credit_limit': credit_limit,
        '_acct_type': acct_type,
        '_port_type': port_type,
        '_open_dt': open_dt,
        '_emi': emi,
        '_dpd': dpd,
    }


def generate_test_dataset(
    num_accounts: int,
    num_months: int,
    start_month: date,
    gap_pct: float = 0.05,
    backfill_pct: float = 0.02
) -> List[Dict]:
    """
    Generate complete test dataset.
    
    Args:
        num_accounts: Number of accounts
        num_months: Number of months
        start_month: Starting month
        gap_pct: Percentage of accounts with gaps
        backfill_pct: Percentage to generate as backfill (later timestamp)
    """
    logger.info(f"Generating {num_accounts} accounts x {num_months} months...")
    
    records = []
    base_ts = datetime.now() - timedelta(days=num_months * 30)
    
    # Generate account IDs
    account_ids = list(range(1, num_accounts + 1))
    
    # Determine which accounts have gaps
    gap_accounts = set(random.sample(account_ids, int(num_accounts * gap_pct)))
    
    for account_id in account_ids:
        prev_data = None
        current_month = start_month
        
        # Determine gap months for this account
        has_gap = account_id in gap_accounts
        gap_month_idx = random.randint(2, num_months - 2) if has_gap else -1
        
        for month_idx in range(num_months):
            # Skip if gap month
            if has_gap and month_idx == gap_month_idx:
                current_month = (current_month.replace(day=28) + relativedelta(days=4)).replace(day=1)
                continue
            
            # Generate timestamp (later for backfill simulation)
            is_backfill = random.random() < backfill_pct and month_idx < num_months - 2
            if is_backfill:
                # Backfill: record arrives after current processing
                record_ts = base_ts + timedelta(days=(num_months + 1) * 30)
            else:
                record_ts = base_ts + timedelta(days=month_idx * 30)
            
            data = generate_account_data(
                account_id,
                current_month,
                record_ts,
                prev_data
            )
            
            # Remove internal tracking fields
            clean_data = {k: v for k, v in data.items() if not k.startswith('_')}
            records.append(clean_data)
            
            # Keep for next month
            prev_data = {
                'credit_limit': data['_credit_limit'],
                'acct_type': data['_acct_type'],
                'port_type': data['_port_type'],
                'open_dt': data['_open_dt'],
                'emi': data['_emi'],
                'dpd': data['_dpd'],
            }
            
            current_month = (current_month.replace(day=28) + relativedelta(days=4)).replace(day=1)
    
    logger.info(f"Generated {len(records)} records")
    return records


# ============================================================
# Spark Operations
# ============================================================

def get_accounts_schema() -> StructType:
    """Schema for accounts_all table."""
    return StructType([
        StructField("cons_acct_key", LongType(), False),
        StructField("bureau_mbr_id", StringType(), True),
        StructField("port_type_cd", StringType(), True),
        StructField("acct_type_dtl_cd", StringType(), True),
        StructField("pymt_terms_cd", StringType(), True),
        StructField("pymt_terms_dtl_cd", StringType(), True),
        StructField("acct_open_dt", DateType(), True),
        StructField("acct_closed_dt", DateType(), True),
        StructField("acct_dt", DateType(), True),
        StructField("last_pymt_dt", DateType(), True),
        StructField("schd_pymt_dt", DateType(), True),
        StructField("orig_pymt_due_dt", DateType(), True),
        StructField("write_off_dt", DateType(), True),
        StructField("acct_stat_cd", StringType(), True),
        StructField("acct_pymt_stat_cd", StringType(), True),
        StructField("acct_pymt_stat_dtl_cd", StringType(), True),
        StructField("acct_credit_ext_am", IntegerType(), True),
        StructField("acct_bal_am", IntegerType(), True),
        StructField("past_due_am", IntegerType(), True),
        StructField("actual_pymt_am", IntegerType(), True),
        StructField("next_schd_pymt_am", IntegerType(), True),
        StructField("write_off_am", IntegerType(), True),
        StructField("asset_class_cd_4in", StringType(), True),
        StructField("days_past_due_ct_4in", IntegerType(), True),
        StructField("high_credit_am_4in", IntegerType(), True),
        StructField("cash_limit_am_4in", IntegerType(), True),
        StructField("collateral_am_4in", IntegerType(), True),
        StructField("total_write_off_am_4in", IntegerType(), True),
        StructField("principal_write_off_am_4in", IntegerType(), True),
        StructField("settled_am_4in", IntegerType(), True),
        StructField("interest_rate_4in", DecimalType(10, 4), True),
        StructField("suit_filed_wilful_def_stat_cd_4in", StringType(), True),
        StructField("wo_settled_stat_cd_4in", StringType(), True),
        StructField("collateral_cd", StringType(), True),
        StructField("rpt_as_of_mo", StringType(), False),
        StructField("base_ts", TimestampType(), True),
    ])


def write_to_iceberg(
    spark: SparkSession,
    records: List[Dict],
    table_name: str,
    mode: str = "append"
):
    """Write records to Iceberg table."""
    logger.info(f"Writing {len(records)} records to {table_name}...")
    
    schema = get_accounts_schema()
    
    # Convert to Rows
    rows = []
    for record in records:
        row_dict = {}
        for field in schema.fields:
            val = record.get(field.name)
            # Convert Decimal if needed
            if field.name == 'interest_rate_4in' and val is not None:
                from decimal import Decimal
                val = Decimal(str(val))
            row_dict[field.name] = val
        rows.append(Row(**row_dict))
    
    df = spark.createDataFrame(rows, schema)
    
    if mode == "overwrite":
        # Clear and rewrite
        spark.sql(f"DELETE FROM {table_name} WHERE 1=1")
    
    df.writeTo(table_name).append()
    
    # Show stats
    count = spark.table(table_name).count()
    months = spark.table(table_name).select("rpt_as_of_mo").distinct().count()
    logger.info(f"Table {table_name} now has {count} records across {months} months")


# ============================================================
# Main
# ============================================================

def create_spark_session() -> SparkSession:
    """Create Spark session."""
    return SparkSession.builder \
        .appName("SummaryPipeline_TestDataGenerator") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()


def main():
    parser = argparse.ArgumentParser(description='Generate test data')
    parser.add_argument('--config', type=str, required=True, help='Config JSON path')
    parser.add_argument('--accounts', type=int, default=1000, help='Number of accounts')
    parser.add_argument('--months', type=int, default=12, help='Number of months')
    parser.add_argument('--start-month', type=str, default='2024-01', help='Start month (YYYY-MM)')
    parser.add_argument('--gap-pct', type=float, default=0.05, help='Percentage with gaps')
    parser.add_argument('--backfill-pct', type=float, default=0.02, help='Percentage backfill')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite existing data')
    
    args = parser.parse_args()
    
    # Load config
    with open(args.config, 'r') as f:
        config = json.load(f)
    
    # Parse start month
    start_month = datetime.strptime(args.start_month, '%Y-%m').date().replace(day=1)
    
    # Generate data
    records = generate_test_dataset(
        num_accounts=args.accounts,
        num_months=args.months,
        start_month=start_month,
        gap_pct=args.gap_pct,
        backfill_pct=args.backfill_pct
    )
    
    # Write to table
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        mode = "overwrite" if args.overwrite else "append"
        write_to_iceberg(spark, records, config['source_table'], mode)
        
        logger.info("Test data generation complete!")
        
    except Exception as e:
        logger.error(f"Failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
