"""
Test Data Generator for Summary Pipeline v9
============================================

Generates sample test data files for local testing.
"""

import json
import os
import random
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any


def generate_account_record(
    account_id: int,
    month: str,
    timestamp: str,
    balance: int = None,
    payment: int = None
) -> Dict[str, Any]:
    """Generate a single account record"""
    
    if balance is None:
        balance = random.randint(1000, 50000)
    if payment is None:
        payment = random.randint(100, 5000)
    
    return {
        "cons_acct_key": account_id,
        "rpt_as_of_mo": month,
        "base_ts": timestamp,
        "balance_am": balance,
        "actual_payment_am": payment,
        "credit_limit_am": random.randint(5000, 100000),
        "past_due_am": random.choice([0, 0, 0, 100, 500, 1000]),
        "days_past_due": random.choice([0, 0, 0, 0, 30, 60, 90]),
        "asset_class_cd": random.choice(["A", "A", "A", "B", "C"]),
        "insert_ts": timestamp,
        "update_ts": timestamp
    }


def generate_summary_record(
    account_id: int,
    month: str,
    timestamp: str,
    history_length: int = 36
) -> Dict[str, Any]:
    """Generate a single summary record"""
    
    # Generate history arrays with some random values
    balance_history = [random.randint(1000, 50000) if random.random() > 0.2 else None 
                      for _ in range(history_length)]
    payment_history = [random.randint(100, 5000) if random.random() > 0.2 else None 
                      for _ in range(history_length)]
    
    return {
        "cons_acct_key": account_id,
        "rpt_as_of_mo": month,
        "base_ts": timestamp,
        "balance_history": balance_history,
        "payment_history": payment_history,
        "credit_limit_history": [10000] * history_length,
        "past_due_history": [0] * history_length,
        "days_past_due_history": [0] * history_length,
        "payment_rating_history": ["0"] * history_length,
        "asset_class_history": ["A"] * history_length,
        "payment_history_grid": "?" * history_length
    }


def generate_test_dataset(
    num_new_accounts: int = 100,
    num_forward_accounts: int = 100,
    num_backfill_accounts: int = 50,
    base_month: str = "2025-12"
) -> Dict[str, List[Dict]]:
    """
    Generate a complete test dataset with all 3 cases.
    
    Returns:
        Dict with 'accounts' and 'summary' lists
    """
    
    accounts = []
    summary = []
    
    base_date = datetime.strptime(base_month, "%Y-%m")
    prev_month = (base_date - relativedelta(months=1)).strftime("%Y-%m")
    old_month = (base_date - relativedelta(months=6)).strftime("%Y-%m")
    
    current_id = 1000
    
    # Case I: New accounts
    for i in range(num_new_accounts):
        accounts.append(generate_account_record(
            account_id=current_id,
            month=base_month,
            timestamp=f"{base_month}-15 10:00:00"
        ))
        current_id += 1
    
    # Case II: Forward accounts
    for i in range(num_forward_accounts):
        # Add current month account
        accounts.append(generate_account_record(
            account_id=current_id,
            month=base_month,
            timestamp=f"{base_month}-15 10:00:00"
        ))
        # Add existing summary
        summary.append(generate_summary_record(
            account_id=current_id,
            month=prev_month,
            timestamp=f"{prev_month}-15 10:00:00"
        ))
        current_id += 1
    
    # Case III: Backfill accounts
    for i in range(num_backfill_accounts):
        # Add old month account with new timestamp
        accounts.append(generate_account_record(
            account_id=current_id,
            month=old_month,
            timestamp=f"{base_month}-20 10:00:00"  # Newer timestamp
        ))
        # Add existing summary at current month
        summary.append(generate_summary_record(
            account_id=current_id,
            month=base_month,
            timestamp=f"{base_month}-15 10:00:00"
        ))
        current_id += 1
    
    return {
        "accounts": accounts,
        "summary": summary,
        "stats": {
            "total_accounts": len(accounts),
            "total_summary": len(summary),
            "case_i_count": num_new_accounts,
            "case_ii_count": num_forward_accounts,
            "case_iii_count": num_backfill_accounts
        }
    }


def save_test_data(output_dir: str = "test_data"):
    """Save test data to files"""
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Small dataset
    small = generate_test_dataset(10, 10, 5)
    with open(os.path.join(output_dir, "small_accounts.json"), "w") as f:
        json.dump(small["accounts"], f, indent=2)
    with open(os.path.join(output_dir, "small_summary.json"), "w") as f:
        json.dump(small["summary"], f, indent=2)
    
    # Medium dataset
    medium = generate_test_dataset(100, 100, 50)
    with open(os.path.join(output_dir, "medium_accounts.json"), "w") as f:
        json.dump(medium["accounts"], f, indent=2)
    with open(os.path.join(output_dir, "medium_summary.json"), "w") as f:
        json.dump(medium["summary"], f, indent=2)
    
    # Large dataset
    large = generate_test_dataset(1000, 1000, 500)
    with open(os.path.join(output_dir, "large_accounts.json"), "w") as f:
        json.dump(large["accounts"], f, indent=2)
    with open(os.path.join(output_dir, "large_summary.json"), "w") as f:
        json.dump(large["summary"], f, indent=2)
    
    # Save stats
    stats = {
        "small": small["stats"],
        "medium": medium["stats"],
        "large": large["stats"]
    }
    with open(os.path.join(output_dir, "dataset_stats.json"), "w") as f:
        json.dump(stats, f, indent=2)
    
    print(f"Test data saved to {output_dir}/")
    print(f"  Small:  {small['stats']}")
    print(f"  Medium: {medium['stats']}")
    print(f"  Large:  {large['stats']}")


if __name__ == "__main__":
    save_test_data()
