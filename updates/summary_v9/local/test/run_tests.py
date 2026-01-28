"""
Local Test Runner for Summary Pipeline v9
==========================================

Runs test scenarios in local Docker Spark environment.
"""

import sys
import os
import json
from datetime import datetime
from typing import Dict, Any, List

# Add parent directories to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'base'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'test'))

from local_spark_config import (
    create_local_spark_session, 
    create_test_tables, 
    cleanup_test_tables,
    LocalTestConfig
)
from test_scenarios import get_all_scenarios, get_scenario_by_name


class TestRunner:
    """Runs v9 pipeline tests locally"""
    
    def __init__(self):
        self.spark = None
        self.config = None
        self.results = []
    
    def setup(self):
        """Initialize Spark and create test tables"""
        print("=" * 80)
        print("SETTING UP TEST ENVIRONMENT")
        print("=" * 80)
        
        self.spark = create_local_spark_session()
        create_test_tables(self.spark)
        self.config = LocalTestConfig()
        
        print("Setup complete\n")
    
    def teardown(self):
        """Clean up after tests"""
        print("\n" + "=" * 80)
        print("CLEANING UP")
        print("=" * 80)
        
        if self.spark:
            cleanup_test_tables(self.spark)
            self.spark.stop()
        
        print("Cleanup complete")
    
    def load_test_data(self, scenario: Dict[str, Any]):
        """Load test data for a scenario"""
        
        # Clear existing data
        self.spark.sql(f"DELETE FROM {self.config.accounts_table}")
        self.spark.sql(f"DELETE FROM {self.config.summary_table}")
        self.spark.sql(f"DELETE FROM {self.config.latest_summary_table}")
        
        # Load accounts
        if scenario.get("input_accounts"):
            accounts_data = []
            for acc in scenario["input_accounts"]:
                accounts_data.append({
                    "cons_acct_key": acc.cons_acct_key,
                    "rpt_as_of_mo": acc.rpt_as_of_mo,
                    "base_ts": acc.base_ts,
                    "balance_am": acc.balance_am,
                    "actual_payment_am": acc.actual_payment_am,
                    "credit_limit_am": acc.credit_limit_am,
                    "past_due_am": acc.past_due_am,
                    "days_past_due": acc.days_past_due,
                    "asset_class_cd": acc.asset_class_cd,
                    "insert_ts": acc.base_ts,
                    "update_ts": acc.base_ts
                })
            
            if accounts_data:
                df = self.spark.createDataFrame(accounts_data)
                df.writeTo(self.config.accounts_table).append()
        
        # Load existing summary
        if scenario.get("existing_summary"):
            summary_data = []
            for summ in scenario["existing_summary"]:
                summary_data.append({
                    "cons_acct_key": summ.cons_acct_key,
                    "rpt_as_of_mo": summ.rpt_as_of_mo,
                    "base_ts": summ.base_ts,
                    "balance_history": summ.balance_history,
                    "payment_history": summ.payment_history,
                    "credit_limit_history": summ.credit_limit_history,
                    "past_due_history": summ.past_due_history,
                    "days_past_due_history": summ.days_past_due_history,
                    "payment_rating_history": summ.payment_rating_history,
                    "asset_class_history": summ.asset_class_history,
                    "payment_history_grid": "?" * 36
                })
            
            if summary_data:
                df = self.spark.createDataFrame(summary_data)
                df.writeTo(self.config.summary_table).append()
                df.writeTo(self.config.latest_summary_table).append()
    
    def run_scenario(self, scenario: Dict[str, Any]) -> Dict[str, Any]:
        """Run a single test scenario"""
        
        name = scenario["name"]
        description = scenario["description"]
        
        print(f"\n{'='*60}")
        print(f"RUNNING: {name}")
        print(f"{'='*60}")
        print(f"Description: {description}")
        
        result = {
            "name": name,
            "status": "UNKNOWN",
            "start_time": datetime.now().isoformat(),
            "errors": []
        }
        
        try:
            # Load test data
            print("\nLoading test data...")
            self.load_test_data(scenario)
            
            # Import and run pipeline
            from summary_pipeline import run_pipeline, PipelineConfig
            
            # Create config matching local tables
            config = PipelineConfig()
            config.accounts_table = self.config.accounts_table
            config.summary_table = self.config.summary_table
            config.latest_summary_table = self.config.latest_summary_table
            config.primary_key = self.config.primary_key
            config.partition_key = self.config.partition_key
            config.timestamp_key = self.config.timestamp_key
            
            # Run pipeline
            print("\nRunning pipeline...")
            stats = run_pipeline(self.spark, config)
            
            result["stats"] = stats
            
            # Validate results
            print("\nValidating results...")
            validation = self.validate_results(scenario, stats)
            result["validation"] = validation
            
            if validation["passed"]:
                result["status"] = "PASSED"
                print(f"\n[PASSED] {name}")
            else:
                result["status"] = "FAILED"
                result["errors"] = validation["errors"]
                print(f"\n[FAILED] {name}")
                for err in validation["errors"]:
                    print(f"  - {err}")
        
        except Exception as e:
            result["status"] = "ERROR"
            result["errors"] = [str(e)]
            print(f"\n[ERROR] {name}: {e}")
        
        result["end_time"] = datetime.now().isoformat()
        self.results.append(result)
        
        return result
    
    def validate_results(self, scenario: Dict[str, Any], stats: Dict[str, Any]) -> Dict[str, Any]:
        """Validate pipeline results against expected output"""
        
        validation = {"passed": True, "errors": [], "checks": []}
        
        expected = scenario.get("expected_output", {})
        
        # Check record counts
        if "total_records" in expected:
            if stats.get("total_records") != expected["total_records"]:
                validation["passed"] = False
                validation["errors"].append(
                    f"Total records: expected {expected['total_records']}, got {stats.get('total_records')}"
                )
        
        if "records_written" in expected:
            if stats.get("records_written") != expected["records_written"]:
                validation["passed"] = False
                validation["errors"].append(
                    f"Records written: expected {expected['records_written']}, got {stats.get('records_written')}"
                )
        
        # Check specific account results
        if "cons_acct_key" in expected:
            account_id = expected["cons_acct_key"]
            
            # Query result from summary
            result_df = self.spark.sql(f"""
                SELECT * FROM {self.config.summary_table}
                WHERE cons_acct_key = {account_id}
                ORDER BY rpt_as_of_mo DESC
                LIMIT 1
            """)
            
            if result_df.count() == 0:
                if not expected.get("should_skip", False):
                    validation["passed"] = False
                    validation["errors"].append(f"Account {account_id} not found in output")
            else:
                row = result_df.first()
                
                # Check array values
                if "balance_history" in expected:
                    actual = list(row["balance_history"])
                    expected_arr = expected["balance_history"]
                    if actual[:len(expected_arr)] != expected_arr[:len(actual)]:
                        validation["passed"] = False
                        validation["errors"].append(
                            f"Balance history mismatch for {account_id}"
                        )
                
                # Check array length
                if "array_length" in expected:
                    if len(row["balance_history"]) != expected["array_length"]:
                        validation["passed"] = False
                        validation["errors"].append(
                            f"Array length: expected {expected['array_length']}, got {len(row['balance_history'])}"
                        )
        
        validation["checks"].append({
            "stats": stats,
            "expected": expected
        })
        
        return validation
    
    def run_all(self):
        """Run all test scenarios"""
        scenarios = get_all_scenarios()
        
        print("=" * 80)
        print(f"RUNNING {len(scenarios)} TEST SCENARIOS")
        print("=" * 80)
        
        for scenario in scenarios:
            self.run_scenario(scenario)
        
        self.print_summary()
    
    def run_selected(self, names: List[str]):
        """Run selected test scenarios by name"""
        print("=" * 80)
        print(f"RUNNING {len(names)} SELECTED SCENARIOS")
        print("=" * 80)
        
        for name in names:
            scenario = get_scenario_by_name(name)
            if scenario:
                self.run_scenario(scenario)
            else:
                print(f"\n[SKIP] Scenario not found: {name}")
        
        self.print_summary()
    
    def print_summary(self):
        """Print test summary"""
        print("\n" + "=" * 80)
        print("TEST SUMMARY")
        print("=" * 80)
        
        passed = sum(1 for r in self.results if r["status"] == "PASSED")
        failed = sum(1 for r in self.results if r["status"] == "FAILED")
        errors = sum(1 for r in self.results if r["status"] == "ERROR")
        
        print(f"\nTotal:  {len(self.results)}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        print(f"Errors: {errors}")
        
        if failed + errors > 0:
            print("\nFailed/Error Tests:")
            for r in self.results:
                if r["status"] in ("FAILED", "ERROR"):
                    print(f"  - {r['name']}: {r['status']}")
                    for err in r.get("errors", []):
                        print(f"      {err}")
        
        print("\n" + "=" * 80)
        
        # Save results to file
        with open("/app/output/test_results.json", "w") as f:
            json.dump(self.results, f, indent=2, default=str)
        
        print("Results saved to /app/output/test_results.json")


def main():
    """Main entry point"""
    runner = TestRunner()
    
    try:
        runner.setup()
        
        if len(sys.argv) > 1:
            # Run specific scenarios
            runner.run_selected(sys.argv[1:])
        else:
            # Run all scenarios
            runner.run_all()
    
    finally:
        runner.teardown()
    
    # Exit with error code if any tests failed
    failed = sum(1 for r in runner.results if r["status"] != "PASSED")
    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
