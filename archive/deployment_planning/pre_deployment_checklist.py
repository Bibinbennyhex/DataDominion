"""
Pre-Deployment Checklist for v8
================================
Validates v8 is ready for testing/deployment
"""

import os
import ast
import re

def check_v8_exists():
    """Verify v8 files exist"""
    v8_dir = "updates/summary_v8"
    pipeline_file = os.path.join(v8_dir, "summary_pipeline.py")
    config_file = os.path.join(v8_dir, "pipeline_config.json")
    
    results = []
    results.append(("v8 directory exists", os.path.exists(v8_dir)))
    results.append(("summary_pipeline.py exists", os.path.exists(pipeline_file)))
    results.append(("pipeline_config.json exists", os.path.exists(config_file)))
    
    return results, pipeline_file

def check_v8_code_quality(filepath):
    """Check v8 code for issues"""
    results = []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        code = f.read()
    
    # Check for classes (should be zero)
    class_count = len(re.findall(r'\nclass\s+\w+', code))
    results.append((f"No classes (found {class_count})", class_count == 0))
    
    # Check line count
    lines = code.split('\n')
    line_count = len([l for l in lines if l.strip() and not l.strip().startswith('#')])
    results.append((f"Single file (~850 lines, actual: {line_count})", 700 <= line_count <= 1000))
    
    # Check for main processing functions
    has_classify = "def classify_records" in code
    has_case1 = "def process_case_i" in code
    has_case2 = "def process_case_ii" in code
    has_case3 = "def process_case_iii" in code
    has_main = "def main" in code
    
    results.append(("Has classify_records function", has_classify))
    results.append(("Has process_case_i function", has_case1))
    results.append(("Has process_case_ii function", has_case2))
    results.append(("Has process_case_iii function", has_case3))
    results.append(("Has main function", has_main))
    
    # Check for Spark imports
    has_spark = "from pyspark.sql import" in code
    results.append(("Has PySpark imports", has_spark))
    
    # Check syntax
    try:
        ast.parse(code)
        results.append(("Valid Python syntax", True))
    except SyntaxError as e:
        results.append((f"Valid Python syntax (ERROR: {e})", False))
    
    return results

def check_production_bug():
    """Check if production bug still exists"""
    prod_file = "scripts/summary.py"
    results = []
    
    if os.path.exists(prod_file):
        with open(prod_file, 'r', encoding='utf-8') as f:
            code = f.read()
        
        has_bug = "concate_ws" in code  # typo version
        results.append((f"Production has typo bug at line 351", has_bug))
        results.append(("** ACTION REQUIRED: Fix typo before v8 testing **", not has_bug))
    else:
        results.append(("Production script found", False))
    
    return results

def generate_test_plan():
    """Generate testing checklist"""
    print("\n" + "="*100)
    print("TESTING CHECKLIST FOR v8")
    print("="*100)
    print()
    
    tests = [
        ("DEV Environment Setup", [
            "Clone production Iceberg tables to dev",
            "Configure v8 to point to dev tables",
            "Verify Spark cluster resources (same as prod)",
        ]),
        ("Test Case 1: Forward Processing (50M records)", [
            "Load historical data with max_month = 2024-11",
            "Load input data for month = 2024-12",
            "Run v8 pipeline",
            "Verify 36-month arrays shifted correctly",
            "Compare output with production script output",
            "Expected time: ~1.2 hours",
        ]),
        ("Test Case 2: Backfill Processing (200M records)", [
            "Load historical data with max_month = 2024-12",
            "Load input data for month = 2024-01 (11 months old)",
            "Run v8 pipeline",
            "Verify arrays rebuilt with backfill",
            "Verify payment_history_grid has correct month",
            "Expected time: ~3.6 hours",
        ]),
        ("Test Case 3: New Accounts (10M records)", [
            "Load input data with accounts NOT in summary table",
            "Run v8 pipeline",
            "Verify new arrays created (all zeros + new month)",
            "Verify correct classification as Case I",
        ]),
        ("Test Case 4: Mixed Batch (100M total)", [
            "Load 50M forward + 40M backfill + 10M new",
            "Run v8 pipeline (single run)",
            "Verify all 3 cases processed correctly",
            "Expected time: ~2.0 hours",
        ]),
        ("Validation Checks", [
            "No duplicate account_ids in output",
            "Array lengths = 36 for all records",
            "payment_history_grid format correct",
            "No null values in critical columns",
            "Row count matches input",
        ]),
        ("Performance Monitoring", [
            "Log Spark UI metrics (stages, tasks, shuffles)",
            "Count number of 60B table scans (should be 4 for backfill)",
            "Monitor memory usage",
            "Verify no spill to disk",
        ]),
        ("Parallel Run (1 week)", [
            "Run v8 alongside production",
            "Compare outputs daily",
            "Log any discrepancies",
            "Monitor failure rates",
        ]),
    ]
    
    for i, (category, items) in enumerate(tests, 1):
        print(f"{i}. {category}")
        print("-" * 100)
        for item in items:
            print(f"   [ ] {item}")
        print()

def main():
    print("="*100)
    print("PRE-DEPLOYMENT VALIDATION FOR v8")
    print("="*100)
    print()
    
    # Check v8 exists
    print("1. FILE EXISTENCE")
    print("-" * 100)
    file_results, pipeline_path = check_v8_exists()
    for test, passed in file_results:
        status = "[PASS]" if passed else "[FAIL]"
        print(f"{status} {test}")
    print()
    
    # Check v8 code quality
    if os.path.exists(pipeline_path):
        print("2. CODE QUALITY")
        print("-" * 100)
        code_results = check_v8_code_quality(pipeline_path)
        for test, passed in code_results:
            status = "[PASS]" if passed else "[FAIL]"
            print(f"{status} {test}")
        print()
    
    # Check production bug
    print("3. PRODUCTION SCRIPT STATUS")
    print("-" * 100)
    bug_results = check_production_bug()
    for test, passed in bug_results:
        status = "[PASS]" if passed else "[WARN]"
        print(f"{status} {test}")
    print()
    
    # Generate test plan
    generate_test_plan()
    
    # Summary
    print("="*100)
    print("NEXT STEPS")
    print("="*100)
    print()
    print("1. Fix production script typo (scripts/summary.py line 351)")
    print("   Change: F.concate_ws -> F.concat_ws")
    print()
    print("2. Setup dev environment for v8 testing")
    print("   - Clone Iceberg tables")
    print("   - Configure pipeline_config.json")
    print()
    print("3. Run test cases (see TESTING CHECKLIST above)")
    print()
    print("4. After successful testing, proceed with deployment plan:")
    print("   - Week 1: Dev testing")
    print("   - Week 2: Parallel production run")
    print("   - Week 3: Full cutover")
    print()
    print("="*100)

if __name__ == "__main__":
    main()
