# Summary Pipeline v9

## Overview

Version 9 of the DPD Summary Pipeline - the fastest and most optimized implementation.

**Key Features:**
- SQL-based array updates (faster than Python UDFs)
- Broadcast optimization for small tables
- Only 3 table scans (vs 4 in other versions)
- 799 lines of code (smallest codebase)
- Handles all 3 cases: New Accounts, Forward, Backfill

## Performance

| Metric | v9 | v8 | v5 | v6 |
|--------|---:|---:|---:|---:|
| Time (200M backfill) | 3.17h | 3.67h | 3.83h | 3.88h |
| Table Scans | 3 | 4 | 4 | 4 |
| Lines of Code | 799 | 1,272 | 1,511 | 1,500 |

## Directory Structure

```
summary_v9/
├── base/                    # Production code
│   ├── summary_pipeline.py  # Main pipeline
│   └── pipeline_config.json # Configuration
├── test/                    # Test scenarios
│   ├── test_scenarios.py    # All test cases
│   └── generate_test_data.py
└── local/                   # Local Docker testing
    ├── base/
    │   ├── Dockerfile
    │   └── docker-compose.yml
    └── test/
        └── run_tests.py
```

## Usage

### Production
```bash
spark-submit base/summary_pipeline.py --config base/pipeline_config.json
```

### Local Testing
```bash
cd local/base
docker-compose up -d
docker-compose exec spark python /app/test/run_tests.py
```

## Processing Order

Critical for correctness:
1. **Backfill (Case III)** - FIRST - Rebuilds history with corrections
2. **New Accounts (Case I)** - SECOND - No dependencies
3. **Forward (Case II)** - LAST - Uses corrected history

## Configuration

Edit `base/pipeline_config.json`:

```json
{
  "tables": {
    "source": "your_accounts_table",
    "summary": "your_summary_table",
    "latest_summary": "your_latest_summary_table"
  },
  "keys": {
    "primary": "cons_acct_key",
    "partition": "rpt_as_of_mo",
    "timestamp": "base_ts"
  }
}
```

## Test Scenarios

17 test scenarios covering:
- Case I (3 scenarios)
- Case II (3 scenarios)  
- Case III (5 scenarios)
- Mixed (2 scenarios)
- Edge cases (4 scenarios)

Run specific tests:
```bash
docker-compose exec spark python /app/test/run_tests.py case_i_simple_new_account
```
