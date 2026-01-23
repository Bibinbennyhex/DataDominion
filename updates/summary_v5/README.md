# Summary Pipeline v5.0

Production-grade, config-driven pipeline for maintaining account summary tables with 36-month rolling history arrays.

## Quick Start

```bash
# Production mode (default - handles all scenarios automatically)
spark-submit summary_pipeline.py --config summary_config.json

# Initial load
spark-submit summary_pipeline.py --config summary_config.json --mode initial --start-month 2016-01 --end-month 2025-12

# Forward processing
spark-submit summary_pipeline.py --config summary_config.json --mode forward --start-month 2026-01

# Manual backfill
spark-submit summary_pipeline.py --config summary_config.json --mode backfill --backfill-filter "cons_acct_key = 123"
```

## Directory Structure

```
summary_v5/
├── summary_pipeline.py      # Main pipeline (single consolidated file)
├── summary_config.json      # Configuration
├── README.md                # This file
├── docs/                    # Documentation
│   ├── README.md            # Detailed usage guide
│   ├── CODE_EXPLANATION.md  # Component-by-component breakdown
│   ├── EXECUTION_FLOW.md    # Step-by-step execution trace
│   ├── PRODUCTION_INCREMENTAL_MODE.md
│   └── INCREMENTAL_MODE_QUICK_REFERENCE.md
└── tests/                   # Test utilities
    ├── setup_tables.py      # Create/reset tables
    ├── generate_test_data.py # Generate test data
    ├── run_test.py          # Run test scenarios
    ├── run_validation.py    # Validate results
    └── verify_backfill.py   # Verify backfill processing
```

## Key Features

- **Single File**: All pipeline code in `summary_pipeline.py` (no separate modules)
- **Four Modes**: incremental (default), initial, forward, backfill
- **Auto-Classification**: Incremental mode automatically detects and handles:
  - Case I: New accounts
  - Case II: Forward entries
  - Case III: Backfill corrections
- **Correct Processing Order**: Backfill → New Accounts → Forward

## Documentation

See `docs/` folder for detailed documentation:
- `README.md` - Complete usage guide
- `CODE_EXPLANATION.md` - Technical deep-dive
- `EXECUTION_FLOW.md` - Execution trace with actual code paths

## Testing

```bash
# Setup tables
spark-submit tests/setup_tables.py --config summary_config.json --drop-existing

# Generate test data
spark-submit tests/generate_test_data.py --config summary_config.json --accounts 100 --months 12

# Run pipeline
spark-submit summary_pipeline.py --config summary_config.json

# Validate
spark-submit tests/run_validation.py --config summary_config.json
```
