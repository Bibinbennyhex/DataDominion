# Summary Pipeline v6.0

A **superior hybrid pipeline** combining the best features from both original codebases for maintaining account summary tables with 36-month rolling history arrays at massive scale (500B+ rows).

## Key Features

| Feature | Source | Description |
|---------|--------|-------------|
| **Config-Driven** | summary_v5 | All columns, transformations in JSON |
| **Smart Classification** | summary_v5 | Auto-detect Case I/II/III |
| **Dynamic Partitioning** | summary_v5 | Power-of-2 optimal partitions |
| **Retry with Backoff** | opencode-test | Exponential backoff on failures |
| **Checkpoint/Resume** | opencode-test | Resume from last successful point |
| **Full Audit Trail** | opencode-test | Dedicated audit Iceberg table |
| **Correct Processing Order** | Both | Backfill → New → Forward |
| **Latest Summary Table** | summary_v5 | Fast lookups without full scan |
| **Table Optimization** | Both | Compaction + snapshot expiry |

## Quick Start

```bash
# Production mode (auto-handles all scenarios)
spark-submit run_pipeline.py --config config/pipeline_config.json

# Resume from checkpoint after failure
spark-submit run_pipeline.py --config config/pipeline_config.json --resume abc12345

# Show table statistics
spark-submit run_pipeline.py --config config/pipeline_config.json --stats

# Force table optimization
spark-submit run_pipeline.py --config config/pipeline_config.json --optimize
```

## Project Structure

```
summary_v6/
├── config/
│   └── pipeline_config.json     # Unified configuration
├── core/
│   ├── __init__.py
│   ├── config.py                # Config loader with validation
│   ├── session.py               # Spark session factory
│   └── types.py                 # Type definitions (enums, dataclasses)
├── processors/
│   ├── __init__.py
│   ├── classifier.py            # Case I/II/III classification
│   ├── array_builder.py         # Rolling column transformations
│   ├── case_i_processor.py      # New accounts
│   ├── case_ii_processor.py     # Forward entries
│   └── case_iii_processor.py    # Backfill with history rebuild
├── orchestration/
│   ├── __init__.py
│   ├── pipeline.py              # Main orchestrator
│   ├── batch_manager.py         # Retry logic
│   └── checkpoint.py            # Resume capability
├── monitoring/
│   ├── __init__.py
│   └── audit.py                 # Audit trail
├── utils/
│   ├── __init__.py
│   ├── partitioning.py          # Dynamic partition calculator
│   └── optimization.py          # Table optimization
├── run_pipeline.py              # CLI entry point
└── README.md
```

## Processing Modes

### Incremental (Default)

Auto-detects and handles all scenarios:
- **Case I**: New accounts → Create initial arrays
- **Case II**: Forward entries → Shift existing arrays
- **Case III**: Backfill → Rebuild entire history

**Critical**: Processing order is Backfill → New → Forward to ensure data consistency.

### Initial

First-time load from scratch:
```bash
spark-submit run_pipeline.py --config config/pipeline_config.json --mode initial
```

## Configuration

Edit `config/pipeline_config.json`:

```json
{
    "tables": {
        "source": "default.accounts_all",
        "summary": "default.summary",
        "latest_summary": "default.latest_summary"
    },
    "rolling_columns": [
        {
            "name": "balance",
            "source_column": "balance_am",
            "mapper_expr": "COALESCE(balance_am, 0)",
            "data_type": "Integer",
            "history_length": 36
        }
    ],
    "resilience": {
        "retry_count": 3,
        "checkpoint_enabled": true
    }
}
```

## Performance at 1B Scale

| Metric | Estimate |
|--------|----------|
| Classification | 20-30 minutes |
| Case I (100M new) | 30-45 minutes |
| Case II (700M forward) | 2-3 hours |
| Case III (200M backfill) | 1-2 hours |
| **Total** | **4-6 hours** |

*With: 100 executors, 8 cores each, 32GB memory per executor*

## Comparison to Original Codebases

| Feature | opencode-test | summary_v5 | **v6.0 Hybrid** |
|---------|--------------|------------|-----------------|
| Retry logic | ✅ | ❌ | ✅ |
| Checkpoint | ✅ | ❌ | ✅ |
| Audit trail | ✅ | ❌ | ✅ |
| Config-driven | ❌ | ✅ | ✅ |
| Case classification | ❌ | ✅ | ✅ |
| Gap handling | ❌ | ✅ | ✅ |
| Dynamic partitioning | ❌ | ✅ | ✅ |
| Latest summary table | ❌ | ✅ | ✅ |
| Modular design | ✅ | ❌ | ✅ |

## Requirements

- Apache Spark 3.3+
- Apache Iceberg 1.0+
- Python 3.8+
- python-dateutil

```bash
pip install pyspark python-dateutil
```
