# Changelog - Summary Pipeline

## v9.1-fixed (January 2026)

### Bug Fix
- **CRITICAL**: Fixed Case III (backfill) logic that was not creating summary rows for backfill months
- Original v9 only updated future summaries when backfill data arrived
- Now correctly creates a new summary row for the backfill month itself, inheriting historical data from the closest prior summary

### New Features
- Full column mapping support (36 source → destination mappings)
- Column transformations (13 sentinel value handlers)
- Inferred columns (orig_loan_am, payment_rating_cd)
- Date column validation (invalid years → NULL)
- Separate column lists for summary vs latest_summary tables
- Configurable Spark settings from JSON
- Dynamic allocation support

### Performance
- ~1.85x faster than v8 for backfill scenarios
- Reduced memory usage (no COLLECT_LIST)
- Configurable broadcast threshold

---

## v9.0 (December 2025)

### Features
- SQL-based array updates using `transform()`
- Broadcast optimization for small tables
- MEMORY_AND_DISK caching
- Single-pass array updates
- PipelineConfig class

### Bug
- **Case III backfill only updates future summaries, does NOT create backfill month row**

---

## v8.0 (November 2025)

### Features
- Single-file, function-only implementation
- 7-CTE SQL approach for backfill
- Dynamic allocation support
- Iceberg table optimization (rewrite_data_files, expire_snapshots)
- Bucketing support

### Performance
- ~220 min for mixed workload (50B table)
- High memory usage for COLLECT_LIST

---

## v5.0 (October 2025)

### Features
- Class-based architecture (RollingColumnBuilder, etc.)
- Column mappers and transformations
- Incremental and initial modes

---

## v4.0 (September 2025)

### Features
- Initial production implementation
- 36-month rolling history arrays
- Case I/II/III classification
- MERGE to Iceberg tables

---

## Migration Guide: v9.0 → v9.1-fixed

1. **Replace pipeline file**: `summary_pipeline.py`
2. **Update config**: Add new sections (see below)
3. **Test backfill**: Run `tests/run_backfill_test.py`

### Config Changes Required

Add these new sections to your config:

```json
{
  "columns": { /* 36 column mappings */ },
  "column_transformations": [ /* 13 transforms */ ],
  "inferred_columns": [ /* 2 derived columns */ ],
  "date_columns": [ /* 7 date columns */ ],
  "summary_columns": [ /* 47 output columns */ ],
  "latest_summary_columns": [ /* 34 output columns */ ]
}
```

See `config/pipeline_config.json` for complete example.
