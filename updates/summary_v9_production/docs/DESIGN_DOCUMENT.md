# Summary Pipeline v9.4 - Technical Design Document

**Document Version**: 1.1  
**Pipeline Version**: 9.4  
**Last Updated**: January 2026  
**Author**: DataDominion Team

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Overview](#2-system-overview)
3. [Architecture Design](#3-architecture-design)
4. [Temp Table Processing (v9.4)](#4-temp-table-processing-v94)
5. [Case Classification Logic](#5-case-classification-logic)
6. [Case I: New Accounts](#6-case-i-new-accounts)
7. [Case II: Forward Entries](#7-case-ii-forward-entries)
8. [Case III: Backfill](#8-case-iii-backfill)
9. [Case IV: Bulk Historical Load](#9-case-iv-bulk-historical-load)
10. [Data Flow Diagrams](#10-data-flow-diagrams)
11. [Performance Considerations](#11-performance-considerations)
12. [Assumptions and Constraints](#12-assumptions-and-constraints)
13. [Edge Cases and Limitations](#13-edge-cases-and-limitations)
14. [Configuration Reference](#14-configuration-reference)
15. [Appendix](#15-appendix)

---

## 1. Executive Summary

### 1.1 Purpose

The Summary Pipeline maintains **36-month rolling history arrays** for consumer credit accounts. It processes incoming account data and maintains a denormalized summary table with pre-computed arrays for efficient downstream queries.

### 1.2 Scale

| Metric | Value |
|--------|-------|
| Total Records | 500+ Billion |
| Accounts | ~2 Billion unique |
| Monthly Partitions | 36 per account |
| Rolling Columns | 7 arrays per record |
| Monthly Throughput | ~50-100 Million records |

### 1.3 Why Case-wise Processing?

The pipeline uses **case-wise separation** for optimal performance at scale:

| Case | Frequency | I/O Pattern | Why Separate? |
|------|-----------|-------------|---------------|
| Case I | ~5% | Write only | No joins needed |
| Case II | ~90% | Join latest row only | Minimal I/O |
| Case III | ~4% | Read full history | Expensive but rare |
| Case IV | ~1% | Window on new data | Bulk optimization |

**A unified approach would require loading existing summary data for ALL records**, resulting in:
- Terabytes of unnecessary I/O for Case I/II
- Massive shuffle operations
- 10-100x slower processing

---

## 2. System Overview

### 2.1 Technology Stack

| Component | Technology |
|-----------|------------|
| Processing Engine | Apache Spark 3.5+ |
| Table Format | Apache Iceberg |
| Storage | AWS S3 |
| Compute | AWS EMR |
| Catalog | AWS Glue / REST Catalog |

### 2.2 Tables

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA FLOW                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌─────────────┐         ┌─────────────┐                       │
│   │   ACCOUNTS  │ ──────► │   SUMMARY   │                       │
│   │   (Source)  │         │   (Target)  │                       │
│   └─────────────┘         └─────────────┘                       │
│         │                       │                                │
│         │                       ▼                                │
│         │               ┌───────────────┐                        │
│         └─────────────► │LATEST_SUMMARY │                        │
│                         │  (Metadata)   │                        │
│                         └───────────────┘                        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

| Table | Purpose | Partitioning |
|-------|---------|--------------|
| `accounts` | Raw monthly account snapshots | `rpt_as_of_mo` |
| `summary` | Denormalized with rolling arrays | `rpt_as_of_mo` |
| `latest_summary` | Latest row per account (metadata) | None or `bucket(cons_acct_key)` |

### 2.3 Rolling History Arrays

Each summary record contains arrays of length 36:

```
balance_am_history = [current, prev_1, prev_2, ..., prev_35]
                      ▲
                      │
                    Position 0 = Current month's value
                    Position 1 = Previous month's value
                    Position 35 = 35 months ago
```

**Rolling Columns (7 total):**
1. `actual_payment_am_history`
2. `balance_am_history`
3. `credit_limit_am_history`
4. `past_due_am_history`
5. `payment_rating_cd_history`
6. `days_past_due_history`
7. `asset_class_cd_4in_history`

**Grid Columns (derived):**
1. `payment_history_grid` - String representation of payment_rating_cd array

---

## 3. Architecture Design

### 3.1 Design Principles

1. **Minimize I/O**: Only read data that's absolutely necessary
2. **Case Optimization**: Each case has optimized processing path
3. **Partition Pruning**: Leverage Iceberg for efficient data access
4. **Single MERGE**: Combine all case outputs into one write operation

### 3.2 Processing Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           PIPELINE EXECUTION FLOW                             │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌─────────────┐                                                              │
│  │   ACCOUNTS  │                                                              │
│  │   (Input)   │                                                              │
│  └──────┬──────┘                                                              │
│         │                                                                     │
│         ▼                                                                     │
│  ┌──────────────┐    ┌───────────────┐                                        │
│  │   PREPARE    │◄───│ LATEST_SUMMARY│                                        │
│  │   SOURCE     │    │  (Metadata)   │                                        │
│  └──────┬───────┘    └───────────────┘                                        │
│         │                                                                     │
│         ▼                                                                     │
│  ┌──────────────────────────────────────────────────────────────────┐        │
│  │                    CLASSIFY ACCOUNTS                              │        │
│  │  ┌─────────┐ ┌──────────┐ ┌───────────┐ ┌────────────────┐       │        │
│  │  │ CASE I  │ │ CASE II  │ │ CASE III  │ │    CASE IV     │       │        │
│  │  │  (New)  │ │(Forward) │ │(Backfill) │ │(Bulk Historic) │       │        │
│  │  └────┬────┘ └────┬─────┘ └─────┬─────┘ └───────┬────────┘       │        │
│  └───────┼───────────┼─────────────┼───────────────┼────────────────┘        │
│          │           │             │               │                          │
│          ▼           ▼             ▼               ▼                          │
│  ┌──────────────────────────────────────────────────────────────────┐        │
│  │                    PROCESS EACH CASE                              │        │
│  │                                                                   │        │
│  │  Case I:  Create initial arrays                                   │        │
│  │  Case II: Shift arrays (join latest only)                         │        │
│  │  Case III: Rebuild arrays (read full history)                     │        │
│  │  Case IV: Window-based array building                             │        │
│  │                                                                   │        │
│  └───────────────────────────┬──────────────────────────────────────┘        │
│                              │                                                │
│                              ▼                                                │
│                    ┌──────────────────┐                                       │
│                    │  UNION ALL CASES │                                       │
│                    └────────┬─────────┘                                       │
│                             │                                                 │
│                             ▼                                                 │
│                    ┌──────────────────┐                                       │
│                    │   SINGLE MERGE   │                                       │
│                    │   TO SUMMARY     │                                       │
│                    └────────┬─────────┘                                       │
│                             │                                                 │
│                             ▼                                                 │
│                    ┌──────────────────┐                                       │
│                    │ UPDATE LATEST    │                                       │
│                    │ SUMMARY METADATA │                                       │
│                    └──────────────────┘                                       │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 Why This Design Works at Scale

| Operation | Unified Approach | Case-wise Approach |
|-----------|------------------|-------------------|
| New Account (Case I) | Load summary (0 rows found) → wasteful scan | Direct insert, no read |
| Forward Entry (Case II) | Load full summary → massive I/O | Join only `latest_summary` (1 row) |
| Backfill (Case III) | Same | Must read history (unavoidable) |
| Bulk Load (Case IV) | Load summary for new accounts (0 rows) | Window on new data only |

**At 500B records scale:**
- `summary` table: ~100TB+
- `latest_summary` table: ~2TB (1 row per account)
- Reading `latest_summary` vs `summary`: **50x less I/O**

---

## 4. Temp Table Processing (v9.4)

### 4.1 Problem: Spark DAG Lineage

In v9.3 and earlier, all case results were held in memory and unioned together:

```python
# v9.3 approach (in-memory union)
case_i_df = process_case_i(...)      # DAG lineage held
case_ii_df = process_case_ii(...)    # DAG lineage held
case_iii_df = process_case_iii(...)  # DAG lineage held
case_iv_df = process_case_iv(...)    # DAG lineage held

# Union all - Spark must track all lineage paths
all_results = case_i_df.unionByName(case_ii_df).unionByName(...)

# Single write - only NOW does Spark materialize
write_results(spark, all_results, config)
```

**Problem**: For large batches (1B+ records), the driver must hold the entire DAG lineage in memory until the final write. This causes:
- Driver OOM errors
- Long GC pauses
- Executor retries

### 4.2 Solution: Temp Table Materialization

v9.4 writes each case's results to a temporary Iceberg table, breaking the DAG lineage:

```python
# v9.4 approach (temp table materialization)
temp_table = generate_temp_table_name()  # demo.temp.pipeline_batch_{ts}_{uuid}
create_temp_table(spark, temp_table, config)

try:
    # Each write materializes data and breaks lineage
    write_to_temp_table(spark, case_i_df, temp_table, "CASE_I")    # Lineage cleared
    write_to_temp_table(spark, case_ii_df, temp_table, "CASE_II")  # Lineage cleared
    write_to_temp_table(spark, case_iii_df, temp_table, "CASE_III")
    write_to_temp_table(spark, case_iv_df, temp_table, "CASE_IV")
    
    # Read back (fresh lineage) and write final results
    final_df = spark.read.table(temp_table)
    write_results(spark, final_df, config)
finally:
    drop_temp_table(spark, temp_table)
```

### 4.3 Temp Table Schema

The temp table includes a `_case_type` column for tracking:

```sql
CREATE TABLE {temp_table} (
    cons_acct_key BIGINT,
    rpt_as_of_mo DATE,
    base_ts TIMESTAMP,
    balance_am_history ARRAY<INT>,
    -- ... all other columns ...
    _case_type STRING  -- Added: tracks CASE_I, CASE_II, CASE_III, CASE_IV
) USING iceberg
```

### 4.4 Temp Table Naming

**Format**: `{catalog}.{temp_schema}.pipeline_batch_{timestamp}_{uuid}`

```python
def generate_temp_table_name(config):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    return f"{config['catalog']}.{config['temp_table_schema']}.pipeline_batch_{timestamp}_{unique_id}"
    
# Example: demo.temp.pipeline_batch_20260131_143052_a1b2c3d4
```

### 4.5 Automatic Cleanup

The `try/finally` pattern ensures cleanup on both success and failure:

```python
temp_table = generate_temp_table_name(config)
create_temp_table(spark, temp_table, config)

try:
    # Process all cases...
    write_final_results(...)
finally:
    drop_temp_table(spark, temp_table)  # Always executes
```

### 4.6 Orphan Cleanup

For crashed/killed runs where `finally` didn't execute:

```python
def cleanup_orphaned_temp_tables(spark, config, max_age_hours=24):
    """
    Find and drop temp tables older than max_age_hours.
    
    Uses table name timestamp to determine age:
    - Parse timestamp from: pipeline_batch_{YYYYMMDD}_{HHMMSS}_{uuid}
    - Drop if age > max_age_hours
    """
    temp_schema = config['temp_table_schema']
    tables = spark.sql(f"SHOW TABLES IN {temp_schema}").collect()
    
    for table in tables:
        if table.tableName.startswith("pipeline_batch_"):
            age = parse_age_from_name(table.tableName)
            if age > max_age_hours:
                spark.sql(f"DROP TABLE IF EXISTS {temp_schema}.{table.tableName}")
```

### 4.7 CLI Usage

```bash
# Run with temp table (default, recommended)
python summary_pipeline.py --config config.json

# Run with legacy in-memory union
python summary_pipeline.py --config config.json --legacy

# Cleanup orphaned temp tables
python summary_pipeline.py --config config.json --cleanup-orphans --max-age-hours 24
```

### 4.8 Performance Impact

| Metric | v9.3 (In-Memory) | v9.4 (Temp Table) |
|--------|------------------|-------------------|
| Driver Memory | High (holds DAG) | Low (lineage broken) |
| I/O | Single write | Multiple writes + read |
| Total Time | Baseline | +5-10% (I/O overhead) |
| Reliability | OOM risk for large batches | Stable |

**Trade-off**: Slightly more I/O in exchange for memory stability and reliability.

### 4.9 Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        v9.4 TEMP TABLE ARCHITECTURE                           │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌─────────────┐                                                              │
│  │   ACCOUNTS  │                                                              │
│  └──────┬──────┘                                                              │
│         │                                                                     │
│         ▼                                                                     │
│  ┌──────────────────────────────────────────────────────────────────┐        │
│  │                    CLASSIFY AND PROCESS                           │        │
│  └──────────────────────────────────────────────────────────────────┘        │
│         │                                                                     │
│         ├─────────────────────────────────────────────────────────────┐       │
│         │                                                             │       │
│         ▼                                                             ▼       │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    │
│  │   CASE I    │    │   CASE II   │    │  CASE III   │    │   CASE IV   │    │
│  │  (process)  │    │  (process)  │    │  (process)  │    │  (process)  │    │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘    └──────┬──────┘    │
│         │                  │                  │                  │            │
│         │    WRITE TO      │                  │                  │            │
│         │    TEMP TABLE    │                  │                  │            │
│         │   (lineage       │                  │                  │            │
│         │    broken)       │                  │                  │            │
│         ▼                  ▼                  ▼                  ▼            │
│  ┌───────────────────────────────────────────────────────────────────┐       │
│  │               TEMP ICEBERG TABLE                                   │       │
│  │         demo.temp.pipeline_batch_20260131_143052_a1b2c3d4          │       │
│  │                                                                    │       │
│  │  ┌─────────────────────────────────────────────────────────────┐  │       │
│  │  │ cons_acct_key | rpt_as_of_mo | balance_am_history | _case   │  │       │
│  │  │ 9001          | 2026-01      | [5000, NULL, ...]  | CASE_I  │  │       │
│  │  │ 2001          | 2026-01      | [5000, 4500, ...]  | CASE_II │  │       │
│  │  │ ...           | ...          | ...                | ...     │  │       │
│  │  └─────────────────────────────────────────────────────────────┘  │       │
│  └─────────────────────────────┬─────────────────────────────────────┘       │
│                                │                                              │
│                                │ READ BACK (fresh lineage)                    │
│                                ▼                                              │
│                    ┌──────────────────┐                                       │
│                    │   DEDUPLICATE    │                                       │
│                    │   BY PRIORITY    │                                       │
│                    └────────┬─────────┘                                       │
│                             │                                                 │
│                             ▼                                                 │
│                    ┌──────────────────┐                                       │
│                    │   MERGE INTO     │                                       │
│                    │     SUMMARY      │                                       │
│                    └────────┬─────────┘                                       │
│                             │                                                 │
│                             ▼                                                 │
│                    ┌──────────────────┐                                       │
│                    │   DROP TEMP      │                                       │
│                    │     TABLE        │                                       │
│                    └──────────────────┘                                       │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Case Classification Logic

### 5.1 Classification Algorithm

```python
def classify_account(account_record, latest_summary_metadata):
    """
    Classification decision tree:
    
    1. Is account in latest_summary?
       NO  → Is this a single month?
             YES → CASE I (New Account)
             NO  → CASE IV (Bulk Historical)
       YES → Is new_month > latest_month?
             YES → CASE II (Forward Entry)
             NO  → CASE III (Backfill)
    """
```

### 5.2 Classification SQL

```sql
WITH classified AS (
    SELECT 
        a.*,
        -- Convert month to integer for comparison
        (YEAR(a.rpt_as_of_mo) * 12 + MONTH(a.rpt_as_of_mo)) as month_int,
        
        -- Get existing metadata
        l.latest_month_int,
        l.latest_base_ts,
        
        CASE
            -- Not in latest_summary
            WHEN l.cons_acct_key IS NULL THEN
                CASE
                    -- Single month for this account in batch
                    WHEN COUNT(*) OVER (PARTITION BY a.cons_acct_key) = 1 
                    THEN 'CASE_I'
                    -- Multiple months for new account
                    ELSE 'CASE_IV'
                END
            -- In latest_summary, check if forward or backfill
            WHEN month_int > l.latest_month_int THEN 'CASE_II'
            ELSE 'CASE_III'
        END as case_type
        
    FROM accounts a
    LEFT JOIN latest_summary l ON a.cons_acct_key = l.cons_acct_key
    WHERE a.base_ts > (SELECT MAX(processed_ts) FROM pipeline_state)
)
```

### 5.3 Classification Matrix

| Account Exists? | Single Month? | Month vs Latest | Classification |
|-----------------|---------------|-----------------|----------------|
| NO | YES | N/A | CASE I |
| NO | NO | N/A | CASE IV |
| YES | * | new > latest | CASE II |
| YES | * | new <= latest | CASE III |

### 5.4 MONTH_DIFF Calculation

For Case II and III, we calculate the month difference:

```sql
MONTH_DIFF = month_int - latest_month_int

-- Examples:
-- 2026-01 vs 2025-12: MONTH_DIFF = 1 (consecutive)
-- 2026-03 vs 2025-12: MONTH_DIFF = 3 (gap of 2 months)
-- 2025-10 vs 2025-12: MONTH_DIFF = -2 (backfill)
```

---

## 6. Case I: New Accounts

### 6.1 Definition

**Case I** applies when:
- Account does NOT exist in `latest_summary`
- Only ONE month of data arrives for this account in the current batch

### 6.2 Processing Logic

```
Input:  Account 9001, Month 2026-01, balance = 5000
Output: balance_am_history = [5000, NULL, NULL, ..., NULL]
                              ▲     ▲
                              │     └── Positions 1-35: NULL (no prior data)
                              └── Position 0: Current value
```

### 6.3 Implementation

```python
def process_case_i(case_i_df, config):
    """
    Create initial arrays for new accounts.
    
    No joins required - just create arrays with:
    - Position 0: Current month's value
    - Positions 1-35: NULL
    """
    for rolling_col in config['rolling_columns']:
        # Create array: [value, NULL, NULL, ..., NULL]
        result = result.withColumn(
            f"{rolling_col['name']}_history",
            F.concat(
                F.array(F.col(rolling_col['mapper_column'])),
                F.array_repeat(F.lit(None), 35)
            )
        )
    
    return result
```

### 6.4 Performance Characteristics

| Metric | Value |
|--------|-------|
| I/O Read | None |
| I/O Write | 1 row to summary, 1 row to latest_summary |
| Shuffle | None |
| Complexity | O(1) per record |

### 6.5 Example

**Input (accounts table):**
```
+-------------+------------+----------+-----------+
|cons_acct_key|rpt_as_of_mo|balance_am|base_ts    |
+-------------+------------+----------+-----------+
|9001         |2026-01     |5000      |2026-01-15 |
+-------------+------------+----------+-----------+
```

**Output (summary table):**
```
+-------------+------------+--------------------------------+
|cons_acct_key|rpt_as_of_mo|balance_am_history              |
+-------------+------------+--------------------------------+
|9001         |2026-01     |[5000, NULL, NULL, ..., NULL]   |
+-------------+------------+--------------------------------+
```

---

## 7. Case II: Forward Entries

### 7.1 Definition

**Case II** applies when:
- Account EXISTS in `latest_summary`
- New month > Latest month in summary

### 7.2 Processing Logic

```
Existing:  Account 2001, Latest = 2025-12
           balance_am_history = [4500, 4000, 3500, ...]
           
New Data:  Account 2001, Month = 2026-01, balance = 5000

Result:    balance_am_history = [5000, 4500, 4000, 3500, ...]
                                 ▲     ▲
                                 │     └── Previous array shifted right
                                 └── New value at position 0
```

### 7.3 Gap Handling

When `MONTH_DIFF > 1`, insert NULLs for missing months:

```
Existing:  Latest = 2025-12, balance_am_history = [4500, 4000, ...]
New Data:  Month = 2026-03, balance = 5500 (MONTH_DIFF = 3)

Result:    balance_am_history = [5500, NULL, NULL, 4500, 4000, ...]
                                 ▲     ▲     ▲     ▲
                                 │     │     │     └── Original position 0
                                 │     │     └── Gap: Feb not reported
                                 │     └── Gap: Jan not reported
                                 └── New value (Mar 2026)
```

### 7.4 Implementation

```python
def process_case_ii(case_ii_df, config):
    """
    Shift arrays for forward entries.
    
    Join with latest_summary (1 row per account) to get existing arrays.
    """
    
    # Load ONLY latest summary row for affected accounts
    latest_for_affected = spark.sql(f"""
        SELECT {pk}, {history_columns}
        FROM latest_summary
        WHERE {pk} IN (SELECT {pk} FROM case_ii_records)
    """)
    
    # Build shift expression
    shift_expr = """
        CASE
            WHEN MONTH_DIFF = 1 THEN
                slice(concat(array(new_value), prev_array), 1, 36)
            WHEN MONTH_DIFF > 1 THEN
                slice(
                    concat(
                        array(new_value),
                        array_repeat(NULL, MONTH_DIFF - 1),
                        prev_array
                    ),
                    1, 36
                )
        END
    """
    
    return result
```

### 7.5 Performance Characteristics

| Metric | Value |
|--------|-------|
| I/O Read | 1 row from `latest_summary` per account |
| I/O Write | 1 row to summary, 1 row update to latest_summary |
| Shuffle | Join shuffle (small - only affected accounts) |
| Complexity | O(1) per record |

### 7.6 Example (Consecutive Month)

**Existing latest_summary:**
```
+-------------+------------+------------------------+
|cons_acct_key|rpt_as_of_mo|balance_am_history      |
+-------------+------------+------------------------+
|2001         |2025-12     |[4500, 4000, 3500, ...] |
+-------------+------------+------------------------+
```

**Input (accounts table):**
```
+-------------+------------+----------+
|cons_acct_key|rpt_as_of_mo|balance_am|
+-------------+------------+----------+
|2001         |2026-01     |5000      |
+-------------+------------+----------+
```

**Output (summary table):**
```
+-------------+------------+----------------------------+
|cons_acct_key|rpt_as_of_mo|balance_am_history          |
+-------------+------------+----------------------------+
|2001         |2026-01     |[5000, 4500, 4000, 3500, ...]|
+-------------+------------+----------------------------+
```

### 7.7 Example (With Gap)

**Existing:** Latest = 2025-12, balance_am_history = [4500, 4000, ...]

**Input:** Month = 2026-03, balance = 5500 (MONTH_DIFF = 3)

**Output:** balance_am_history = [5500, NULL, NULL, 4500, 4000, ...]

---

## 8. Case III: Backfill

### 8.1 Definition

**Case III** applies when:
- Account EXISTS in `latest_summary`
- New month < Latest month (late-arriving data)

### 7.2 Processing Logic

Backfill requires TWO operations:

1. **CREATE** new summary row for the backfill month
2. **UPDATE** all future summary rows to include backfill data

```
Existing Summary:
  2025-11: [1000, 900, ...]       ← Will be UPDATED
  2025-12: [1100, 1000, 900, ...] ← Will be UPDATED
  2026-01: [1200, 1100, 1000, ...]← Will be UPDATED
  
Backfill Arrives: 2025-10, balance = 950

After Processing:
  2025-10: [950, 900, ...]        ← NEW ROW CREATED
  2025-11: [1000, 950, 900, ...]  ← UPDATED (950 inserted at position 1)
  2025-12: [1100, 1000, 950, ...] ← UPDATED (950 inserted at position 2)
  2026-01: [1200, 1100, 1000, ...]← UPDATED (950 inserted at position 3)
```

### 7.3 Implementation

```python
def process_case_iii(case_iii_df, config):
    """
    Backfill processing - most complex case.
    
    Part A: Create new summary rows for backfill months
    Part B: Update future summary rows with backfill data
    Part C: Combine new and updated rows
    """
    
    # PART A: Create new summary rows for backfill months
    # Find closest prior summary row
    # Inherit history from prior row
    # Insert new value at position 0
    
    # PART B: Update future summary rows
    # Load ALL summary rows for affected accounts (expensive!)
    # For each future row:
    #   Calculate position where backfill should be inserted
    #   Rebuild array with backfill value at correct position
    
    # PART C: Combine and return
    return new_rows.unionByName(updated_rows)
```

### 7.4 Performance Characteristics

| Metric | Value |
|--------|-------|
| I/O Read | ALL summary rows for affected accounts |
| I/O Write | 1 new row + N updates per backfill |
| Shuffle | Large shuffle for history rebuild |
| Complexity | O(N) per account where N = future months |

**This is the most expensive case** - but backfills are rare (~4% of volume).

### 7.5 Example

**Existing Summary for Account 3001:**
```
+------------+----------------------------+
|rpt_as_of_mo|balance_am_history          |
+------------+----------------------------+
|2025-11     |[8000, 7500, 7000, ...]     |
|2025-12     |[8500, 8000, 7500, ...]     |
|2026-01     |[9000, 8500, 8000, ...]     |
+------------+----------------------------+
```

**Backfill Arrives:** 2025-10, balance = 7800

**After Processing:**
```
+------------+----------------------------+--------+
|rpt_as_of_mo|balance_am_history          |Action  |
+------------+----------------------------+--------+
|2025-10     |[7800, 7500, 7000, ...]     |CREATED |
|2025-11     |[8000, 7800, 7500, ...]     |UPDATED |
|2025-12     |[8500, 8000, 7800, ...]     |UPDATED |
|2026-01     |[9000, 8500, 8000, ...]     |UPDATED |
+------------+----------------------------+--------+
```

### 7.6 Why Case III is Kept Separate

Despite being expensive, Case III MUST be separate because:
1. It's the only case that modifies EXISTING summary rows
2. It requires reading historical data (unavoidable)
3. Mixing with Case II would force unnecessary history reads for forward entries

---

## 9. Case IV: Bulk Historical Load

### 9.1 Definition

**Case IV** applies when:
- Account does NOT exist in `latest_summary`
- MULTIPLE months of data arrive for this account in the current batch

### 8.2 Processing Logic

Uses **window functions** to build arrays progressively:

```
Input: Account 8001 with 12 months (Jan-Dec 2025)

Processing:
  Jan 2025: [10000, NULL, NULL, ...]                ← First month
  Feb 2025: [9500, 10000, NULL, ...]                ← Window sees Jan
  Mar 2025: [9000, 9500, 10000, NULL, ...]          ← Window sees Jan, Feb
  ...
  Dec 2025: [4500, 5000, 5500, ..., 10000, NULL...] ← Window sees all 11 prior
```

### 8.3 Implementation

```python
def process_case_iv(case_iv_df, config):
    """
    Bulk historical load using window functions.
    
    No joins with existing summary - process entirely within new data.
    """
    
    # Window specification: all rows from start to current, ordered by month
    window_spec = Window.partitionBy('cons_acct_key') \
                        .orderBy('month_int') \
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    # Collect values within window and reverse (most recent first)
    for rolling_col in config['rolling_columns']:
        result = result.withColumn(
            f"{rolling_col['name']}_history",
            F.slice(
                F.reverse(
                    F.collect_list(rolling_col['mapper_column']).over(window_spec)
                ),
                1, 36
            )
        )
    
    # Pad with NULLs if less than 36 months
    # Handle gaps within the data
    
    return result
```

### 8.4 Gap Handling in Bulk Load

When there are gaps in the bulk data:

```
Input: Account 5001 with months Jan, Mar, Apr (Feb missing)

Processing:
  Jan 2025: [6000, NULL, NULL, ...]           
  Mar 2025: [5400, NULL, 6000, NULL, ...]     ← NULL for missing Feb
  Apr 2025: [5100, 5400, NULL, 6000, ...]     ← NULL for missing Feb propagates
```

### 8.5 Performance Characteristics

| Metric | Value |
|--------|-------|
| I/O Read | None (new account, no existing data) |
| I/O Write | N rows per account |
| Shuffle | Partition by account (bounded) |
| Complexity | O(N) per account using window functions |

### 8.6 Example

**Input (accounts table) - Account 8001 with 12 months:**
```
+-------------+------------+----------+
|cons_acct_key|rpt_as_of_mo|balance_am|
+-------------+------------+----------+
|8001         |2025-01     |10000     |
|8001         |2025-02     |9500      |
|8001         |2025-03     |9000      |
...
|8001         |2025-12     |4500      |
+-------------+------------+----------+
```

**Output (summary table):**
```
+-------------+------------+----------------------------------------+
|cons_acct_key|rpt_as_of_mo|balance_am_history                      |
+-------------+------------+----------------------------------------+
|8001         |2025-01     |[10000, NULL, NULL, ...]                |
|8001         |2025-02     |[9500, 10000, NULL, ...]                |
|8001         |2025-03     |[9000, 9500, 10000, NULL, ...]          |
...
|8001         |2025-12     |[4500, 5000, ..., 10000, NULL, ...]     |
+-------------+------------+----------------------------------------+
```

---

## 10. Data Flow Diagrams

### 10.1 Overall Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│    ┌──────────────┐                                                          │
│    │   ACCOUNTS   │                                                          │
│    │    TABLE     │                                                          │
│    └──────┬───────┘                                                          │
│           │                                                                  │
│           │ Filter: base_ts > last_processed_ts                              │
│           │                                                                  │
│           ▼                                                                  │
│    ┌──────────────────────────────────────────────────────────────────┐     │
│    │                    PREPARE SOURCE DATA                            │     │
│    │  - Apply column mappings                                          │     │
│    │  - Apply column transformations (sentinel values)                 │     │
│    │  - Calculate inferred columns                                     │     │
│    │  - Convert month to integer                                       │     │
│    └───────────────────────────┬──────────────────────────────────────┘     │
│                                │                                             │
│                                ▼                                             │
│    ┌──────────────────────────────────────────────────────────────────┐     │
│    │                 JOIN WITH LATEST_SUMMARY                          │     │
│    │                                                                   │     │
│    │  accounts LEFT JOIN latest_summary ON cons_acct_key               │     │
│    │                                                                   │     │
│    └───────────────────────────┬──────────────────────────────────────┘     │
│                                │                                             │
│                                ▼                                             │
│    ┌──────────────────────────────────────────────────────────────────┐     │
│    │                      CLASSIFY                                     │     │
│    │                                                                   │     │
│    │  ┌─────────────────────────────────────────────────────────────┐ │     │
│    │  │  CASE I:  account NOT in latest_summary AND single month    │ │     │
│    │  │  CASE II: account in latest_summary AND new > latest        │ │     │
│    │  │  CASE III: account in latest_summary AND new <= latest      │ │     │
│    │  │  CASE IV: account NOT in latest_summary AND multi-month     │ │     │
│    │  └─────────────────────────────────────────────────────────────┘ │     │
│    │                                                                   │     │
│    └───────────────────────────┬──────────────────────────────────────┘     │
│                                │                                             │
│         ┌──────────────────────┼──────────────────────┐                      │
│         │                      │                      │                      │
│         ▼                      ▼                      ▼                      │
│    ┌─────────┐           ┌──────────┐           ┌──────────┐                │
│    │ CASE I  │           │ CASE II  │           │ CASE III │                │
│    │ CASE IV │           │          │           │          │                │
│    └────┬────┘           └────┬─────┘           └────┬─────┘                │
│         │                     │                      │                       │
│         │ No joins            │ Join latest only     │ Read full history    │
│         │                     │                      │                       │
│         ▼                     ▼                      ▼                       │
│    ┌─────────┐           ┌──────────┐           ┌───────────┐               │
│    │ Create  │           │  Shift   │           │  Rebuild  │               │
│    │ Arrays  │           │  Arrays  │           │  + Update │               │
│    └────┬────┘           └────┬─────┘           └─────┬─────┘               │
│         │                     │                       │                      │
│         └─────────────────────┴───────────────────────┘                      │
│                               │                                              │
│                               ▼                                              │
│                    ┌──────────────────┐                                      │
│                    │   UNION ALL      │                                      │
│                    └────────┬─────────┘                                      │
│                             │                                                │
│                             ▼                                                │
│                    ┌──────────────────┐                                      │
│                    │   DEDUPLICATE    │                                      │
│                    │   (if needed)    │                                      │
│                    └────────┬─────────┘                                      │
│                             │                                                │
│                             ▼                                                │
│                    ┌──────────────────┐                                      │
│                    │   MERGE INTO     │                                      │
│                    │     SUMMARY      │                                      │
│                    └────────┬─────────┘                                      │
│                             │                                                │
│                             ▼                                                │
│                    ┌──────────────────┐                                      │
│                    │   MERGE INTO     │                                      │
│                    │  LATEST_SUMMARY  │                                      │
│                    └──────────────────┘                                      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 9.2 Case II Detail Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                    CASE II: FORWARD ENTRY                        │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌────────────────┐                ┌─────────────────┐          │
│  │  case_ii_df    │                │ latest_summary  │          │
│  │  (new records) │                │  (1 row/acct)   │          │
│  └───────┬────────┘                └────────┬────────┘          │
│          │                                  │                    │
│          │  Extract affected account keys   │                    │
│          │ ───────────────────────────────► │                    │
│          │                                  │                    │
│          │  Filter to affected accounts     │                    │
│          │ ◄─────────────────────────────── │                    │
│          │                                  │                    │
│          ▼                                  ▼                    │
│  ┌────────────────────────────────────────────────────────┐     │
│  │                       JOIN                              │     │
│  │  case_ii_records c JOIN latest_summary p                │     │
│  │  ON c.cons_acct_key = p.cons_acct_key                   │     │
│  └─────────────────────────┬──────────────────────────────┘     │
│                            │                                     │
│                            ▼                                     │
│  ┌────────────────────────────────────────────────────────┐     │
│  │                  SHIFT ARRAYS                           │     │
│  │                                                         │     │
│  │  CASE MONTH_DIFF = 1:                                   │     │
│  │    new_array = [new_value] + prev_array[0:35]           │     │
│  │                                                         │     │
│  │  CASE MONTH_DIFF > 1:                                   │     │
│  │    new_array = [new_value] +                            │     │
│  │                [NULL * (MONTH_DIFF-1)] +                │     │
│  │                prev_array[0:36-MONTH_DIFF]              │     │
│  │                                                         │     │
│  └─────────────────────────┬──────────────────────────────┘     │
│                            │                                     │
│                            ▼                                     │
│  ┌────────────────────────────────────────────────────────┐     │
│  │                GENERATE GRID COLUMNS                    │     │
│  │  payment_history_grid = concat(payment_rating_array)    │     │
│  └─────────────────────────┬──────────────────────────────┘     │
│                            │                                     │
│                            ▼                                     │
│                    ┌──────────────┐                              │
│                    │    OUTPUT    │                              │
│                    └──────────────┘                              │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 9.3 Case III Detail Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                    CASE III: BACKFILL                            │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌────────────────┐                ┌─────────────────┐          │
│  │  case_iii_df   │                │    summary      │          │
│  │  (backfills)   │                │ (full history)  │          │
│  └───────┬────────┘                └────────┬────────┘          │
│          │                                  │                    │
│          │  Identify affected accounts      │                    │
│          │  Calculate month range           │                    │
│          │ ───────────────────────────────► │                    │
│          │                                  │                    │
│          │  Load partitions in range        │                    │
│          │  for affected accounts           │                    │
│          │ ◄─────────────────────────────── │                    │
│          │                                  │                    │
│          ▼                                  ▼                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                     PART A: CREATE NEW ROWS               │   │
│  │                                                           │   │
│  │  For each backfill month:                                 │   │
│  │  1. Find closest PRIOR summary row                        │   │
│  │  2. Create new array: [backfill_value] + prior_array      │   │
│  │                                                           │   │
│  └─────────────────────────┬────────────────────────────────┘   │
│                            │                                     │
│                            ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                     PART B: UPDATE FUTURE ROWS            │   │
│  │                                                           │   │
│  │  For each future summary row (month > backfill_month):    │   │
│  │  1. Calculate position for backfill value                 │   │
│  │  2. Insert backfill value at correct position             │   │
│  │  3. Truncate array to 36 elements                         │   │
│  │                                                           │   │
│  │  Example:                                                 │   │
│  │    Backfill month: 2025-10                                │   │
│  │    Future month: 2025-12 (diff = 2)                       │   │
│  │    Insert backfill at position 2                          │   │
│  │                                                           │   │
│  └─────────────────────────┬────────────────────────────────┘   │
│                            │                                     │
│                            ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                     PART C: COMBINE                       │   │
│  │                                                           │   │
│  │  new_rows UNION updated_rows                              │   │
│  │                                                           │   │
│  └─────────────────────────┬────────────────────────────────┘   │
│                            │                                     │
│                            ▼                                     │
│                    ┌──────────────┐                              │
│                    │    OUTPUT    │                              │
│                    └──────────────┘                              │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## 11. Performance Considerations

### 11.1 I/O Optimization

| Case | Read Strategy | Why |
|------|---------------|-----|
| Case I | No read | New account, nothing to read |
| Case II | Read `latest_summary` only | 50x smaller than `summary` |
| Case III | Partition pruning | Read only affected month range |
| Case IV | No read | New account, nothing to read |

### 10.2 Partition Pruning (Iceberg)

```python
# Case III partition pruning
# Only read partitions in the affected range
affected_range = f"rpt_as_of_mo BETWEEN '{min_month}' AND '{max_month}'"

spark.sql(f"""
    SELECT * FROM summary
    WHERE {pk} IN (affected_accounts)
    AND {affected_range}
""")
```

### 10.3 Broadcast Optimization

```python
# Broadcast small DataFrames to avoid shuffle
if affected_count < config['broadcast_threshold']:
    affected_keys = F.broadcast(affected_keys)
```

### 10.4 Caching Strategy

```python
# Cache intermediate results for reuse
classified_df = classified_df.persist(StorageLevel.MEMORY_AND_DISK)

# Unpersist after use
classified_df.unpersist()
```

### 10.5 Resource Allocation

| Case Type | Executors | Memory | Notes |
|-----------|-----------|--------|-------|
| Case I/IV heavy | Standard | Standard | CPU-bound |
| Case II heavy | Standard | Standard | Small shuffle |
| Case III heavy | Increase | Increase | Large shuffle |

---

## 12. Assumptions and Constraints

### 12.1 Data Assumptions

| Assumption | Description | Impact if Violated |
|------------|-------------|-------------------|
| **Single month per batch (Case II)** | Each account has at most ONE forward month per batch | See Section 13.1 |
| **Ordered processing** | Batches processed in `base_ts` order | Out-of-order may cause incorrect classification |
| **Unique primary key per month** | One record per `(cons_acct_key, rpt_as_of_mo)` | Duplicates cause unpredictable results |
| **History length = 36** | Fixed array size | Configuration change requires schema migration |

### 11.2 Processing Constraints

| Constraint | Description |
|------------|-------------|
| **No intra-batch dependencies** | Records within a batch are processed independently |
| **Idempotent MERGE** | Re-running with same data produces same result |
| **latest_summary accuracy** | Must be in sync with summary table |

### 11.3 Operational Constraints

| Constraint | Description |
|------------|-------------|
| **Sequential batch processing** | Batches must be processed in order |
| **Partition immutability** | Old partitions should not be modified after processing |
| **Backfill window** | Backfills only valid within 36-month history window |

---

## 13. Edge Cases and Limitations

### 13.1 Multiple Forward Months in Single Batch

**Scenario:**
```
Account 2001 arrives with 3 forward months in same batch:
  2026-01, 2026-02, 2026-03
```

**Current Behavior:** All three classified as CASE II, all join with same latest_summary (2025-12).

**Result:** Incorrect arrays - all three have same shifted array.

**Mitigation Options:**
1. Route multi-month forward to Case IV-style processing
2. Pre-process to handle within-batch dependencies
3. Document as constraint: one forward month per account per batch

### 12.2 Multiple Backfills in Single Batch

**Scenario:**
```
Account 3001 arrives with 3 backfills in same batch:
  2025-08, 2025-09, 2025-10
```

**Current Behavior:** Case III processes them, but may not handle interdependencies correctly.

**Mitigation:** Case III should process backfills in order within the batch.

### 12.3 Mixed Forward and Backfill

**Scenario:**
```
Account arrives with both forward and backfill:
  2025-10 (backfill)
  2026-01 (forward)
```

**Current Behavior:** Classified separately - 2025-10 as CASE III, 2026-01 as CASE II.

**Consideration:** CASE II should account for backfill that will be processed.

### 12.4 Gap Beyond 36 Months

**Scenario:**
```
Account last seen 2022-01, new data arrives 2026-01 (48 month gap)
```

**Behavior:** All historical data falls outside array window. Treated as effectively new account.

### 12.5 latest_summary Inconsistency

**Scenario:** latest_summary out of sync with summary table.

**Impact:** Misclassification of accounts.

**Mitigation:** Periodic reconciliation job.

---

## 14. Configuration Reference

### 14.1 Pipeline Configuration

```json
{
  "source_table": "catalog.schema.accounts",
  "destination_table": "catalog.schema.summary",
  "latest_history_table": "catalog.schema.latest_summary",
  
  "primary_column": "cons_acct_key",
  "partition_column": "rpt_as_of_mo",
  "max_identifier_column": "base_ts",
  
  "history_length": 36,
  "broadcast_threshold": 100000,
  "cache_level": "MEMORY_AND_DISK",
  
  "rolling_columns": [
    {
      "name": "balance_am",
      "mapper_column": "cur_bal_am",
      "mapper_expr": "cur_bal_am",
      "type": "decimal(15,2)",
      "num_cols": 36
    }
  ],
  
  "grid_columns": [
    {
      "name": "payment_history_grid",
      "mapper_rolling_column": "payment_rating_cd",
      "placeholder": "?",
      "seperator": ""
    }
  ],
  
  "column_transformations": [],
  "inferred_columns": [],
  "columns": {}
}
```

### 13.2 Configuration Fields

| Field | Type | Description |
|-------|------|-------------|
| `source_table` | string | Fully qualified source table name |
| `destination_table` | string | Fully qualified summary table name |
| `latest_history_table` | string | Fully qualified latest_summary table name |
| `primary_column` | string | Primary key column |
| `partition_column` | string | Partition column (month) |
| `max_identifier_column` | string | Timestamp column for change detection |
| `history_length` | int | Array length (default 36) |
| `broadcast_threshold` | int | Max records to broadcast |
| `rolling_columns` | array | Rolling array column definitions |
| `grid_columns` | array | Grid column definitions |

---

## 15. Appendix

### 15.1 Glossary

| Term | Definition |
|------|------------|
| **Rolling Array** | Fixed-length array where position 0 is current month, position N is N months ago |
| **Forward Entry** | New data for a month after the latest processed month |
| **Backfill** | Late-arriving data for a month before the latest processed month |
| **Bulk Historical** | Multiple months of data for a new account |
| **latest_summary** | Metadata table containing latest row per account |
| **Grid Column** | String representation of a rolling array |

### 14.2 Related Documents

- `CHANGELOG.md` - Version history
- `PERFORMANCE.md` - Performance benchmarks
- `DEPLOYMENT.md` - Deployment procedures

### 14.3 Version History

| Version | Date | Changes |
|---------|------|---------|
| 9.0 | 2025-10 | Initial case-based architecture |
| 9.1 | 2025-11 | Added partition pruning |
| 9.2 | 2025-12 | Optimized Case III backfill |
| 9.3 | 2026-01 | Refactored to dict config, added grid columns |

---

*End of Design Document*
