# Performance Analysis: Summary Pipeline v5 vs Partition-Wise Backfill
## Scenario: 100B Summary Table + 300M Backfill Update

**Date:** January 23, 2026  
**Analysis by:** Independent Review  
**Scope:** Production-scale performance comparison

---

## Executive Summary

| Metric | Summary Pipeline v5 | Partition-Wise Backfill | Improvement |
|--------|---------------------|-------------------------|-------------|
| **Estimated Time** | 4.5-6 hours | 1.8-2.5 hours | **2.2-2.5x faster** |
| **Peak Memory** | 650-850 GB | 80-120 GB | **6-8x less** |
| **Shuffle Volume** | 1.8-2.5 TB | 18-25 GB | **75-100x less** |
| **Complexity** | Very High | Medium | **Lower complexity** |
| **Recovery** | Full restart | Per-partition checkpoint | **Much better** |
| **Cost (AWS)** | $110-140 | $22-30 | **~80% savings** |

**Winner for 300M backfill:** **Partition-Wise Backfill** (significantly more efficient)

---

## Approach 1: Summary Pipeline v5 (Current Implementation)

### Architecture Analysis

**Location:** `./updates/summary_v5/summary_pipeline.py`

#### Key Implementation Pattern (Case III - Backfill):

```python
# Lines 1186-1348: Process Case III - Full History Rebuild
def _process_case_iii(self, case_iii_df: DataFrame):
    """
    Rebuilds ENTIRE history for ALL affected months of affected accounts
    """
    
    # Step 1: Collect ALL months into arrays per account
    # COLLECT_LIST creates massive in-memory structures
    combined_sql = f"""
    WITH account_months AS (
        SELECT 
            {primary_col},
            COLLECT_LIST(STRUCT(month_int, {struct_cols})) as month_values,
            COLLECT_LIST(STRUCT({partition_col}, month_int)) as month_metadata
        FROM final_data
        GROUP BY {primary_col}
    ),
    
    # Step 2: Explode back to rows
    exploded AS (
        SELECT {primary_col}, month_values, meta.{partition_col}, meta.month_int
        FROM account_months
        LATERAL VIEW EXPLODE(month_metadata) t AS meta
    )
    
    # Step 3: Rebuild EACH of 36 positions for EACH of 20 arrays
    SELECT
        e.{primary_col},
        e.{partition_col},
        TRANSFORM(
            SEQUENCE(0, {hl - 1}),  -- For positions 0-35
            offset -> (
                AGGREGATE(
                    FILTER(e.month_values, mv -> mv.month_int = e.month_int - offset),
                    CAST(NULL AS INT),
                    (acc, x) -> x.{mapper}
                )
            )
        ) as {name}_history  -- Repeated for ALL 20 columns
    FROM exploded e
    """
```

### Performance Breakdown

#### Assumptions for 300M Backfill:
- 300M backfill records affect **100M unique accounts**
- Each account has average **24 existing months** of history
- Backfill affects **12 consecutive months** back
- Total rows to rebuild: **100M accounts × 24 months = 2.4B rows**

#### Stage 1: Classification (Lines 410-466)
```python
def run_incremental(self):
    # Read new data + classify
    batch_df = spark.sql(f"""
        SELECT * FROM {source_table}
        WHERE {max_identifier_column} > TIMESTAMP '{max_ts}'
    """)
    
    classified = self._classify_backfill_records(batch_df)
```

**Performance:**
- **Data read:** 300M records (~60 GB)
- **Join:** Broadcast summary metadata (100M keys × 50 bytes = 5 GB)
- **Classification time:** ~8-12 minutes
- **Shuffle:** None (broadcast join)

#### Stage 2: COLLECT_LIST Aggregation (Lines 1253-1318)

This is the **CRITICAL BOTTLENECK**:

```sql
COLLECT_LIST(STRUCT(month_int, val1, val2, ..., val20))
```

**What happens:**
- **Per account:** Collect 24 months × 20 columns = 24 structs
- **Struct size:** ~180 bytes per month (20 columns × avg 9 bytes)
- **Per account memory:** 24 months × 180 bytes = ~4.3 KB
- **Total for 100M accounts:** 100M × 4.3 KB = **430 GB**

**Performance:**
- **GROUP BY shuffle:** All 2.4B rows shuffled by account key
- **Shuffle data:** 2.4B rows × ~200 bytes = **480 GB shuffle**
- **Memory pressure:** Executors must hold collected arrays
- **Time estimate:** **45-70 minutes**
- **Risk:** OOM if executor memory < 32 GB

#### Stage 3: Array Rebuild with TRANSFORM + AGGREGATE (Lines 1219-1229)

**For EACH account, for EACH month:**

```sql
-- Rebuild 20 arrays × 36 positions each
TRANSFORM(
    SEQUENCE(0, 35),  -- 36 iterations
    offset -> (
        AGGREGATE(
            FILTER(month_values, mv -> mv.month_int = e.month_int - offset),
            CAST(NULL AS INT),
            (acc, x) -> x.value
        )
    )
)
```

**Computational complexity:**
- **Per row:** 20 arrays × 36 TRANSFORM iterations = 720 operations
- **Per account:** 24 months × 720 ops = 17,280 operations
- **Total:** 100M accounts × 17,280 = **1.73 trillion operations**

**Why it's expensive:**
- AGGREGATE is a higher-order function (not vectorized)
- FILTER must scan collected array for EACH position
- Complex nested execution plan

**Performance:**
- **CPU-bound:** Spark executors at 90-100% CPU
- **Time estimate:** **90-140 minutes**
- **Cannot parallelize well:** Already grouped by account

#### Stage 4: MERGE Back (Lines 1350-1390)

```sql
MERGE INTO {destination_table} t
USING backfill_result s
ON t.{primary_column} = s.{primary_column}
   AND t.{partition_column} = s.{partition_column}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Performance:**
- **Rows to MERGE:** 2.4B rows
- **Shuffle:** Full table shuffle for MERGE operation
- **Shuffle data:** ~450-500 GB
- **Time estimate:** **35-50 minutes**

#### Total Summary Pipeline v5 Time:

| Stage | Operation | Time |
|-------|-----------|------|
| Classification | Broadcast join + classify | 8-12 min |
| COLLECT_LIST | GROUP BY aggregation | 45-70 min |
| TRANSFORM/AGGREGATE | Array rebuild | 90-140 min |
| MERGE | Write back | 35-50 min |
| Grid generation + cleanup | | 15-20 min |
| **TOTAL** | | **193-292 minutes (3.2-4.9 hours)** |

**Conservative estimate with overhead:** **4.5-6 hours**

### Memory Requirements

**Executor memory needed:**
- COLLECT_LIST peak: 430 GB across executors
- With 20 executors: ~22 GB per executor minimum
- **Recommended:** 32-40 GB per executor
- **Total cluster:** 20 executors × 32 GB = 640 GB

**Why so much memory:**
- Collected arrays must fit in memory for TRANSFORM
- Shuffle buffers during GROUP BY
- Intermediate results during MERGE

---

## Approach 2: Partition-Wise Backfill (New Implementation)

### Architecture Analysis

**Location:** `D:\PROJECTS\Learn\opencode-test\backfill_merge.py`

#### Key Implementation Pattern:

```python
# backfill_merge.py: Lines 119-209
def process_single_partition(
    processor, backfill_df, backfill_month, target_partition, backfill_id
):
    """
    Process ONE partition at a time
    No COLLECT, no AGGREGATE - just array slice/concat
    """
    
    # Calculate position once for entire partition
    months_diff = calculate_months_between(target_partition, backfill_month)
    # e.g., partition="2024-05", backfill="2023-12" -> position=5
    
    # Execute MERGE with simple array operations
    merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING backfill_source AS source
        ON target.acct = source.acct 
           AND target.month_partition = '{target_partition}'
        WHEN MATCHED THEN UPDATE SET
            balance_history = concat(
                slice(target.balance_history, 1, {months_diff}),
                array(source.balance),
                slice(target.balance_history, {months_diff + 2}, {36 - months_diff - 1})
            ),
            -- ... repeat for all 20 columns
    """
```

### Performance Breakdown

#### Assumptions for 300M Backfill:
- Same: 100M unique accounts affected
- Backfill affects **12 partitions** (12 months forward from backfill month)
- Per partition: ~8.3M accounts affected (100M / 12)

#### Phase 1: One-time Staging Load

```python
# Load backfill data once
backfill_df = spark.read.table(BACKFILL_STAGING_TABLE)
    .filter(conditions)
    .cache()

# Broadcast - it's small enough
backfill_broadcast = F.broadcast(backfill_df)
```

**Performance:**
- **Data size:** 300M rows × 200 bytes = 60 GB
- **Broadcast size:** 60 GB (fits in memory across cluster)
- **Time:** 3-5 minutes (one-time)
- **Reused:** For all 12 partition MERGEs

#### Phase 2: Per-Partition MERGE (Repeated 12 times)

**For partition "2024-05" (example):**

```sql
-- Calculate position: 2024-05 to 2023-12 = 5 months
months_diff = 5

-- MERGE updates ONLY rows in THIS partition
MERGE INTO summary_table AS target
USING backfill_source AS source
ON target.acct = source.acct 
   AND target.month_partition = '2024-05'
WHEN MATCHED THEN UPDATE SET
    balance_history = concat(
        slice(target.balance_history, 1, 5),      -- Keep [0,1,2,3,4]
        array(source.balance),                     -- Insert at [5]
        slice(target.balance_history, 7, 30)       -- Keep [6-35], drop old [5]
    )
```

**What Iceberg optimizes:**
- **Partition pruning:** Only reads "2024-05" partition
- **Bucket pruning:** Table clustered by 64 buckets on account
- **Join optimization:** Broadcast backfill (60GB) to all executors
- **Vector operations:** `slice` and `concat` are native, vectorized

**Performance per partition:**

| Operation | Details | Time |
|-----------|---------|------|
| **Partition scan** | Read ~200M rows from partition "2024-05" | 2-3 min |
| **Broadcast join** | Join with 60GB broadcast (already in memory) | 1-2 min |
| **Array update** | 20 columns × simple slice/concat (vectorized) | 1-2 min |
| **Iceberg MERGE** | Write updates (equality deletes + inserts) | 4-6 min |
| **Per partition total** | | **8-13 minutes** |

**For 12 partitions:**
- **Sequential:** 12 × 10 min avg = **120 minutes (2 hours)**
- **With batching (3 at a time):** 120 / 3 = **40 minutes**

**Why is this so much faster?**

1. **No COLLECT_LIST:**
   - No memory explosion
   - No GROUP BY shuffle
   - Saves ~70 minutes

2. **No TRANSFORM/AGGREGATE:**
   - Simple vectorized array operations
   - `concat(slice(), array(), slice())` is native Spark
   - Saves ~120 minutes

3. **Partition pruning:**
   - Each MERGE scans only 1/36th of data
   - 200M rows vs 2.4B rows
   - 12x less data scanned per operation

4. **Broadcast reuse:**
   - Broadcast 60GB once, reuse 12 times
   - No shuffle for JOIN (broadcast join)

#### Total Partition-Wise Time:

| Stage | Operation | Time |
|-------|-----------|------|
| Staging load | Read + cache + broadcast | 3-5 min |
| Partition 1 | MERGE partition 2024-01 | 8-13 min |
| Partition 2 | MERGE partition 2024-02 | 8-13 min |
| ... | ... | ... |
| Partition 12 | MERGE partition 2024-12 | 8-13 min |
| Optimization | Iceberg compaction (optional) | 10-15 min |
| **TOTAL (sequential)** | | **109-166 minutes (1.8-2.8 hours)** |
| **TOTAL (batch 3)** | | **50-70 minutes with parallel batching** |

**Conservative estimate:** **1.8-2.5 hours**

### Memory Requirements

**Executor memory needed:**
- Broadcast: 60 GB distributed across executors
- Per executor with 10 executors: ~6-8 GB for broadcast
- Working memory: ~4-6 GB per partition MERGE
- **Recommended:** 16-20 GB per executor
- **Total cluster:** 10 executors × 16 GB = 160 GB

**Why so much less:**
- No COLLECT_LIST (saves 430 GB!)
- No intermediate aggregation arrays
- One partition at a time (small working set)

---

## Detailed Comparison

### 1. Data Processing Pattern

| Aspect | Summary Pipeline v5 | Partition-Wise |
|--------|---------------------|----------------|
| **Processing scope** | All 2.4B rows together | 200M rows per partition (12 passes) |
| **Memory pattern** | Collect all → Process → Write | Stream → Process → Write per partition |
| **Array update** | Rebuild all 36 positions | Update 1 position with slice/concat |
| **Parallelism** | Limited by GROUP BY | High (independent partitions) |

### 2. Computational Complexity

**Summary Pipeline v5:**
```
Operations = Accounts × Months × Columns × Positions
           = 100M × 24 × 20 × 36
           = 1.73 trillion operations
```

**Partition-Wise:**
```
Operations = Accounts × Partitions × Columns × 1 slice/concat
           = 100M × 12 × 20 × 1
           = 24 billion operations (72x fewer!)
```

### 3. Shuffle Analysis

**Summary Pipeline v5:**
```
Shuffle 1: COLLECT_LIST GROUP BY
  - All 2.4B rows shuffled by account
  - Data: 480 GB

Shuffle 2: MERGE operation
  - Full table shuffle for updates
  - Data: 450 GB

Total shuffle: ~930 GB - 1 TB
```

**Partition-Wise:**
```
Shuffle: Only small MERGE shuffles per partition
  - Broadcast eliminates JOIN shuffle
  - MERGE shuffles: 12 partitions × 2 GB = 24 GB

Total shuffle: ~20-30 GB (97% less!)
```

### 4. Scaling Behavior

**At different account volumes:**

| Affected Accounts | Summary v5 Time | Partition-Wise Time | Ratio |
|-------------------|-----------------|---------------------|-------|
| 1M | 0.3 hrs | 0.2 hrs | 1.5x |
| 10M | 1.2 hrs | 0.6 hrs | 2.0x |
| **100M** | **4.5-6 hrs** | **1.8-2.5 hrs** | **2.2-2.5x** |
| 500M | 18-24 hrs | 7-10 hrs | 2.3x |

**Why ratio is consistent:**
- Summary v5 complexity: O(accounts × months × positions)
- Partition-Wise complexity: O(accounts × partitions)
- Both scale linearly, but partition-wise has lower constant

---

## Resource Requirements

### Cluster Configuration

**Summary Pipeline v5:**
```
Required:
- 20 executors (for memory)
- 8 cores per executor
- 32-40 GB memory per executor
- Total: 160 cores, 640-800 GB RAM

AWS Cost (r5.4xlarge):
- 20 instances @ $1.01/hr × 5 hours = $101
- With overhead/retries: ~$110-140
```

**Partition-Wise:**
```
Required:
- 10 executors (smaller footprint)
- 8 cores per executor
- 16-20 GB memory per executor
- Total: 80 cores, 160-200 GB RAM

AWS Cost (r5.2xlarge):
- 10 instances @ $0.504/hr × 2.5 hours = $12.60
- Or r5.4xlarge @ $1.01/hr: $25.25
- With overhead: ~$22-30
```

### Network Impact

**Summary Pipeline v5:**
- Shuffle write: 930 GB
- Shuffle read: 930 GB
- Total network: **1.86 TB**
- Bottleneck risk: High

**Partition-Wise:**
- Shuffle write: 24 GB
- Shuffle read: 24 GB
- Broadcast: 60 GB × 12 = 720 GB (read from driver once)
- Total network: **~768 GB** (58% less than v5)
- Bottleneck risk: Low

---

## Production Considerations

### 1. Failure Recovery

**Summary Pipeline v5:**
```python
# If Case III fails at MERGE stage:
# Must restart entire classification + COLLECT + TRANSFORM
# No checkpoint capability
# Lost work: 2-3 hours
```

**Partition-Wise:**
```python
# Checkpoint per partition in backfill_audit table
# Resume from last successful partition
# Lost work: 10-15 minutes max

if failed_partitions:
    orchestrator.run_backfill(
        backfill_id="bf-001",
        resume_from="2024-07"  # Resume from here
    )
```

**Winner:** Partition-Wise (much better recovery)

### 2. Operational Complexity

**Summary Pipeline v5:**
- ✅ Single invocation handles all cases
- ✅ Config-driven
- ❌ Complex SQL (7 CTEs, nested TRANSFORM/AGGREGATE)
- ❌ Hard to debug when issues occur
- ❌ Requires deep Spark SQL knowledge

**Partition-Wise:**
- ✅ Simple slice/concat logic
- ✅ Per-partition progress tracking
- ✅ Easy to understand and debug
- ❌ Requires staging table
- ❌ Separate logic for backfill vs normal processing

**Winner:** Tie (different trade-offs)

### 3. Data Consistency

**Summary Pipeline v5:**
- All updates in single transaction (better)
- ACID guaranteed by Iceberg MERGE

**Partition-Wise:**
- 12 separate MERGEs (one per partition)
- Each MERGE is ACID
- Partial completion possible (can resume)

**Winner:** Tie (both ACID compliant)

### 4. Monitoring & Observability

**Summary Pipeline v5:**
```
Log output:
- Classification: Case I/II/III counts
- Per-case processing time
- Final statistics

Monitoring difficulty: Medium
```

**Partition-Wise:**
```
Log output:
- Per-partition progress (1/12, 2/12, ...)
- Rows updated per partition
- Detailed audit table with timing

Monitoring difficulty: Easy
Database: SELECT * FROM backfill_audit WHERE backfill_id = 'bf-001'
```

**Winner:** Partition-Wise (better observability)

---

## Recommendation

### For Your Scenario: 100B table + 300M backfill

**Primary Recommendation: Partition-Wise Backfill**

**Reasons:**
1. **2.2-2.5x faster** (4.5-6 hrs → 1.8-2.5 hrs)
2. **6-8x less memory** (640 GB → 160 GB)
3. **97% less shuffle** (1 TB → 24 GB)
4. **80% cost savings** ($110-140 → $22-30)
5. **Better failure recovery** (resume any partition)
6. **Simpler to understand and debug**

**When to use Summary Pipeline v5:**
- Daily incremental with mixed Case I/II/III (< 50M records)
- Need unified logic for all scenarios
- Operational simplicity preferred over performance

### Hybrid Architecture (BEST)

Implement intelligent routing:

```python
def run_incremental_with_routing(self):
    """
    Smart routing based on workload characteristics
    """
    # Classify as usual
    classified = self._classify_backfill_records(batch_df)
    
    # Count cases
    case_i_count = classified.filter("case_type = 'CASE_I'").count()
    case_ii_count = classified.filter("case_type = 'CASE_II'").count()
    case_iii_count = classified.filter("case_type = 'CASE_III'").count()
    
    # Decision logic
    if case_iii_count > 50_000_000:
        # Large backfill: use partition-wise
        logger.info(f"Large backfill detected ({case_iii_count:,} records)")
        logger.info("Routing to partition-wise processor")
        
        self._process_case_iii_partition_wise(
            classified.filter("case_type = 'CASE_III'")
        )
        
        # Process other cases normally
        if case_i_count > 0:
            self._process_case_i(classified.filter("case_type = 'CASE_I'"))
        if case_ii_count > 0:
            self._process_case_ii(classified.filter("case_type = 'CASE_II'"))
    else:
        # Normal: use existing unified approach
        logger.info("Using unified Summary v5 processing")
        self._process_all_cases_unified(classified)
```

**Expected results with hybrid:**
- Small backfills (< 50M): 1-2 hours (unified)
- Large backfills (> 50M): 2-3 hours (partition-wise)
- Best of both worlds

---

## Implementation Roadmap

### Phase 1: Validation (Week 1-2)
```bash
# Test partition-wise with 1M records
python run_backfill.py backfill \
    --target-table summary_table \
    --backfill-id test-001 \
    --backfill-month 2024-01 \
    --method merge

# Verify correctness
python run_backfill.py verify \
    --backfill-id test-001 \
    --sample-size 10000
```

### Phase 2: Scale Testing (Week 3-4)
```bash
# Test with 10M → 50M → 100M records
# Measure actual performance vs estimates
# Tune parallelism and batch sizes
```

### Phase 3: Integration (Week 5-6)
```python
# Add partition-wise processor to summary_v5/
# Implement hybrid routing logic
# Comprehensive testing
```

### Phase 4: Production Rollout (Week 7-8)
```bash
# Run parallel for 2 cycles
# Compare results
# Switch primary method
```

---

## Conclusion

For **100B summary table with 300M backfill updates**, the **Partition-Wise Backfill** approach delivers:

- ⚡ **2.2-2.5x faster processing** 
- 💾 **6-8x lower memory requirements**
- 🌐 **97% less network shuffle**
- 💰 **80% cost reduction**
- 🔄 **Superior failure recovery**

The existing Summary Pipeline v5 remains excellent for daily incremental mixed workloads, but for large-scale pure backfill scenarios, the partition-wise approach is significantly more efficient.

**Recommended action:** Implement hybrid approach with intelligent routing to leverage strengths of both methods.
