# Performance Analysis — Summary Pipeline (main)

**Pipeline Version**: main (post v9.4.8)
**Last Updated**: March 2026

---

## Table of Contents

1. [Version Comparison](#1-version-comparison)
2. [Current Production Environment](#2-current-production-environment)
3. [Workload Profile](#3-workload-profile)
4. [Current Spark Configuration](#4-current-spark-configuration)
5. [Per-Phase I/O Breakdown](#5-per-phase-io-breakdown)
6. [Current Processing Times](#6-current-processing-times)
7. [Bottleneck Analysis](#7-bottleneck-analysis)
8. [Optimization Plan](#8-optimization-plan)
9. [Recommended Cluster Configuration](#9-recommended-cluster-configuration)
10. [Projected Improvements](#10-projected-improvements)
11. [Implementation Roadmap](#11-implementation-roadmap)
12. [Write Partition Sizing](#12-write-partition-sizing)
13. [Case III Chunked Merge](#13-case-iii-chunked-merge)
14. [Memory Profile](#14-memory-profile)
15. [Local Docker Benchmarks](#15-local-docker-benchmarks)
16. [Running Benchmarks](#16-running-benchmarks)

---

## 1. Version Comparison

| Version | Watermark Tracker | Per-Case Temp Tables | Chunked Merge | Source Window Filter | Bucketed Temps | Soft Delete | Rollback |
|---------|-------------------|----------------------|---------------|----------------------|----------------|-------------|----------|
| v9.3 | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |
| v9.4 | ✗ | Pooled temp table | ✗ | ✗ | ✗ | ✗ | ✗ |
| v9.4.8 | ✗ | Pooled temp table | ✗ | ✗ | ✗ | ✗ | ✗ |
| **main (Feb)** | **✓** | **✓ (per-case)** | **✓ (III)** | **✓** | ✗ | ✗ | ✗ |
| **main (Mar)** | **✓ (enhanced)** | **✓ (per-case)** | **✓ (II+III)** | **✓** | **✓ (64 buckets)** | **✓** | **✓** |

---

## 2. Current Production Environment

### 2.1 Table Layout

| Table | Size | Partitioning | Bucketing | File Size | Table Format |
|-------|------|--------------|-----------|-----------|-------------|
| `summary` | **8 TB** | `rpt_as_of_mo` (~36 active partitions) | 64 buckets by `cons_acct_key` | 512 MB | Iceberg MOR |
| `latest_summary` | ~800 GB | None / bucketed | — | — | Iceberg MOR |
| `watermark_tracker` | Negligible | None | — | — | Iceberg |

### 2.2 File Distribution (Estimated)

```
summary table:
  Total files:   8 TB / 512 MB = ~16,000 files
  Per partition:  ~444 files
  Per bucket:     ~7 files per bucket per partition

  Partition: rpt_as_of_mo = "2025-12" (~220 GB)
  ├── bucket-00/  (~7 files × 512 MB = ~3.5 GB)
  ├── bucket-01/  (~7 files × 512 MB = ~3.5 GB)
  ├── ...
  └── bucket-63/  (~7 files × 512 MB = ~3.5 GB)
```

### 2.3 Current Cluster

| Component | Spec |
|-----------|------|
| Platform | AWS EMR |
| Instance | r6i.8xlarge (32 vCPU, 256 GB RAM) |
| Min Executors | 80 |
| Max Executors | 120 |
| Executor cores | 5 |
| Executor memory | 30g |

---

## 3. Workload Profile

### 3.1 Monthly Input Volume

| Case | Typical Record Count | Estimated Size | % of Total |
|------|---------------------|----------------|-----------|
| Case I — New Accounts | ~50 M | ~10 GB | ~5% |
| Case II — Forward Entries | ~750 M | ~150 GB | ~90% |¹
| Case III — Backfills | ~200 M | ~40 GB | ~4% |
| Case IV — Bulk Historical | ~100 M | ~20 GB | ~1% |
| **Total monthly increment** | **~1 B** | **~220 GB** | |

¹ Case II dominates record count but is I/O-light (join on latest_summary only).

### 3.2 What the Pipeline Actually Reads Per Run

The pipeline **never** reads the full 8 TB. Thanks to partition pruning and case segmentation:

| Phase | Data Read | How |
|-------|-----------|-----|
| Case II join | ~800 GB | Full scan of `latest_summary` |
| Case III summary read | ~500 GB – 1.2 TB | Partition-pruned: ±36 months around backfill range |
| Case III-B merge target | ~200–400 GB | Only partitions containing future rows |
| Case I + IV | ~0 | No summary reads — write only |
| **Effective read per run** | **~1.5–2 TB** | **~20% of summary table** |

---

## 4. Current Spark Configuration

Defined in `main/config.json` under the `spark` key:

### 4.1 Dynamic Allocation

| Setting | Value |
|---------|-------|
| `spark.dynamicAllocation.enabled` | true |
| `spark.dynamicAllocation.initialExecutors` | 80 |
| `spark.dynamicAllocation.minExecutors` | 80 |
| `spark.dynamicAllocation.maxExecutors` | 120 |
| `spark.executor.cores` | 5 |
| `spark.executor.memory` | 30g |

### 4.2 Adaptive Query Execution (AQE)

| Setting | Value | Impact |
|---------|-------|--------|
| `spark.sql.adaptive.enabled` | true | Auto-optimises joins and partitions |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | Merges small post-shuffle partitions |
| `spark.sql.adaptive.skewJoin.enabled` | true | Splits skewed partitions automatically |
| `spark.sql.adaptive.localShuffleReader.enabled` | true | Avoids network fetch for local shuffle |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 128 MB | Target partition size after coalesce |

### 4.3 Join Strategy

```json
"spark.sql.autoBroadcastJoinThreshold": "-1"
```
All joins go through Sort-Merge Join. The `affected_accounts` broadcast in Case III uses an explicit `F.broadcast()` hint rather than the auto-threshold.

### 4.4 Parquet / File Settings

| Setting | Value | Rationale |
|---------|-------|-----------|
| `spark.sql.files.maxPartitionBytes` | 1 GB | Large input files → fewer, larger tasks |
| `spark.sql.files.openCostInBytes` | 256 MB | Prevents small file overhead |
| `spark.sql.parquet.block.size` | 256 MB | Matches recommended Parquet block for S3 |
| `spark.sql.parquet.datetimeRebaseModeInWrite` | LEGACY | Compatibility with existing Parquet files |

### 4.5 S3 Tuning

| Setting | Value |
|---------|-------|
| `spark.hadoop.fs.s3a.threads.max` | 512 |
| `spark.hadoop.fs.s3a.max.total.tasks` | 512 |
| `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version` | 2 |

Algorithm version 2 eliminates the double-rename commit overhead on S3, significantly reducing write latency.

### 4.6 Iceberg Extensions

```json
"spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
"spark.sql.catalog.primary_catalog": "org.apache.iceberg.spark.SparkCatalog",
"spark.sql.catalog.primary_catalog.type": "glue",
"spark.sql.catalog.primary_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
```

---

## 5. Per-Phase I/O Breakdown

### Case I

| Operation | I/O |
|-----------|-----|
| Source read | Watermark-filtered accounts_all partitions |
| Summary read | None |
| Write | 1 row per new account → case_1 temp |

### Case II

| Operation | I/O |
|-----------|-----|
| Source read | Watermark-filtered increment |
| latest_summary read | Only rows for affected accounts (filter pushdown) |
| Write | 1 row per account → case_2 temp |

### Case III

| Operation | I/O |
|-----------|-----|
| Source read | Watermark-filtered increment |
| Summary read | Partitions within ±36 months of backfill range only |
| Write | case_3a (new rows) + case_3b (future updates) |

**Partition pruning formula:**
```
earliest_partition = min_backfill_month − 36 months
latest_partition   = max_backfill_month + 36 months
```
Reduces I/O by **50–90%** vs a full table scan.

### Case IV

| Operation | I/O |
|-----------|-----|
| Source read | Watermark-filtered increment (new accounts only) |
| Summary read | None |
| Write | case_iv temp (one row per month per new account) |

---

## 6. Current Processing Times

### 6.1 Estimated Phase Breakdown (80–120 executor cluster)

| Phase | Estimated Time | Notes |
|-------|---------------|-------|
| Classification | 5 min | Left join on latest_summary + window |
| Case III processing | 40–50 min | Partition-pruned read + peer_map + write 3a/3b |
| Case I processing | 10 min | No join, direct array init |
| Case IV processing | 45 min | MAP building + GROUP BY + JOIN |
| write_backfill_results | 15–20 min | Chunked MERGE for 3a, 3b; single MERGE for 1+IV |
| Case II processing | 25 min | Filter pushdown on latest_summary |
| write_forward_results | 10 min | Single MERGE with base_ts guard |
| Watermark refresh | <1 min | 3-row MERGE into tracker |
| **TOTAL** | **~2.5–4 hrs** | |

### 6.2 Phase Time Distribution

```
Classification           ██░░░░░░░░░░░░░░░░░░  5 min
Case III processing      ████████████████░░░░░  40-50 min
Case I processing        ████░░░░░░░░░░░░░░░░░  10 min
Case IV processing       ████████████████████░  45 min
write_backfill_results   ██████░░░░░░░░░░░░░░░  15-20 min
Case II processing       ██████████░░░░░░░░░░░  25 min
write_forward_results    ████░░░░░░░░░░░░░░░░░  10 min
                         ─────────────────────
                         TOTAL: ~2.5-4 hrs
```

---

## 7. Bottleneck Analysis

### 7.1 Root Cause: MERGE Write Amplification

Even with MOR (Merge-on-Read) and 512 MB files, `MERGE INTO` in Iceberg still performs **copy-on-write file rewrites** under the hood. The Spark execution for each merge is:

```
1. SCAN target partitions to find matching rows       ← reads ~220 GB per partition
2. SHUFFLE target + source by (cons_acct_key, prt)    ← ~220 GB shuffled
3. MATCH rows (50K matches out of ~500M rows)
4. REWRITE only the matched files                     ← ~50 GB written

Steps 1 and 2 are the bottleneck — reading and shuffling 220 GB
to update 50 MB of actual data.
```

### 7.2 Why Bucketing Alone Doesn't Solve It

The summary table is bucketed into 64 buckets by `cons_acct_key`. However, **Spark's MERGE INTO does not automatically use bucket-aware joins** unless explicitly enabled. Without the bucket-aware configs, Spark:

1. Treats the merge target as a regular table
2. Performs a full sort-merge join with complete shuffle
3. Ignores the bucket structure entirely

### 7.3 Why MOR Doesn't Help Merges

MOR creates **delete files** (position deletes) alongside new data files. This helps reads (log-based deltas), but:

- Spark's `MERGE INTO` still does full COW-style file rewrites
- Delete files accumulate after each run, making subsequent reads progressively slower
- Without compaction, MOR performance **degrades over time**

### 7.4 Current Bottleneck Summary

| Bottleneck | Phase | Current Impact | Root Cause |
|------------|-------|----------------|-----------|
| Full partition shuffle during MERGE | Case III-B write | **45–90 min** | Bucket-aware join not enabled |
| MOR delete file accumulation | All reads after merge | Progressive slowdown | No post-run compaction |
| Oversized cluster (80–120 on-demand) | All phases | **~$800–1,500/run** | Over-provisioned for workload |
| Case IV MAP building | Bulk loads | 45 min | GROUP BY + collect overhead |
| S3 rename overhead | All writes | Minor | Mitigated by committer v2 |

---

## 8. Optimization Plan

### 8.1 Change 1 — Enable Bucket-Aware Merge (Config Only)

Add to `config.json` → `spark` section:

```json
"spark.sql.iceberg.planning.preserve-data-grouping": "true",
"spark.sql.sources.v2.bucketing.enabled": "true",
"spark.sql.sources.v2.bucketing.pushPartValues.enabled": "true",
"spark.sql.requireAllClusterKeysForDistribution": "false"
```

**What it does:** Tells Spark that both source and target are bucketed by `cons_acct_key`. Instead of shuffling the entire 220 GB partition, Spark performs bucket-to-bucket local joins — eliminating the network shuffle.

**Impact:** ~40–50% faster merges. Zero code changes.

### 8.2 Change 2 — Post-Run Compaction for MOR

Add after `refresh_watermark_tracker()` in the pipeline:

```python
# Compact only the partitions touched by this run
for month in affected_months:
    spark.sql(f"""
        CALL system.rewrite_data_files(
            table => '{summary_table}',
            strategy => 'binpack',
            options => map(
                'target-file-size-bytes', '536870912',
                'min-file-size-bytes',    '402653184'
            ),
            where => "rpt_as_of_mo = '{month}'"
        )
    """)

    # Clean up position delete files
    spark.sql(f"""
        CALL system.rewrite_position_delete_files(
            table => '{summary_table}',
            options => map('rewrite-all', 'true'),
            where => "rpt_as_of_mo = '{month}'"
        )
    """)

# Also compact latest_summary (smaller table, full compact)
spark.sql(f"""
    CALL system.rewrite_data_files(
        table => '{latest_summary_table}',
        strategy => 'binpack',
        options => map('target-file-size-bytes', '536870912')
    )
""")

# Expire old snapshots (cleanup S3 storage)
spark.sql(f"""
    CALL system.expire_snapshots(
        table => '{summary_table}',
        older_than => TIMESTAMP '{cutoff_date}',
        retain_last => 3
    )
""")
```

**What it does:** Prevents MOR delete-file accumulation. Merges small files, removes position deletes, and keeps read performance consistent across runs.

**Impact:** Prevents progressive degradation. Adds ~15–20 min to each run but saves cumulative slowdown.

### 8.3 Change 3 — Bucket-Matched Temp Tables

Write temp tables with the same 64-bucket hash on `cons_acct_key` so that MERGE INTO can perform co-partitioned joins:

```python
# When writing case_3b temp table:
case_3b_df.writeTo(temp_table) \
    .tableProperty("write.distribution-mode", "hash") \
    .append()
```

**What it does:** If both source (temp table) and target (summary) have the same bucket structure, Spark can do a co-partitioned merge — zero shuffle on either side.

**Impact:** ~40% additional merge improvement on top of Change 1.

### 8.4 Change 4 — Cluster Right-Sizing

Switch from 80–120 large on-demand instances to a smaller, memory-optimized fleet with spot task nodes.

See [§9 Recommended Cluster Configuration](#9-recommended-cluster-configuration).

**Impact:** ~90% cost reduction.

### 8.5 Change 5 — Executor Tuning

```json
"spark.executor.cores": "5",
"spark.executor.memory": "36g",
"spark.executor.memoryOverhead": "5g",
"spark.dynamicAllocation.minExecutors": "16",
"spark.dynamicAllocation.maxExecutors": "40",
"spark.dynamicAllocation.executorIdleTimeout": "90s"
```

**What it does:** More memory per executor (36g vs 30g) reduces shuffle spill. Lower executor count matches actual I/O needs after bucket-aware merge.

---

## 9. Recommended Cluster Configuration

### 9.1 Proposed Cluster

| Component | Instance | Count | Pricing | Cost/hr |
|-----------|----------|-------|---------|---------|
| Driver | r6g.xlarge (4 vCPU, 32 GB) | 1 | On-Demand | ~$0.25 |
| Core nodes | r6g.2xlarge (8 vCPU, 64 GB) | 8 | On-Demand | ~$4.00 |
| Task nodes | **r8g.4xlarge** (16 vCPU, 128 GB) | up to 8 | **Spot** | ~$2.90–3.80 |
| EMR surcharge | — | — | — | ~$1.50 |
| **Total at peak** | | | | **~$8.65–9.55** |

### 9.2 Executor Layout

**Core nodes (r6g.2xlarge — 8 vCPU, 64 GB):**
```
2 executors per node × 8 nodes = 16 executors (always-on)
  executor.cores = 4
  executor.memory = 26g
  executor.memoryOverhead = 4g
```

**Task nodes (r8g.4xlarge — 16 vCPU, 128 GB):**
```
3 executors per node × 8 nodes = 24 executors (spot burst)
  executor.cores = 5
  executor.memory = 36g
  executor.memoryOverhead = 5g
```

**Total max: 40 executors, ~200 concurrent tasks**

### 9.3 Why r8g.4xlarge Spot for Task Nodes

| Advantage | Detail |
|-----------|--------|
| 128 GB memory per node | Less shuffle spill during merge joins |
| Graviton 4 (ARM) | ~20% better price/performance vs Intel x86 |
| Fewer instances = fewer spot interruptions | 8 large nodes more stable than 30 small ones |
| Spot availability | Larger Graviton instances have better spot capacity |
| Pipeline is rerunnable | Watermark tracker = safe reruns on spot reclamation |

### 9.4 Spot Fleet Diversity

```json
{
  "InstanceFleets": [{
    "Name": "TaskNodes",
    "InstanceFleetType": "TASK",
    "TargetSpotCapacity": 8,
    "LaunchSpecifications": {
      "SpotSpecification": {
        "TimeoutDurationMinutes": 30,
        "AllocationStrategy": "capacity-optimized"
      }
    },
    "InstanceTypeConfigs": [
      {"InstanceType": "r8g.4xlarge", "WeightedCapacity": 1},
      {"InstanceType": "r7g.4xlarge", "WeightedCapacity": 1},
      {"InstanceType": "r6g.4xlarge", "WeightedCapacity": 1},
      {"InstanceType": "r5.4xlarge",  "WeightedCapacity": 1}
    ]
  }]
}
```

Falls back across instance generations if r8g spot isn't available.

### 9.5 Cluster Tiers

| Tier | Core Nodes | Task Nodes (Spot) | Max Executors | Use Case |
|------|-----------|-------------------|---------------|----------|
| Development / Docker | 1 (local) | 0 | 1 | Testing, <1K records |
| **Budget Production** | 6 × r6g.2xlarge | 6 × r8g.4xlarge | 30 | Normal monthly runs |
| **Balanced Production** | 8 × r6g.2xlarge | 8 × r8g.4xlarge | 40 | **Recommended** |
| High Priority / SLA | 10 × r6g.2xlarge | 12 × r8g.4xlarge | 56 | Catch-up runs, large backfills |

---

## 10. Projected Improvements

### 10.1 Per-Phase Comparison

| Phase | Current | After Optimizations | Improvement |
|-------|---------|-------------------|-------------|
| Classification | 5 min | 5 min | — |
| Case III processing | 40–50 min | 30–40 min | ~20% |
| Case I processing | 10 min | 8 min | ~20% |
| Case IV processing | 45 min | 35 min | ~22% |
| **write_backfill_results** | **45–90 min** | **10–20 min** | **⚡ 75–80%** |
| Case II processing | 25 min | 20 min | ~20% |
| write_forward_results | 10 min | 8 min | ~20% |
| Compaction (new) | N/A | 15–20 min | — |
| **TOTAL** | **~2.5–4 hrs** | **~45 min – 1.5 hrs** | **⚡ ~65%** |

### 10.2 Overall Impact Summary

| Metric | Current | After Changes | Improvement |
|--------|---------|---------------|-------------|
| Pipeline runtime | 2.5–4 hrs | **45 min – 1.5 hrs** | ⚡ ~65% faster |
| Total I/O per run | ~2–3 TB | ~600 GB – 1 TB | 📉 ~65% less I/O |
| Case III-B merge (bottleneck) | 45–90 min | 10–20 min | ⚡ ~75–80% faster |
| Cluster cost per run | ~$800–1,500 | **$70–120** | 💰 ~90% cheaper |
| Monthly cost | ~$800–1,500 | **$100–150** | 💰 ~90% cheaper |
| Run-over-run degradation | Gets slower (MOR) | Stable (compaction) | ✅ Consistent |

### 10.3 Cost Breakdown Comparison

| Component | Current Monthly | Proposed Monthly |
|-----------|----------------|-----------------|
| Core nodes (on-demand) | ~$600–900 | ~$50–60 |
| Task nodes | ~$200–600 (on-demand) | ~$25–35 (spot) |
| EMR surcharge | ~$100–200 | ~$15–20 |
| **Total per run** | **~$800–1,500** | **~$70–120** |

---

## 11. Implementation Roadmap

### Priority Order (Effort vs Impact)

| Priority | Change | Effort | Impact | Risk |
|----------|--------|--------|--------|------|
| **🥇 Week 1** | Bucket-aware Spark configs (§8.1) | 3 lines of config | **40–50% faster merges** | Zero |
| **🥇 Week 1** | Executor tuning (§8.5) | 4 lines of config | Right-sized memory | Zero |
| **🥈 Week 2** | Post-run compaction (§8.2) | ~20 lines of code | Prevents MOR degradation | Low |
| **🥉 Week 3** | Cluster right-sizing (§8.4, §9) | EMR config change | **~90% cost reduction** | Low |
| **Week 4** | Bucket-matched temp tables (§8.3) | ~10 lines of code | ~40% more merge improvement | Medium |

### Validation Steps

After each change, measure:

```
1. Total pipeline runtime (end-to-end)
2. Case III-B write_backfill_results time specifically
3. Peak shuffle read/write bytes (Spark UI → SQL tab → shuffle metrics)
4. Number of Iceberg data files rewritten per merge (Iceberg history metadata)
5. S3 GET/PUT request counts (CloudWatch)
```

### Rollback Plan

All changes are independently reversible:

| Change | Rollback |
|--------|----------|
| Bucket-aware configs | Remove 3 lines from config.json |
| Compaction | Skip the compaction step (no side effects) |
| Cluster sizing | Revert EMR fleet config |
| Bucketed temp tables | Revert code to un-bucketed writes |

---

## 12. Write Partition Sizing

```python
def get_write_partitions(spark, config, expected_rows=None, scale_factor=1.0, stage=""):
    # 1. Respect explicit override if set in config
    if "write_partitions" in config:
        return config["write_partitions"]

    # 2. Compute from shuffle/parallelism (cached per run)
    shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
    default_parallelism = spark.sparkContext.defaultParallelism

    # 3. Scale by expected row count (one partition per ~5M rows by default)
    if expected_rows:
        row_based = expected_rows / rows_per_partition * scale_factor
        return max(shuffle_partitions, row_based, parallelism_floor)

    return shuffle_partitions
```

---

## 13. Case III Chunked Merge

Large backfill batches are split into balanced month-chunks before merging. The bin-packing algorithm prevents any single Iceberg commit from becoming too large.

```
Input: month_weights = [(month, weighted_load), ...]
       weighted_load = case3a_count + 1.3 × case3b_count

Algorithm:
  1. Sort months by weighted_load descending
  2. Greedily assign months to chunks
  3. Overflow threshold: target_load × (1 + 0.10)

Example (3 backfill months):
  2025-10: load = 50K + 1.3 × 200K = 310K
  2025-11: load = 30K + 1.3 × 150K = 225K
  2025-12: load = 20K + 1.3 × 100K = 150K

  target = 685K / 2 chunks = 342K
  Chunk 1: [2025-10]         (310K)
  Chunk 2: [2025-11, 2025-12] (375K — within overflow)
```

---

## 14. Memory Profile

### 14.1 Phase Memory Impact

| Phase | Peak Memory Impact | Risk |
|-------|--------------------|------|
| Classification | Medium — latest_summary join | Low |
| Case III peer_map | Medium — windowed MAP per account | Low–Medium |
| Case IV MAP building | Medium — GROUP BY + collect | Low–Medium |
| Case II join | Low — only affected accounts | Low |
| Writes (all cases) | Low — temp table breaks DAG lineage | Low |

### 14.2 Temp Table DAG Lineage Break

Each case writes to an Iceberg temp table before the final MERGE. This breaks Spark DAG lineage after each write, keeping driver memory stable even for billion-record batches.

### 14.3 Memory Improvement with r8g.4xlarge

| Metric | Current (30g) | Proposed (36g) | Impact |
|--------|--------------|----------------|--------|
| Shuffle spill to disk | Frequent during Case III-B | Rare | ~30% faster shuffles |
| Iceberg file rewrite buffer | Constrained | Comfortable | Fewer GC pauses |
| Array column expansion (7 × 36) | Tight fit for wide rows | Adequate headroom | Stable memory usage |

---

## 15. Local Docker Benchmarks

### 15.1 TINY Scale Results

```
================================================================================
PERFORMANCE BENCHMARK RESULTS (main pipeline, TINY scale)
================================================================================
Phase                          Time (s)     Records      Throughput
--------------------------------------------------------------------------------
Data Generation                    ~5s         ~160         ~32/s
Classification                     ~3s         ~160         ~53/s
Case I  (New)                      ~4s          ~50         ~12/s
Case II (Forward)                  ~3s          ~25          ~8/s
Case III (Backfill)                ~6s          ~15          ~2/s
Case IV (Bulk Historical)          ~8s         ~150         ~18/s
Write Results                      ~5s         ~160         ~32/s
--------------------------------------------------------------------------------
TOTAL                             ~34s         ~160         ~4.7/s

Note: TINY scale includes ~2 min Spark startup. Not representative of production.
Expect 50–100x improvement on production cluster with parallelism.
```

---

## 16. Running Benchmarks

```powershell
# Start Docker environment
docker compose -f main/docker_test/docker-compose.yml up -d

# Run benchmark (TINY)
docker compose exec -T spark-iceberg-main python3 /workspace/main/docker_test/tests/run_all_tests.py --include-performance --performance-scale TINY

# Run benchmark (SMALL)
docker compose exec -T spark-iceberg-main python3 /workspace/main/docker_test/tests/run_all_tests.py --include-performance --performance-scale SMALL

# Run benchmark (MEDIUM — takes 20+ min)
docker compose exec -T spark-iceberg-main python3 /workspace/main/docker_test/tests/run_all_tests.py --include-performance --performance-scale MEDIUM
```
