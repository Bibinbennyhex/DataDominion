# Performance Analysis - Summary Pipeline v9.4

## Version Comparison

| Version | Case I | Case II | Case III | Case IV | Gap Handling | Optimizations | Status |
|---------|--------|---------|----------|---------|--------------|---------------|--------|
| v9.0 | OK | OK | Broken | N/A | N/A | None | Deprecated |
| v9.1 | OK | OK | Fixed | N/A | N/A | None | Deprecated |
| v9.2 | OK | OK | Fixed | Added | Broken | None | Deprecated |
| v9.2.1 | OK | OK | Fixed | Fixed | Fixed | None | Deprecated |
| v9.3 | OK | OK | Fixed | Fixed | Fixed | Partition pruning, Single MERGE, Filter pushdown | Deprecated |
| v9.4 | OK | OK | Fixed | Fixed | Fixed | Temp table processing, DAG lineage breaking | **Current** |

## v9.4 Optimizations

### 1. Temp Table Processing (Breaking DAG Lineage)
- **Problem**: v9.3 held all case results in memory until final MERGE, causing driver OOM for large batches
- **Solution**: Each case writes to temporary Iceberg table, breaking DAG lineage
- **Impact**: Significantly reduced driver memory pressure, improved reliability

### 2. Unique Batch ID Generation
- **Format**: `{schema}.pipeline_batch_{timestamp}_{uuid}`
- **Example**: `demo.temp.pipeline_batch_20260131_143052_a1b2c3d4`
- **Purpose**: No collision between concurrent runs

### 3. Automatic Cleanup (try/finally)
- **Implementation**: Temp table wrapped in try/finally block
- **Behavior**: Temp table dropped on success AND failure
- **Safety**: No orphaned temp tables under normal operation

### 4. Orphan Cleanup Utility
- **Function**: `cleanup_orphaned_temp_tables(spark, config, max_age_hours=24)`
- **Purpose**: Clean up temp tables from crashed/killed runs
- **CLI**: `python summary_pipeline.py --cleanup-orphans --max-age-hours 24`

## v9.3 Optimizations (Retained in v9.4)

### 1. Case III Partition Pruning (50-90% I/O Reduction)
- **Problem**: v9.2 read entire 50B+ row summary table for backfill
- **Solution**: Added partition filter based on backfill month range (±36 months)
- **Impact**: Reads only relevant partitions instead of full table scan

### 2. Single Consolidated MERGE (4x Less Write Amplification)
- **Problem**: 4 separate MERGE operations (one per case type)
- **Solution**: Collect all results, union, deduplicate by priority, single MERGE
- **Impact**: 4x reduction in write I/O, better Iceberg commit efficiency

### 3. Case II Filter Pushdown (Iceberg Data File Pruning)
- **Problem**: Case II loaded entire latest_summary table, then filtered
- **Solution**: SQL subquery pushes filter to Iceberg
- **Impact**: Reads only data files containing affected accounts

## Workload Profile

| Table | Records | Size |
|-------|---------|------|
| Summary | 50B | ~10 TB |
| Latest Summary | 4B | ~800 GB |
| Backfill Input | 200M | ~40 GB |
| Forward Input | 750M | ~150 GB |
| New Accounts | 50M | ~10 GB |
| Bulk Historical | 100M | ~20 GB |

## Time Estimates (50 node cluster)

| Phase | v9.0 | v9.1 | v9.2 | v9.2.1 | v9.3 | v9.4 |
|-------|------|------|------|--------|------|------|
| Classification | 5 min | 5 min | 5 min | 5 min | 5 min | 5 min |
| Case I (50M new) | 10 min | 10 min | 10 min | 10 min | 10 min | 10 min |
| Case II (750M forward) | 30 min | 30 min | 30 min | 30 min | 25 min | 25 min |
| Case III (200M backfill) | ~65 min (buggy) | 75 min | 75 min | 75 min | **40-50 min** | **40-50 min** |
| Case IV (100M bulk) | N/A | N/A | 40 min | 45 min | 45 min | 45 min |
| Write Results | 15 min | 15 min | 15 min | 15 min | **10 min** | **12 min** |
| **TOTAL** | **~2 hrs** | **~2.3 hrs** | **~2.9 hrs** | **~3 hrs** | **~2.5 hrs** | **~2.5 hrs** |

Note: v9.4 adds ~2 min I/O overhead for temp table write/read but gains reliability.

### v9.4 Performance Characteristics

| Metric | v9.3 (In-Memory) | v9.4 (Temp Table) |
|--------|------------------|-------------------|
| Driver Memory | High (holds DAG) | Low (lineage broken) |
| I/O Operations | Single write | Multiple writes + read |
| Total Time | Baseline | +2-5% (I/O overhead) |
| Reliability | OOM risk for large batches | Stable |
| Max Batch Size | ~500M records | 1B+ records |

### v9.3 Performance Gains (Retained)

| Optimization | Time Saved | Mechanism |
|--------------|------------|-----------|
| Case III partition pruning | 25-35 min | 50-90% less I/O |
| Single MERGE | 5-8 min | 4x less write amplification |
| Case II filter pushdown | 3-5 min | Iceberg data file pruning |
| **Total** | **30-50 min** | **~30% faster** |

## Case IV Performance Details

### Algorithm Complexity

| Step | Operation | Complexity |
|------|-----------|------------|
| 1. Build MAPs | GROUP BY account, COLLECT_LIST per column | O(accounts × columns) |
| 2. Join MAPs | Records JOIN Maps | O(records) |
| 3. Build Arrays | TRANSFORM per column | O(records × columns × 36) |

### Memory Usage

| Component | Memory | Notes |
|-----------|--------|-------|
| Account MAPs | ~5GB per 1M accounts | 36-month × 7 columns × avg 20 bytes |
| Broadcast threshold | 500MB | MAPs broadcast if under threshold |
| Executor overhead | ~20% | For MAP lookups |

### Benchmarks (Docker Local - v9.4)

| Scale | Records | Total Pipeline Time | Throughput |
|-------|---------|---------------------|------------|
| TINY | ~160 | 4.67 min | 0.6/s |
| SMALL | ~5K | ~8 min | ~10/s |
| MEDIUM | ~100K | ~20 min | ~80/s |

Note: Local Docker benchmarks include Spark startup overhead (~2 min).
Expect 10-50x improvement on production cluster due to parallelism.

### Test Results (v9.4 - All Pass)

```
test_all_scenarios.py:           25/25 PASSED
test_comprehensive_50_cases.py:  52/52 PASSED
```

## Memory Requirements

| Phase | Peak Memory | Risk |
|-------|-------------|------|
| Classification | ~200 GB | Low |
| Case I | ~100 GB | Low |
| Case II | ~375 GB | Medium |
| Case III | ~150 GB | Low |
| Case IV (COLLECT_LIST) | ~300 GB | Medium |
| Case IV (MAP approach) | ~400 GB | Medium |

## Cluster Sizing Guide

### Small (Development/Testing)
- 10 × r6i.4xlarge
- Runtime: 8+ hours
- Cost: ~$80/run

### Standard (Production)
- 20 core (On-Demand) + 30 task (Spot) r6i.8xlarge
- Runtime: ~3 hours
- Cost: ~$150/run

### Fast (SLA Critical)
- 40 × r6i.12xlarge (On-Demand)
- Runtime: ~2 hours
- Cost: ~$200/run

## Key Configuration Values

| Setting | Value | Why |
|---------|-------|-----|
| `shuffle.partitions` | 8192 | 1B records / 5M per partition |
| `executor.memory` | 45g | Leaves room for overhead |
| `autoBroadcastJoinThreshold` | 500m | Broadcast latest_summary meta |
| `cache_level` | MEMORY_AND_DISK | Prevents OOM |
| `dynamicAllocation.maxExecutors` | 250 | Scale up for large batches |

## Case IV Specific Settings

| Setting | Value | Reason |
|---------|-------|--------|
| `spark.sql.shuffle.partitions` | 8192 | High for MAP operations |
| `spark.sql.adaptive.enabled` | true | Auto-optimize shuffle |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | Reduce small partitions |

## I/O Throughput

| Resource | Capacity | Time to Read 10TB |
|----------|----------|-------------------|
| 50 nodes × 500 MB/s | 25 GB/s | ~7 min |
| S3 write | 10 GB/s | ~3 min for 2TB |

## Bottleneck Analysis

1. **Case IV MAP Building**: GROUP BY with COLLECT_LIST for each account
2. **Case III Backfill**: Summary table scan for affected months
3. **Shuffle**: 8192 partitions with 50B record table
4. **MERGE Write**: Iceberg merge-on-read overhead

## Optimization Opportunities

1. **Partition summary table by month**: Reduce scan for backfill
2. **Pre-aggregate backfill**: Group multiple backfills before processing
3. **Increase broadcast threshold**: If memory allows, broadcast more
4. **Use Iceberg bucketing**: On `cons_acct_key` for faster joins
5. **Batch bulk historical**: Process large bulk loads in separate runs

## Running Performance Benchmarks

```bash
# Tiny scale (quick test)
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_performance_benchmark.py --scale TINY

# Small scale (development)
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_performance_benchmark.py --scale SMALL

# Medium scale (integration)
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_performance_benchmark.py --scale MEDIUM

# Large scale (stress test)
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_performance_benchmark.py --scale LARGE
```

## Expected Benchmark Output

```
================================================================================
PERFORMANCE BENCHMARK RESULTS
================================================================================
Phase                          Time (s)     Records      Throughput     
--------------------------------------------------------------------------------
Data Generation                    12.34s      5,000        405.2/s
Classification                      1.23s      5,000      4,065.0/s
Case I (New)                        2.15s        500        232.6/s
Case II (Forward)                   1.89s        250        132.3/s
Case III (Backfill)                 2.45s        150         61.2/s
Case IV (Bulk)                      4.67s      4,100        878.0/s
Write Results                       3.21s      5,000      1,557.6/s
--------------------------------------------------------------------------------
TOTAL (processing)                 15.60s      5,000        320.5/s

PRODUCTION EXTRAPOLATION (single node)
----------------------------------------
  50M new accounts: ~2,604.2 minutes
  200M backfill: ~10,416.7 minutes
  ...
```

Note: Production extrapolation is linear from single-node benchmarks. 
Actual cluster performance scales near-linearly with node count.
