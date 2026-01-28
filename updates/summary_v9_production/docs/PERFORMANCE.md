# Performance Analysis - Summary Pipeline v9.1

## Workload Profile

| Table | Records | Size |
|-------|---------|------|
| Summary | 50B | ~10 TB |
| Latest Summary | 4B | ~800 GB |
| Backfill Input | 200M | ~40 GB |
| Forward Input | 750M | ~150 GB |
| New Accounts | 50M | ~10 GB |

## Version Comparison

### Time Estimates (50 node cluster)

| Phase | v4/v5 | v8 | v9 (bug) | v9.1 Fixed |
|-------|-------|----|----|------------|
| Classification | 5 min | 5 min | 5 min | 5 min |
| Case III (200M backfill) | 160 min | 145 min | 65 min | 75 min |
| Case I (50M new) | 10 min | 10 min | 10 min | 10 min |
| Case II (750M forward) | 45 min | 40 min | 30 min | 30 min |
| **TOTAL** | **~4 hrs** | **~3.7 hrs** | **~1.8 hrs** | **~2 hrs** |

### Why v9.1 is Faster than v4/v5/v8

1. **No COLLECT_LIST + EXPLODE**: Old versions collected all months into memory per account
2. **Simple transform()**: Uses `transform(array, (x,i) -> IF(i=pos, new, x))` instead of rebuilding
3. **Fewer table scans**: 3 scans vs 4+ scans
4. **Better broadcast usage**: Configurable threshold (10M rows)

### Case III Backfill - Algorithm Comparison

| Approach | v4/v5/v8 | v9.1 Fixed |
|----------|----------|------------|
| Method | 7-CTE SQL with COLLECT_LIST | 3-part (new rows + updates + union) |
| Memory | High (collect all months) | Low (join only) |
| Table Scans | 4+ | 3 |
| Complexity | O(accounts × months × 36) | O(records × 36) |

## Memory Requirements

| Phase | Peak Memory | Risk |
|-------|-------------|------|
| Classification | ~200 GB (broadcast latest_summary meta) | Low |
| Case III v4/v5/v8 | ~500+ GB (COLLECT_LIST) | **HIGH OOM** |
| Case III v9.1 | ~150 GB (joins only) | Low |
| Case II | ~375 GB | Medium |

## Cluster Sizing Guide

### Small (Development/Testing)
- 10 × r6i.4xlarge
- Runtime: 8+ hours
- Cost: ~$80/run

### Standard (Production)
- 20 core (On-Demand) + 30 task (Spot) r6i.8xlarge
- Runtime: ~2 hours
- Cost: ~$130/run

### Fast (SLA Critical)
- 40 × r6i.12xlarge (On-Demand)
- Runtime: ~1.5 hours
- Cost: ~$180/run

## Key Configuration Values

| Setting | Value | Why |
|---------|-------|-----|
| `shuffle.partitions` | 8192 | 1B records / 5M per partition |
| `executor.memory` | 45g | Leaves room for overhead |
| `autoBroadcastJoinThreshold` | 500m | Broadcast latest_summary meta |
| `cache_level` | MEMORY_AND_DISK | Prevents OOM |
| `dynamicAllocation.maxExecutors` | 250 | Scale up for large batches |

## I/O Throughput

| Resource | Capacity | Time to Read 10TB |
|----------|----------|-------------------|
| 50 nodes × 500 MB/s | 25 GB/s | ~7 min |
| S3 write | 10 GB/s | ~3 min for 2TB |

## Bottleneck Analysis

1. **Case III Backfill**: Most expensive due to summary table scan
2. **Shuffle**: 8192 partitions with 50B record table
3. **MERGE write**: Iceberg merge-on-read overhead

## Optimization Opportunities

1. **Partition summary table by month**: Reduce scan for backfill
2. **Pre-aggregate backfill**: Group multiple backfills before processing
3. **Increase broadcast threshold**: If memory allows, broadcast more
4. **Use Iceberg bucketing**: On `cons_acct_key` for faster joins
