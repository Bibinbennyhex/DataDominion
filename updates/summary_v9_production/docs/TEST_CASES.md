# Test Cases Documentation - Summary Pipeline v9.3

## Test Files Overview

| File | Tests | Purpose |
|------|-------|---------|
| `test_all_scenarios.py` | 21 | Core functionality - all 4 cases + gaps |
| `test_comprehensive_edge_cases.py` | 40+ | Extended edge cases and boundaries |
| `test_bulk_historical_load.py` | 15 | 72-month bulk historical scenario |
| `test_performance_benchmark.py` | N/A | Performance measurement at scale |
| `run_backfill_test.py` | 5 | Case III backfill cascade |

## v9.3 Test Verification

All tests pass with v9.3 optimizations. Log evidence:

```
Applied partition filter: rpt_as_of_mo BETWEEN '2022-11' AND '2028-11'
Using SQL subquery for filter pushdown (Iceberg optimization)
WRITING ALL RESULTS (single MERGE operation)
```

---

## Test Suite 1: test_all_scenarios.py (21 tests)

### Purpose
Validates all 4 case types in a single integrated test run with gap handling.

### Test Accounts

| Account | Case | Scenario |
|---------|------|----------|
| 1001 | I | New account, single month (Jan 2026) |
| 2001 | II | Existing account, forward entry (Feb → Mar 2026) |
| 3001 | III | Existing account, backfill (Nov 2025 arrives late) |
| 4001 | IV | New account, 12 months bulk (Jan-Dec 2025) |
| 5001 | IV + GAPS | New account, 9 months with 3 gaps |

### Case I Tests (Account 1001)
| # | Test | Expected |
|---|------|----------|
| 1 | bal[0] = current month value | 3000 |
| 2 | bal[1] = NULL (no prior data) | NULL |

### Case II Tests (Account 2001)
| # | Test | Expected |
|---|------|----------|
| 3 | Mar 2026 bal[0] = current month | 5500 |
| 4 | Mar 2026 bal[1] = Feb 2026 (shifted) | 5000 |
| 5 | Mar 2026 bal[2] = Jan 2026 (shifted) | 4500 |
| 6 | latest_summary updated to Mar 2026 | 2026-03 |

### Case III Tests (Account 3001)
| # | Test | Expected |
|---|------|----------|
| 7 | Total rows = 3 (Nov, Dec, Jan) | 3 |
| 8 | Nov 2025 row CREATED | Row exists |
| 9 | Nov 2025 bal[0] = backfill value | 8500 |
| 10 | Jan 2026 bal[2] = Nov 2025 (backfilled) | 8500 |

### Case IV Tests (Account 4001)
| # | Test | Expected |
|---|------|----------|
| 11 | Total rows = 12 | 12 |
| 12 | Jan 2025 bal[0] = first month | 10000 |
| 13 | Jan 2025 bal[1] = NULL | NULL |
| 14 | Mar 2025 bal[0] = Mar value | 9000 |
| 15 | Mar 2025 bal[1] = Feb value | 9500 |
| 16 | Mar 2025 bal[2] = Jan value | 10000 |
| 17 | Dec 2025 bal[0] = Dec value | 4500 |
| 18 | Dec 2025 bal[11] = Jan value | 10000 |

### Case IV + Gaps Tests (Account 5001)
| # | Test | Expected |
|---|------|----------|
| 19 | Total rows = 9 (gaps not reported) | 9 |
| 20 | Mar 2025 bal[1] = NULL (Feb gap) | NULL |
| 21 | Mar 2025 bal[2] = Jan value | 6000 |

### How to Run
```bash
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_all_scenarios.py
```

---

## Test Suite 2: test_comprehensive_edge_cases.py (40+ tests)

### Purpose
Validates edge cases, boundary conditions, and unusual data patterns.

### Test Categories

#### Case I Edge Cases (Accounts 6001-6003)
| Account | Scenario | Tests |
|---------|----------|-------|
| 6001 | Minimal values (balance=1) | Value preserved correctly |
| 6002 | Maximum values (INT_MAX=2147483647) | No overflow/truncation |
| 6003 | NULL values in optional columns | NULLs propagated to arrays |

#### Case II Edge Cases (Accounts 7001-7002)
| Account | Scenario | Tests |
|---------|----------|-------|
| 7001 | Skip 1 month (Feb → April) | March shows as NULL at bal[1] |
| 7002 | Skip 5 months (Jan → July) | Months 2-6 show as NULL |

#### Case III Edge Cases (Accounts 8001-8002)
| Account | Scenario | Tests |
|---------|----------|-------|
| 8001 | Deep backfill (12 months back) | Value appears at bal[12] |
| 8002 | Multiple backfills in one batch | Both values correctly positioned |

#### Case IV Edge Cases (Accounts 9001-9006)
| Account | Scenario | Tests |
|---------|----------|-------|
| 9001 | Full 36 months (exactly fills array) | bal[35] has first month |
| 9002 | 48 months (exceeds 36) | Correct 36-month window |
| 9003 | Only 2 months (minimal bulk) | bal[1] has first month |
| 9004 | Every-other-month pattern | Alternating NULL values |
| 9005 | First and last only (35-month gap) | bal[1-34] all NULL |
| 9006 | Mixed pattern (consecutive then gap) | Gap in middle correctly NULL |

### Detailed Test Cases

#### 6002: Maximum Integer Values
```
Input: balance_am = 2147483647 (INT_MAX)
Expected: bal_history[0] = 2147483647 (no overflow)
```

#### 7001: Skip 1 Month (Gap in Forward)
```
Existing: Feb 2026 summary
Input: April 2026 (skip March)

Expected April array:
  bal[0] = 5200 (April)
  bal[1] = NULL (March - gap!)
  bal[2] = 5000 (Feb - shifted from existing)
```

#### 9004: Alternating Gaps
```
Input: Jan, Mar, May, Jul, Sep, Nov 2025 (Feb, Apr, Jun, Aug, Oct missing)

Expected Mar 2025 array:
  bal[0] = 5700 (Mar)
  bal[1] = NULL (Feb - gap!)
  bal[2] = 5900 (Jan)
```

#### 9005: Extreme Gap (35 months)
```
Input: Jan 2023 and Dec 2025 only (35 months apart)

Expected Dec 2025 array:
  bal[0] = 8000 (Dec 2025)
  bal[1-34] = NULL (gaps)
  bal[35] = 10000 (Jan 2023)
```

### How to Run
```bash
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_comprehensive_edge_cases.py
```

---

## Test Suite 3: test_bulk_historical_load.py (15 tests)

### Purpose
Validates 72-month bulk historical upload scenario.

### Test Account
- **Account 9001**: 72 months of history (Jan 2020 - Dec 2025)

### Tests
| # | Test | Expected |
|---|------|----------|
| 1 | Total rows created | 72 |
| 2 | Jan 2020 (first) bal[0] | 10000 |
| 3 | Jan 2020 bal[1] | NULL |
| 4 | Feb 2020 bal[0] | 9900 |
| 5 | Feb 2020 bal[1] | 10000 |
| 6 | Dec 2020 (12th) bal[11] | 10000 |
| 7 | Dec 2021 (24th) bal[23] | 10000 |
| 8 | Dec 2022 (36th) bal[35] | 10000 |
| 9 | Jan 2023 (37th) bal[35] | Value (Jan 2020 rolled off) |
| 10 | Dec 2025 (72nd) bal[0] | Latest value |
| 11 | Dec 2025 bal[35] | 36 months back value |
| 12 | Array length = 36 | All arrays have exactly 36 elements |
| 13 | Grid string length = 36 | Payment grid is 36 characters |
| 14 | latest_summary has Dec 2025 | Correct |
| 15 | All rows in summary table | 72 |

### How to Run
```bash
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_bulk_historical_load.py
```

---

## Test Suite 4: test_performance_benchmark.py

### Purpose
Measures pipeline throughput at various data scales.

### Scales
| Scale | New (Case I) | Forward (Case II) | Backfill (Case III) | Bulk (Case IV) | Total Records |
|-------|--------------|-------------------|---------------------|----------------|---------------|
| TINY | 50 | 25 | 15 | ~150 | ~200 |
| SMALL | 500 | 250 | 150 | ~3,000 | ~5,000 |
| MEDIUM | 5,000 | 2,500 | 1,500 | ~50,000 | ~100,000 |
| LARGE | 50,000 | 25,000 | 15,000 | ~500,000 | ~1,000,000 |

### Metrics Collected
- Data generation time
- Classification time
- Case I processing time
- Case II processing time
- Case III processing time
- Case IV processing time
- Write time
- Total throughput (records/second)

### Expected Output
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
```

### How to Run
```bash
# Quick test
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_performance_benchmark.py --scale TINY

# Development
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_performance_benchmark.py --scale SMALL

# Integration
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_performance_benchmark.py --scale MEDIUM

# Stress test
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_performance_benchmark.py --scale LARGE
```

---

## Test Suite 5: run_backfill_test.py (5 tests)

### Purpose
Validates Case III backfill cascade (updates to future months).

### Test Account
- **Account 3001**: Has Dec 2025 and Jan 2026, receives Nov 2025 backfill

### Tests
| # | Test | Expected |
|---|------|----------|
| 1 | Nov 2025 row CREATED | Row exists |
| 2 | Nov 2025 bal[0] | Backfill value |
| 3 | Dec 2025 bal[1] = Nov 2025 | Propagated |
| 4 | Jan 2026 bal[2] = Nov 2025 | Propagated |
| 5 | Total rows = 3 | Correct |

### How to Run
```bash
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/run_backfill_test.py
```

---

## Running All Tests

### Sequential (Recommended)
```bash
# Run each test suite one at a time
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_all_scenarios.py
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_comprehensive_edge_cases.py
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_bulk_historical_load.py
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_performance_benchmark.py --scale SMALL
```

### Quick Validation
```bash
# Just run the core all-scenarios test
docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_all_scenarios.py
```

---

## Test Data Cleanup

Each test creates its own isolated database:
- `demo.all_cases_test` - test_all_scenarios.py
- `demo.edge_case_test` - test_comprehensive_edge_cases.py
- `demo.bulk_test` - test_bulk_historical_load.py
- `demo.perf_test` - test_performance_benchmark.py
- `demo.backfill_test` - run_backfill_test.py

Tables are automatically dropped and recreated at the start of each test run.

---

## Expected Results Summary

| Test File | Tests | Expected | v9.3 Result |
|-----------|-------|----------|-------------|
| test_all_scenarios.py | 21 | 21 PASSED | **21 PASSED** |
| test_comprehensive_edge_cases.py | 40+ | All PASSED | All PASSED |
| test_bulk_historical_load.py | 15 | 15 PASSED | 15 PASSED |
| test_performance_benchmark.py | N/A | Completes | Completes (4.67 min TINY) |
| run_backfill_test.py | 5 | 5 PASSED | 5 PASSED |

**Total: 80+ tests, all passing on v9.3**

### Latest Test Run (January 2026)

```
================================================================================
FINAL RESULTS: 21 passed, 0 failed
================================================================================

*** ALL TESTS PASSED ***

Pipeline Duration: 4.67 minutes
Records Written: 173
```
