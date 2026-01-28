# Summary Pipeline v9 - Test Cases Documentation

## Overview

This document details all test cases with their **before** and **after** states for:
- `accounts` table (input)
- `summary` table (before/after)
- `latest_summary` table (before/after)

Each test validates specific pipeline behavior for Case I, II, or III processing.

---

## Test Case Index

| # | Test Name | Case | Description |
|---|-----------|------|-------------|
| 1 | `case_i_simple_new_account` | I | New account with no existing summary |
| 2 | `case_i_multiple_new_accounts` | I | Multiple new accounts in batch |
| 3 | `case_i_null_values` | I | Sentinel values (-2147483647) |
| 4 | `case_ii_normal_forward` | II | Forward 1 month |
| 5 | `case_ii_gap_forward` | II | Forward with 3-month gap |
| 6 | `case_ii_max_gap` | II | Forward with 35-month gap |
| 7 | `case_iii_simple_backfill` | III | Backfill previous month |
| 8 | `case_iii_deep_backfill` | III | Backfill 12 months ago |
| 9 | `case_iii_edge_of_window` | III | Backfill at position 35 |
| 10 | `case_iii_outside_window` | III | Backfill beyond 36-month window |
| 11 | `case_iii_multiple_backfill` | III | Multiple backfills for same account |
| 12 | `mixed_all_three_cases` | Mixed | Batch with I, II, III |
| 13 | `mixed_duplicate_accounts` | Mixed | Deduplication test |
| 14 | `edge_empty_batch` | Edge | Empty input |
| 15 | `edge_year_boundary` | Edge | Dec to Jan transition |
| 16 | `edge_large_values` | Edge | Max integer values |

---

## Notation

- `[A, B, C, ...]` = Array with 36 elements (showing first few)
- `NULL` or `?` = No value at that position
- `bal_0` = Balance at position 0 (current month)
- `bal_1` = Balance at position 1 (1 month ago)
- Position in array = months ago from latest month

---

# CASE I - NEW ACCOUNTS

## Test 1: `case_i_simple_new_account`

**Description:** Brand new account, no existing summary record.

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_am | payment_am | credit_limit_am |
|---------------|--------------|---------|------------|------------|-----------------|
| 1001 | 2025-12 | 2025-12-15 10:00:00 | 5000 | 500 | 10000 |

### BEFORE: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| *(empty)* | | |

### BEFORE: latest_summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| *(empty)* | | |

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 1001 | 2025-12 | [5000, NULL, NULL, ..., NULL] |

### AFTER: latest_summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 1001 | 2025-12 | [5000, NULL, NULL, ..., NULL] |

### Expected Behavior
- New array created with 36 elements
- Current value at position 0
- All other positions NULL

---

## Test 2: `case_i_multiple_new_accounts`

**Description:** 5 new accounts processed in single batch.

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_am |
|---------------|--------------|---------|------------|
| 2001 | 2025-12 | 2025-12-15 10:00:00 | 1000 |
| 2002 | 2025-12 | 2025-12-15 10:00:00 | 2000 |
| 2003 | 2025-12 | 2025-12-15 10:00:00 | 3000 |
| 2004 | 2025-12 | 2025-12-15 10:00:00 | 4000 |
| 2005 | 2025-12 | 2025-12-15 10:00:00 | 5000 |

### BEFORE: summary table
*(empty)*

### BEFORE: latest_summary table
*(empty)*

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history[0] |
|---------------|--------------|-------------------|
| 2001 | 2025-12 | 1000 |
| 2002 | 2025-12 | 2000 |
| 2003 | 2025-12 | 3000 |
| 2004 | 2025-12 | 4000 |
| 2005 | 2025-12 | 5000 |

### AFTER: latest_summary table
| cons_acct_key | rpt_as_of_mo | balance_history[0] |
|---------------|--------------|-------------------|
| 2001 | 2025-12 | 1000 |
| 2002 | 2025-12 | 2000 |
| 2003 | 2025-12 | 3000 |
| 2004 | 2025-12 | 4000 |
| 2005 | 2025-12 | 5000 |

### Expected Behavior
- All 5 accounts processed as Case I
- Each gets independent array
- Record count = 5

---

## Test 3: `case_i_null_values`

**Description:** New account with sentinel values (-2147483647, -1) that should be converted to NULL.

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | balance_am | payment_am | credit_limit_am |
|---------------|--------------|------------|------------|-----------------|
| 3001 | 2025-12 | -2147483647 | -1 | 10000 |

### BEFORE: summary table
*(empty)*

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history[0] | payment_history[0] | credit_limit_history[0] |
|---------------|--------------|-------------------|-------------------|------------------------|
| 3001 | 2025-12 | NULL | NULL | 10000 |

### Expected Behavior
- Sentinel values converted to NULL in array
- Valid values preserved
- Grid shows "?" for NULL positions

---

# CASE II - FORWARD ENTRIES

## Test 4: `case_ii_normal_forward`

**Description:** Account with existing November summary, now processing December (1-month forward).

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_am |
|---------------|--------------|---------|------------|
| 4001 | 2025-12 | 2025-12-15 10:00:00 | 6000 |

### BEFORE: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 4001 | 2025-11 | [5000, 4500, 4000, NULL, ..., NULL] |

### BEFORE: latest_summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 4001 | 2025-11 | [5000, 4500, 4000, NULL, ..., NULL] |

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 4001 | 2025-11 | [5000, 4500, 4000, NULL, ..., NULL] |
| 4001 | 2025-12 | **[6000, 5000, 4500, 4000, NULL, ..., NULL]** |

### AFTER: latest_summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 4001 | 2025-12 | **[6000, 5000, 4500, 4000, NULL, ..., NULL]** |

### Expected Behavior
- New value prepended to array
- Previous values shifted right by 1
- Array truncated to 36 elements
- latest_summary updated to December

---

## Test 5: `case_ii_gap_forward`

**Description:** Account with September summary, now processing December (3-month gap).

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_am |
|---------------|--------------|---------|------------|
| 5001 | 2025-12 | 2025-12-15 10:00:00 | 7000 |

### BEFORE: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 5001 | 2025-09 | [5000, 4500, NULL, ..., NULL] |

### BEFORE: latest_summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 5001 | 2025-09 | [5000, 4500, NULL, ..., NULL] |

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 5001 | 2025-09 | [5000, 4500, NULL, ..., NULL] |
| 5001 | 2025-12 | **[7000, NULL, NULL, 5000, 4500, NULL, ..., NULL]** |

### AFTER: latest_summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 5001 | 2025-12 | **[7000, NULL, NULL, 5000, 4500, NULL, ..., NULL]** |

### Expected Behavior
- New value at position 0
- NULL inserted for Oct (pos 1) and Nov (pos 2)
- Previous values shifted to positions 3, 4, ...
- MONTH_DIFF = 3

---

## Test 6: `case_ii_max_gap`

**Description:** Account with Jan 2023 summary, now processing Dec 2025 (35-month gap - edge of 36-month window).

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_am |
|---------------|--------------|---------|------------|
| 6001 | 2025-12 | 2025-12-15 10:00:00 | 8000 |

### BEFORE: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 6001 | 2023-01 | [1000, NULL, ..., NULL] |

### BEFORE: latest_summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 6001 | 2023-01 | [1000, NULL, ..., NULL] |

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 6001 | 2023-01 | [1000, NULL, ..., NULL] |
| 6001 | 2025-12 | **[8000, NULL×34, 1000]** |

### AFTER: latest_summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 6001 | 2025-12 | **[8000, NULL×34, 1000]** |

### Expected Behavior
- New value at position 0
- 34 NULLs for missing months
- Old value at position 35 (last valid position)
- MONTH_DIFF = 35

---

# CASE III - BACKFILL

## Test 7: `case_iii_simple_backfill`

**Description:** Account has December summary, now backfilling November data (position 1).

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_am |
|---------------|--------------|---------|------------|
| 7001 | 2025-11 | 2025-12-20 10:00:00 | 5500 |

*Note: timestamp is NEWER than existing, but month is OLDER*

### BEFORE: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 7001 | 2025-12 | [6000, **NULL**, 4500, NULL, ..., NULL] |

### BEFORE: latest_summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 7001 | 2025-12 | [6000, **NULL**, 4500, NULL, ..., NULL] |

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 7001 | 2025-12 | [6000, **5500**, 4500, NULL, ..., NULL] |

### AFTER: latest_summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 7001 | 2025-12 | [6000, **5500**, 4500, NULL, ..., NULL] |

### Expected Behavior
- Position 1 updated from NULL to 5500
- Position 0 and 2+ unchanged
- No new row created - existing row updated
- rpt_as_of_mo stays 2025-12

---

## Test 8: `case_iii_deep_backfill`

**Description:** Account has Jan 2026 summary, now backfilling Jan 2025 data (position 12).

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_am |
|---------------|--------------|---------|------------|
| 8001 | 2025-01 | 2026-01-20 10:00:00 | 3000 |

### BEFORE: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 8001 | 2026-01 | [6000, NULL×11, **NULL**, NULL×23] |

### BEFORE: latest_summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 8001 | 2026-01 | [6000, NULL×11, **NULL**, NULL×23] |

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 8001 | 2026-01 | [6000, NULL×11, **3000**, NULL×23] |

### AFTER: latest_summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 8001 | 2026-01 | [6000, NULL×11, **3000**, NULL×23] |

### Expected Behavior
- Position 12 updated from NULL to 3000
- All other positions unchanged
- backfill_position = 12

---

## Test 9: `case_iii_edge_of_window`

**Description:** Backfill at position 35 (last valid position in 36-month window).

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_am |
|---------------|--------------|---------|------------|
| 9001 | 2023-02 | 2026-01-20 10:00:00 | 1000 |

*Note: Feb 2023 is 35 months before Jan 2026*

### BEFORE: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 9001 | 2026-01 | [6000, NULL×34, **NULL**] |

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 9001 | 2026-01 | [6000, NULL×34, **1000**] |

### Expected Behavior
- Position 35 (last) updated
- backfill_position = 35
- This is the OLDEST month that can be backfilled

---

## Test 10: `case_iii_outside_window`

**Description:** Backfill attempt for data older than 36-month window - should be SKIPPED.

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_am |
|---------------|--------------|---------|------------|
| 10001 | 2022-12 | 2026-01-20 10:00:00 | 500 |

*Note: Dec 2022 is 37 months before Jan 2026*

### BEFORE: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 10001 | 2026-01 | [6000, NULL×35] |

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 10001 | 2026-01 | [6000, NULL×35] *(unchanged)* |

### Expected Behavior
- Record SKIPPED (backfill_position >= 36)
- No changes to summary
- Log message: "No valid backfill records (all outside 36-month window)"

---

## Test 11: `case_iii_multiple_backfill`

**Description:** Multiple backfill records for same account, different months.

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_am |
|---------------|--------------|---------|------------|
| 11001 | 2025-11 | 2025-12-20 10:00:00 | 5500 |
| 11001 | 2025-10 | 2025-12-20 10:00:00 | 5000 |
| 11001 | 2025-09 | 2025-12-20 10:00:00 | 4500 |

### BEFORE: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 11001 | 2025-12 | [6000, NULL, NULL, NULL, 4000, NULL, ...] |

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 11001 | 2025-12 | [6000, **5500**, **5000**, **4500**, 4000, NULL, ...] |

### Expected Behavior
- Positions 1, 2, 3 all updated
- Each backfill applied independently
- Original value at position 4 preserved

---

# MIXED SCENARIOS

## Test 12: `mixed_all_three_cases`

**Description:** Single batch containing Case I, II, and III records.

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_am | Case |
|---------------|--------------|---------|------------|------|
| 12001 | 2025-12 | 2025-12-15 10:00:00 | 5000 | I (new) |
| 12002 | 2025-12 | 2025-12-15 10:00:00 | 7000 | II (forward) |
| 12003 | 2025-10 | 2025-12-20 10:00:00 | 4000 | III (backfill) |

### BEFORE: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 12002 | 2025-11 | [6000, NULL, ...] |
| 12003 | 2025-12 | [5000, NULL, 3000, ...] |

### BEFORE: latest_summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 12002 | 2025-11 | [6000, NULL, ...] |
| 12003 | 2025-12 | [5000, NULL, 3000, ...] |

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history | Notes |
|---------------|--------------|-----------------|-------|
| 12001 | 2025-12 | [5000, NULL, ...] | Case I: New |
| 12002 | 2025-11 | [6000, NULL, ...] | Original |
| 12002 | 2025-12 | [7000, 6000, NULL, ...] | Case II: Forward |
| 12003 | 2025-12 | [5000, NULL, **4000**, 3000, ...] | Case III: Backfill pos 2 |

### AFTER: latest_summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 12001 | 2025-12 | [5000, NULL, ...] |
| 12002 | 2025-12 | [7000, 6000, NULL, ...] |
| 12003 | 2025-12 | [5000, NULL, **4000**, 3000, ...] |

### Expected Processing Order
1. **Case III (Backfill)** - 12003 processed FIRST
2. **Case I (New)** - 12001 processed SECOND
3. **Case II (Forward)** - 12002 processed LAST

---

## Test 13: `mixed_duplicate_accounts`

**Description:** Same account appears multiple times with same month - keep latest timestamp.

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_am |
|---------------|--------------|---------|------------|
| 13001 | 2025-12 | 2025-12-15 **08:00:00** | 5000 |
| 13001 | 2025-12 | 2025-12-15 **12:00:00** | 5500 |
| 13001 | 2025-12 | 2025-12-15 **10:00:00** | 5200 |

### BEFORE: summary table
*(empty)*

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history[0] | base_ts |
|---------------|--------------|-------------------|---------|
| 13001 | 2025-12 | **5500** | 2025-12-15 12:00:00 |

### Expected Behavior
- Only 1 record written (deduplicated)
- Record with latest timestamp (12:00:00) wins
- Balance = 5500 (from 12:00:00 record)

---

# EDGE CASES

## Test 14: `edge_empty_batch`

**Description:** Empty input - no accounts to process.

### Input: accounts table
*(empty)*

### BEFORE: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 99999 | 2025-12 | [1000, ...] |

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 99999 | 2025-12 | [1000, ...] *(unchanged)* |

### Expected Behavior
- No processing occurs
- stats.total_records = 0
- stats.records_written = 0
- No errors

---

## Test 15: `edge_year_boundary`

**Description:** Forward from December 2025 to January 2026 - validates year boundary math.

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_am |
|---------------|--------------|---------|------------|
| 15001 | 2026-01 | 2026-01-15 10:00:00 | 6000 |

### BEFORE: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 15001 | 2025-12 | [5000, ...] |

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 15001 | 2025-12 | [5000, ...] |
| 15001 | 2026-01 | [6000, 5000, ...] |

### Expected Behavior
- MONTH_DIFF = 1 (not 89!)
- Year boundary handled correctly
- month_int formula: (YEAR * 12) + MONTH

---

## Test 16: `edge_large_values`

**Description:** Maximum integer values - test for overflow.

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | balance_am | payment_am | credit_limit_am |
|---------------|--------------|------------|------------|-----------------|
| 16001 | 2025-12 | 2147483646 | 2147483646 | 2147483646 |

### BEFORE: summary table
*(empty)*

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history[0] | payment_history[0] |
|---------------|--------------|-------------------|-------------------|
| 16001 | 2025-12 | 2147483646 | 2147483646 |

### Expected Behavior
- Large values preserved without overflow
- No truncation or wrap-around
- Array functions handle max int correctly

---

# ADDITIONAL TEST CASES TO CONSIDER

## Test 17: `case_ii_forward_uses_backfill_result`

**Description:** Forward processing should use CORRECTED history from backfill (processing order validation).

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_am | Case |
|---------------|--------------|---------|------------|------|
| 17001 | 2025-11 | 2025-12-20 10:00:00 | 5500 | III (backfill) |
| 17001 | 2026-01 | 2026-01-15 10:00:00 | 7000 | II (forward) |

### BEFORE: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 17001 | 2025-12 | [6000, **NULL**, 4500, ...] |

### Processing Steps:
1. **Backfill first:** Position 1 gets 5500
2. **Forward second:** Uses [6000, 5500, 4500, ...]

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 17001 | 2025-12 | [6000, 5500, 4500, ...] |
| 17001 | 2026-01 | [**7000**, 6000, **5500**, 4500, ...] |

### Expected Behavior
- Position 2 in Jan 2026 = 5500 (from backfill)
- NOT NULL (if forward ran before backfill)
- Validates correct processing order

---

## Test 18: `case_iii_overwrite_existing_value`

**Description:** Backfill where position already has a value - should it overwrite?

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_am |
|---------------|--------------|---------|------------|
| 18001 | 2025-11 | 2025-12-20 10:00:00 | 5500 |

### BEFORE: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 18001 | 2025-12 | [6000, **5000**, 4500, ...] |

*Note: Position 1 already has value 5000*

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 18001 | 2025-12 | [6000, **5500**, 4500, ...] |

### Expected Behavior
- Backfill OVERWRITES existing value
- New timestamp is newer, so update wins
- Position 1: 5000 → 5500

---

## Test 19: `case_iii_older_timestamp_loses`

**Description:** Backfill with OLDER timestamp than what created the existing value - should NOT overwrite.

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_am |
|---------------|--------------|---------|------------|
| 19001 | 2025-11 | 2025-11-10 10:00:00 | 4000 |

*Note: Timestamp 2025-11-10 is OLDER than existing summary timestamp*

### BEFORE: summary table
| cons_acct_key | rpt_as_of_mo | base_ts | balance_history |
|---------------|--------------|---------|-----------------|
| 19001 | 2025-12 | 2025-12-15 10:00:00 | [6000, 5500, 4500, ...] |

### AFTER: summary table
| cons_acct_key | rpt_as_of_mo | balance_history |
|---------------|--------------|-----------------|
| 19001 | 2025-12 | [6000, 5500, 4500, ...] *(unchanged)* |

### Expected Behavior
- Record SKIPPED (older timestamp)
- Existing value preserved
- **QUESTION:** Does current implementation handle this?

---

## Test 20: `payment_history_grid_generation`

**Description:** Validate payment_history_grid string is generated correctly from array.

### Input: accounts table
| cons_acct_key | rpt_as_of_mo | days_past_due | asset_class_cd |
|---------------|--------------|---------------|----------------|
| 20001 | 2025-12 | 0 | A |

### BEFORE: latest_summary
| cons_acct_key | payment_rating_history | payment_history_grid |
|---------------|----------------------|---------------------|
| 20001 | ['0', '1', NULL, '2', ...] | 01?2... |

### AFTER: latest_summary (after forward)
| cons_acct_key | payment_rating_history | payment_history_grid |
|---------------|----------------------|---------------------|
| 20001 | ['0', '0', '1', NULL, '2', ...] | 001?2... |

### Expected Behavior
- Grid string = concat of payment_rating with '?' for NULL
- Length = 36 characters
- Separator = '' (empty)

---

# SUMMARY OF TEST COVERAGE

## Covered Cases

| Category | Count | Status |
|----------|-------|--------|
| Case I (New) | 3 | ✓ |
| Case II (Forward) | 3 | ✓ |
| Case III (Backfill) | 5 | ✓ |
| Mixed | 2 | ✓ |
| Edge Cases | 4 | ✓ |
| **Total** | **17** | |

## Potential Gaps

| Gap | Description | Priority |
|-----|-------------|----------|
| Timestamp comparison | Does backfill check if its timestamp is newer? | HIGH |
| Overwrite vs skip | When position already has value, what happens? | HIGH |
| Processing order | Validated that forward uses backfill result? | HIGH |
| Concurrent backfills | Same position, different timestamps | MEDIUM |
| All arrays | Tests focus on balance_history, test all 8? | MEDIUM |
| Payment rating logic | DPD → payment_rating_cd mapping | LOW |

---

# NEXT STEPS

1. Review this document for accuracy
2. Identify any missing test cases
3. Clarify expected behavior for gaps
4. Update pipeline code if needed
5. Implement automated tests for all scenarios
