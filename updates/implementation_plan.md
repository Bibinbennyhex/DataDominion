# Incremental Summary Update Script (v3)

Scale: **500B+ summary records**, **1B monthly updates**

## Tables

| Table | Description |
|-------|-------------|
| **`default.default.accounts_all`** | Source raw account records |
| **`default.summary_testing`** | Target summary with 36-month history arrays |

---

## Scenarios

### Case I - New `cons_acct_key`
- Create new summary row with NULL history padding
- MERGE to summary

### Case II - Forward Month Entry
- If there are **month gaps** between last entry and new month, pad with NULLs
- Shift history arrays, insert new values at index 0
- MERGE to summary

### Case III - Backfill

#### III.1 - Month Already Exists
- **Only update if `new.base_ts > existing.base_ts`**, else ignore
- If updated, fix all future rows' history arrays

#### III.2 - Single-Gap Fill
- Insert row for missing month
- Update all future rows' history arrays

#### III.3 - Multi-Gap Fill  
- Insert row for missing month
- Update all future rows' history arrays

> [!IMPORTANT]
> Use **MERGE for ALL scenarios** - no APPEND.

---

## Data Flow

```mermaid
flowchart TD
    A[accounts_all] --> B{base_ts > max_summary_ts?}
    B -->|No| X[Skip]
    B -->|Yes| C[Deduplicate per account+month]
    C --> D{Account in summary?}
    D -->|No| E[Case I: New Account]
    D -->|Yes| F{Month comparison}
    F -->|Newer| G[Case II: Forward]
    F -->|Older| H[Case III: Backfill]
    G --> G1{Gap exists?}
    G1 -->|Yes| G2[Pad NULLs for gaps]
    G1 -->|No| G3[Shift arrays]
    G2 --> G3
    H --> H1{Month exists?}
    H1 -->|Yes| H2{new.base_ts > existing?}
    H2 -->|Yes| H3[Update + fix future]
    H2 -->|No| X2[Skip]
    H1 -->|No| H4[Insert + fix future]
    E --> M[MERGE]
    G3 --> M
    H3 --> M
    H4 --> M
```

---

## Scalability

1. **Partition pruning**: Filter by `rpt_as_of_mo`
2. **Broadcast joins**: For filtered account lists
3. **Native Spark array ops**: No `applyInPandas`
4. **Iceberg MERGE**: Atomic upserts

---

## Verification

1. Test all 3 cases + sub-scenarios
2. Verify future rows updated correctly on backfill
3. Performance target: <10 min for 1B updates
