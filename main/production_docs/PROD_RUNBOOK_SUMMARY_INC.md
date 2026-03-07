# Summary Incremental Pipeline Production Runbook

## Scope

This checklist is for running `main/summary_inc.py` safely against production Iceberg tables.

Target tables from current `main/config.json`:

- Source: `spark_catalog.edf_gold.ivaps_consumer_accounts_all`
- Hist rpt: `primary_catalog.edf_gold.consumer_account_hist_rpt`
- Summary: `primary_catalog.edf_gold.summary`
- Latest summary: `primary_catalog.edf_gold.latest_summary`
- Watermark tracker: `primary_catalog.edf_gold.summary_watermark_tracker`

## 1) Preflight (must pass)

Run these in Spark SQL (or equivalent).

```sql
-- 1. Confirm no in-flight run.
SELECT source_name, status, run_id, updated_at
FROM primary_catalog.edf_gold.summary_watermark_tracker
WHERE source_name IN ('summary', 'latest_summary', 'committed_ingestion_watermark')
ORDER BY updated_at DESC;
```

Pass criteria:
- No `RUNNING` status for active run.

```sql
-- 2. Confirm required columns exist.
DESCRIBE TABLE primary_catalog.edf_gold.summary;
DESCRIBE TABLE primary_catalog.edf_gold.latest_summary;
```

Pass criteria:
- Both include `soft_del_cd`.

```sql
-- 3. Capture rollback points (save these snapshot IDs externally).
SELECT snapshot_id, committed_at, operation
FROM primary_catalog.edf_gold.summary.snapshots
ORDER BY committed_at DESC
LIMIT 5;

SELECT snapshot_id, committed_at, operation
FROM primary_catalog.edf_gold.latest_summary.snapshots
ORDER BY committed_at DESC
LIMIT 5;
```

```sql
-- 4. Baseline key integrity checks (before run).
SELECT COUNT(*) AS summary_rows FROM primary_catalog.edf_gold.summary;
SELECT COUNT(*) AS latest_rows FROM primary_catalog.edf_gold.latest_summary;

SELECT COUNT(*) AS dup_summary_keys
FROM (
  SELECT cons_acct_key, rpt_as_of_mo, COUNT(*) c
  FROM primary_catalog.edf_gold.summary
  GROUP BY cons_acct_key, rpt_as_of_mo
  HAVING COUNT(*) > 1
);

SELECT COUNT(*) AS dup_latest_keys
FROM (
  SELECT cons_acct_key, COUNT(*) c
  FROM primary_catalog.edf_gold.latest_summary
  GROUP BY cons_acct_key
  HAVING COUNT(*) > 1
);
```

Pass criteria:
- `dup_summary_keys = 0`
- `dup_latest_keys = 0`

## 2) Execution Guardrails

- Run only one pipeline instance at a time.
- Disable overlapping schedules during run window.
- Keep the saved pre-run snapshot IDs for both tables.

Example run:

```bash
spark-submit main/summary_inc.py \
  --config '{"bucket_name":"<bucket>","key":"<config-key>"}' \
  --mode incremental
```

## 3) Post-run Validation (must pass)

```sql
-- 1. Tracker status should be SUCCESS for latest run.
SELECT source_name, status, run_id, updated_at, max_base_ts, max_rpt_as_of_mo, error_message
FROM primary_catalog.edf_gold.summary_watermark_tracker
WHERE source_name IN ('summary', 'latest_summary', 'committed_ingestion_watermark')
ORDER BY updated_at DESC;
```

Pass criteria:
- Latest run status is `SUCCESS` for summary/latest/committed rows.

```sql
-- 2. Key integrity checks after run.
SELECT COUNT(*) AS dup_summary_keys
FROM (
  SELECT cons_acct_key, rpt_as_of_mo, COUNT(*) c
  FROM primary_catalog.edf_gold.summary
  GROUP BY cons_acct_key, rpt_as_of_mo
  HAVING COUNT(*) > 1
);

SELECT COUNT(*) AS dup_latest_keys
FROM (
  SELECT cons_acct_key, COUNT(*) c
  FROM primary_catalog.edf_gold.latest_summary
  GROUP BY cons_acct_key
  HAVING COUNT(*) > 1
);
```

Pass criteria:
- `dup_summary_keys = 0`
- `dup_latest_keys = 0`

```sql
-- 3. latest_summary consistency against summary (active rows only).
WITH summary_latest AS (
  SELECT cons_acct_key, rpt_as_of_mo, base_ts
  FROM (
    SELECT
      cons_acct_key,
      rpt_as_of_mo,
      base_ts,
      ROW_NUMBER() OVER (
        PARTITION BY cons_acct_key
        ORDER BY rpt_as_of_mo DESC, base_ts DESC
      ) AS rn
    FROM primary_catalog.edf_gold.summary
    WHERE COALESCE(soft_del_cd, '') NOT IN ('1', '4')
  ) x
  WHERE rn = 1
),
latest_rows AS (
  SELECT cons_acct_key, rpt_as_of_mo, base_ts
  FROM primary_catalog.edf_gold.latest_summary
)
SELECT
  (SELECT COUNT(*) FROM summary_latest) AS expected_latest,
  (SELECT COUNT(*) FROM latest_rows) AS actual_latest,
  (SELECT COUNT(*) FROM (
      SELECT s.*
      FROM summary_latest s
      LEFT ANTI JOIN latest_rows l
      ON s.cons_acct_key = l.cons_acct_key
     AND s.rpt_as_of_mo = l.rpt_as_of_mo
     AND s.base_ts = l.base_ts
  )) AS missing_in_latest,
  (SELECT COUNT(*) FROM (
      SELECT l.*
      FROM latest_rows l
      LEFT ANTI JOIN summary_latest s
      ON s.cons_acct_key = l.cons_acct_key
     AND s.rpt_as_of_mo = l.rpt_as_of_mo
     AND s.base_ts = l.base_ts
  )) AS extra_in_latest;
```

Pass criteria:
- `missing_in_latest = 0`
- `extra_in_latest = 0`

```sql
-- 4. Optional: latest should not point to deleted rows.
SELECT COUNT(*) AS latest_deleted_rows
FROM primary_catalog.edf_gold.latest_summary
WHERE COALESCE(soft_del_cd, '') IN ('1', '4');
```

Pass criteria:
- `latest_deleted_rows = 0`

## 4) Rollback Playbook

If post-checks fail, rollback both tables to captured pre-run snapshots.

```sql
-- Replace IDs with captured pre-run snapshot IDs.
CALL primary_catalog.system.rollback_to_snapshot(
  table => 'edf_gold.summary',
  snapshot_id => <SUMMARY_PRE_RUN_SNAPSHOT_ID>
);

CALL primary_catalog.system.rollback_to_snapshot(
  table => 'edf_gold.latest_summary',
  snapshot_id => <LATEST_PRE_RUN_SNAPSHOT_ID>
);
```

Then re-check:

```sql
SELECT snapshot_id, committed_at
FROM primary_catalog.edf_gold.summary.snapshots
ORDER BY committed_at DESC
LIMIT 3;

SELECT snapshot_id, committed_at
FROM primary_catalog.edf_gold.latest_summary.snapshots
ORDER BY committed_at DESC
LIMIT 3;
```

## 5) Final Go/No-Go

Go only if:
- Tracker shows `SUCCESS`.
- Duplicate key checks are zero.
- Latest-vs-summary consistency checks are zero mismatch.
- Rollback snapshot IDs were captured and validated.
