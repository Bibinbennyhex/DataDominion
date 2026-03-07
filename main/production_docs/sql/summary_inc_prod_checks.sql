-- Summary Incremental Pipeline Production Checks
-- Target namespace: edf_gold
-- Catalog: primary_catalog (summary/latest/tracker), spark_catalog (source)

-- ============================================================
-- SECTION A: PREFLIGHT
-- ============================================================

-- A1) Confirm no in-flight run.
SELECT source_name, status, run_id, updated_at
FROM primary_catalog.edf_gold.summary_watermark_tracker
WHERE source_name IN ('summary', 'latest_summary', 'committed_ingestion_watermark')
ORDER BY updated_at DESC;

-- A2) Confirm required column exists in both tables.
DESCRIBE TABLE primary_catalog.edf_gold.summary;
DESCRIBE TABLE primary_catalog.edf_gold.latest_summary;

-- A3) Capture rollback points (save externally before run).
SELECT snapshot_id, committed_at, operation
FROM primary_catalog.edf_gold.summary.snapshots
ORDER BY committed_at DESC
LIMIT 5;

SELECT snapshot_id, committed_at, operation
FROM primary_catalog.edf_gold.latest_summary.snapshots
ORDER BY committed_at DESC
LIMIT 5;

-- A4) Baseline row counts.
SELECT COUNT(*) AS summary_rows
FROM primary_catalog.edf_gold.summary;

SELECT COUNT(*) AS latest_rows
FROM primary_catalog.edf_gold.latest_summary;

-- A5) Key uniqueness checks.
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

-- ============================================================
-- SECTION B: POST-RUN VALIDATION
-- ============================================================

-- B1) Tracker status for latest run.
SELECT source_name, status, run_id, updated_at, max_base_ts, max_rpt_as_of_mo, error_message
FROM primary_catalog.edf_gold.summary_watermark_tracker
WHERE source_name IN ('summary', 'latest_summary', 'committed_ingestion_watermark')
ORDER BY updated_at DESC;

-- B2) Key uniqueness checks after run.
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

-- B3) latest_summary consistency vs latest active summary row per account.
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

-- B4) Optional: latest should not contain deleted rows.
SELECT COUNT(*) AS latest_deleted_rows
FROM primary_catalog.edf_gold.latest_summary
WHERE COALESCE(soft_del_cd, '') IN ('1', '4');

-- ============================================================
-- SECTION C: ROLLBACK (RUN ONLY IF NEEDED)
-- ============================================================

-- Replace snapshot IDs below with pre-run IDs captured in A3.
-- CALL primary_catalog.system.rollback_to_snapshot(
--   table => 'edf_gold.summary',
--   snapshot_id => <SUMMARY_PRE_RUN_SNAPSHOT_ID>
-- );
--
-- CALL primary_catalog.system.rollback_to_snapshot(
--   table => 'edf_gold.latest_summary',
--   snapshot_id => <LATEST_PRE_RUN_SNAPSHOT_ID>
-- );

-- Verify current snapshots after rollback.
SELECT snapshot_id, committed_at, operation
FROM primary_catalog.edf_gold.summary.snapshots
ORDER BY committed_at DESC
LIMIT 3;

SELECT snapshot_id, committed_at, operation
FROM primary_catalog.edf_gold.latest_summary.snapshots
ORDER BY committed_at DESC
LIMIT 3;
