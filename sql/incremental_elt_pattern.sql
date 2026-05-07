/*
  incremental_elt_pattern.sql
  ============================
  Watermark-based incremental ELT — the most important pattern for
  production BigQuery pipelines.

  THE PROBLEM WITH FULL LOADS:
  If you reload the entire source table every day, you're scanning and
  writing TB of data for what might be only a few GB of new records.
  At scale this means: slow pipelines, high costs, SLA breaches.

  THE SOLUTION — INCREMENTAL LOADING:
  1. Record the MAX timestamp we processed last run (the "watermark")
  2. On next run, only read records AFTER that watermark
  3. Merge new records into the target table (upsert — no duplicates)
  4. Update the watermark for the next run

  This pattern is IDEMPOTENT — if the pipeline reruns on the same day,
  it produces the same result. No double-counting, no data loss.

  In production: Airflow passes execution_date as the watermark parameter.
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 1: Watermark tracking table
-- Stores the last successfully processed timestamp per pipeline
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `project.dataset.pipeline_watermarks`
(
  pipeline_name       STRING    NOT NULL,  -- e.g. 'orders_incremental'
  last_processed_ts   TIMESTAMP NOT NULL,  -- watermark: process records AFTER this
  last_run_ts         TIMESTAMP,           -- when did the pipeline itself last run
  records_processed   INT64,               -- how many records were loaded last run
  updated_by          STRING               -- which Airflow DAG updated this
)
OPTIONS (
  description = 'Tracks incremental load watermarks for all ELT pipelines'
);

-- Initialise watermark for a new pipeline (run once on first deploy)
INSERT INTO `project.dataset.pipeline_watermarks`
  (pipeline_name, last_processed_ts, last_run_ts, records_processed, updated_by)
VALUES
  ('orders_incremental', TIMESTAMP('2024-01-01 00:00:00'), CURRENT_TIMESTAMP(), 0, 'manual_init');


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2: Extract only new/changed records from source
-- In production, @last_watermark is injected by Airflow as a query parameter
-- ─────────────────────────────────────────────────────────────────────────────

/*
  Airflow passes execution parameters like this:
    query_params = [
        bigquery.ScalarQueryParameter("last_watermark", "TIMESTAMP", last_watermark_value),
        bigquery.ScalarQueryParameter("current_run_ts", "TIMESTAMP", datetime.utcnow()),
    ]
*/

-- Incremental extract: only records updated since last watermark
CREATE OR REPLACE TEMP TABLE incremental_batch AS
SELECT
  order_id,
  customer_id,
  product_id,
  order_amount,
  order_status,
  order_date,
  region,
  updated_at                             -- source system's last-modified timestamp
FROM `project.dataset.orders_source`
WHERE updated_at > @last_watermark       -- only NEW and CHANGED records
  AND updated_at <= @current_run_ts;     -- upper bound prevents partial-minute races


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 3: MERGE new records into target (upsert pattern)
-- MERGE is BigQuery's atomic upsert — no separate DELETE + INSERT needed.
-- Guarantees consistency even if the pipeline fails halfway through.
-- ─────────────────────────────────────────────────────────────────────────────

MERGE `project.dataset.orders_silver` AS target
USING incremental_batch AS source
ON target.order_id = source.order_id    -- match on business primary key

WHEN MATCHED THEN                       -- record exists → UPDATE changed fields
  UPDATE SET
    target.customer_id   = source.customer_id,
    target.product_id    = source.product_id,
    target.order_amount  = source.order_amount,
    target.order_status  = source.order_status,
    target.order_date    = source.order_date,
    target.region        = source.region,
    target.updated_at    = source.updated_at,
    target._silver_updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED BY TARGET THEN         -- new record → INSERT
  INSERT (
    order_id, customer_id, product_id,
    order_amount, order_status, order_date,
    region, updated_at, _silver_updated_at
  )
  VALUES (
    source.order_id, source.customer_id, source.product_id,
    source.order_amount, source.order_status, source.order_date,
    source.region, source.updated_at, CURRENT_TIMESTAMP()
  );

-- Note: WHEN NOT MATCHED BY SOURCE → omitted intentionally.
-- We never DELETE from Silver — deleted source records are handled via
-- a separate soft-delete flag, preserving historical audit trail.


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 4: Update watermark after successful merge
-- Only run this AFTER the MERGE succeeds — Airflow handles this via task ordering
-- ─────────────────────────────────────────────────────────────────────────────

UPDATE `project.dataset.pipeline_watermarks`
SET
  last_processed_ts = @current_run_ts,
  last_run_ts       = CURRENT_TIMESTAMP(),
  records_processed = (SELECT COUNT(*) FROM incremental_batch),
  updated_by        = 'orders_incremental_dag'
WHERE pipeline_name = 'orders_incremental';


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 5: Reconciliation check — run after every incremental load
-- Compares source record count vs target to catch silent data loss
-- ─────────────────────────────────────────────────────────────────────────────

WITH source_count AS (
  SELECT COUNT(*) AS cnt
  FROM `project.dataset.orders_source`
  WHERE updated_at > @last_watermark
    AND updated_at <= @current_run_ts
),
batch_count AS (
  SELECT COUNT(*) AS cnt FROM incremental_batch
)
SELECT
  source_count.cnt    AS source_records,
  batch_count.cnt     AS loaded_records,
  source_count.cnt - batch_count.cnt AS discrepancy,
  CASE
    WHEN source_count.cnt = batch_count.cnt THEN 'PASS'
    ELSE 'FAIL — investigate before proceeding'
  END AS reconciliation_status
FROM source_count, batch_count;
