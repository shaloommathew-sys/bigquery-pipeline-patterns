/*
  data_quality_checks.sql
  ========================
  BigQuery-native data quality checks — run between pipeline layers.

  PHILOSOPHY:
  Data quality checks are NOT optional. In production, a bad record
  that passes through undetected corrupts Gold layer aggregations,
  breaks dashboards, and erodes stakeholder trust.

  These checks run as Airflow tasks BETWEEN pipeline stages:
    Bronze ingestion → [DQ Check] → Silver cleaning → [DQ Check] → Gold aggregation

  If a CRITICAL check fails, Airflow stops the DAG and sends an alert.
  If a WARNING check fails, the DAG continues but an alert is sent.

  PATTERN: All checks write results to a quality_log table so you
  have a historical audit trail of data quality over time.
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- SETUP: Quality log table — stores results of all DQ checks
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `project.dataset.dq_check_log`
(
  check_id          STRING    NOT NULL,   -- UUID generated per run
  pipeline_name     STRING,
  table_name        STRING,
  check_name        STRING,
  severity          STRING,               -- CRITICAL / WARNING / INFO
  status            STRING,               -- PASSED / FAILED
  expected_value    STRING,
  actual_value      STRING,
  message           STRING,
  run_date          DATE,
  checked_at        TIMESTAMP
)
PARTITION BY run_date
OPTIONS (
  description = 'Audit log of all data quality check results across pipelines'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- CHECK 1: Table not empty (CRITICAL)
-- A completely empty table almost always means upstream pipeline failure
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO `project.dataset.dq_check_log`
WITH row_count AS (
  SELECT COUNT(*) AS cnt
  FROM `project.dataset.orders_silver`
  WHERE _ingestion_date = DATE(@run_date)
)
SELECT
  GENERATE_UUID()                                      AS check_id,
  'orders_incremental'                                 AS pipeline_name,
  'orders_silver'                                      AS table_name,
  'not_empty'                                          AS check_name,
  'CRITICAL'                                           AS severity,
  CASE WHEN cnt > 0 THEN 'PASSED' ELSE 'FAILED' END   AS status,
  '> 0 rows'                                           AS expected_value,
  CAST(cnt AS STRING)                                  AS actual_value,
  CASE
    WHEN cnt > 0 THEN CONCAT('Row count: ', cnt)
    ELSE 'CRITICAL: Table is empty — upstream pipeline likely failed'
  END                                                  AS message,
  DATE(@run_date)                                      AS run_date,
  CURRENT_TIMESTAMP()                                  AS checked_at
FROM row_count;


-- ─────────────────────────────────────────────────────────────────────────────
-- CHECK 2: No nulls in primary key (CRITICAL)
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO `project.dataset.dq_check_log`
WITH null_check AS (
  SELECT COUNT(*) AS null_count
  FROM `project.dataset.orders_silver`
  WHERE _ingestion_date = DATE(@run_date)
    AND order_id IS NULL
)
SELECT
  GENERATE_UUID()                                                AS check_id,
  'orders_incremental'                                           AS pipeline_name,
  'orders_silver'                                                AS table_name,
  'order_id_not_null'                                            AS check_name,
  'CRITICAL'                                                     AS severity,
  CASE WHEN null_count = 0 THEN 'PASSED' ELSE 'FAILED' END      AS status,
  '0 nulls'                                                      AS expected_value,
  CONCAT(null_count, ' nulls')                                   AS actual_value,
  CONCAT('Null order_id count: ', null_count)                    AS message,
  DATE(@run_date)                                                AS run_date,
  CURRENT_TIMESTAMP()                                            AS checked_at
FROM null_check;


-- ─────────────────────────────────────────────────────────────────────────────
-- CHECK 3: No duplicate primary keys (CRITICAL)
-- Duplicates in Silver cause row multiplication (fan-out) in Gold joins
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO `project.dataset.dq_check_log`
WITH dup_check AS (
  SELECT
    COUNT(*)                    AS total_rows,
    COUNT(DISTINCT order_id)    AS unique_keys
  FROM `project.dataset.orders_silver`
  WHERE _ingestion_date = DATE(@run_date)
)
SELECT
  GENERATE_UUID()                                                    AS check_id,
  'orders_incremental'                                               AS pipeline_name,
  'orders_silver'                                                    AS table_name,
  'no_duplicate_order_ids'                                           AS check_name,
  'CRITICAL'                                                         AS severity,
  CASE WHEN total_rows = unique_keys THEN 'PASSED' ELSE 'FAILED' END AS status,
  '0 duplicates'                                                     AS expected_value,
  CONCAT(total_rows - unique_keys, ' duplicates')                    AS actual_value,
  CONCAT(
    'Total: ', total_rows, ' | Unique keys: ', unique_keys,
    ' | Duplicates: ', total_rows - unique_keys
  )                                                                  AS message,
  DATE(@run_date)                                                    AS run_date,
  CURRENT_TIMESTAMP()                                                AS checked_at
FROM dup_check;


-- ─────────────────────────────────────────────────────────────────────────────
-- CHECK 4: Row count drift vs previous day (WARNING)
-- A >30% drop in daily volume usually means a source system issue
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO `project.dataset.dq_check_log`
WITH today AS (
  SELECT COUNT(*) AS cnt
  FROM `project.dataset.orders_silver`
  WHERE _ingestion_date = DATE(@run_date)
),
yesterday AS (
  SELECT COUNT(*) AS cnt
  FROM `project.dataset.orders_silver`
  WHERE _ingestion_date = DATE_SUB(DATE(@run_date), INTERVAL 1 DAY)
),
drift AS (
  SELECT
    today.cnt                         AS today_count,
    yesterday.cnt                     AS yesterday_count,
    SAFE_DIVIDE(
      ABS(today.cnt - yesterday.cnt),
      yesterday.cnt
    ) * 100                           AS pct_change
  FROM today, yesterday
)
SELECT
  GENERATE_UUID()                                                      AS check_id,
  'orders_incremental'                                                 AS pipeline_name,
  'orders_silver'                                                      AS table_name,
  'row_count_drift'                                                    AS check_name,
  'WARNING'                                                            AS severity,
  CASE WHEN pct_change <= 30 OR yesterday_count = 0
       THEN 'PASSED' ELSE 'FAILED' END                                 AS status,
  '<= 30% change vs yesterday'                                         AS expected_value,
  CONCAT(ROUND(pct_change, 1), '% change')                            AS actual_value,
  CONCAT(
    'Today: ', today_count, ' | Yesterday: ', yesterday_count,
    ' | Change: ', ROUND(pct_change, 1), '%'
  )                                                                    AS message,
  DATE(@run_date)                                                      AS run_date,
  CURRENT_TIMESTAMP()                                                  AS checked_at
FROM drift;


-- ─────────────────────────────────────────────────────────────────────────────
-- CHECK 5: Valid values check (WARNING)
-- Catches upstream enum changes before they corrupt Gold aggregations
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO `project.dataset.dq_check_log`
WITH invalid_status AS (
  SELECT COUNT(*) AS cnt
  FROM `project.dataset.orders_silver`
  WHERE _ingestion_date = DATE(@run_date)
    AND order_status NOT IN ('COMPLETED', 'PENDING', 'CANCELLED')
    AND order_status IS NOT NULL
)
SELECT
  GENERATE_UUID()                                                  AS check_id,
  'orders_incremental'                                             AS pipeline_name,
  'orders_silver'                                                  AS table_name,
  'order_status_valid_values'                                      AS check_name,
  'WARNING'                                                        AS severity,
  CASE WHEN cnt = 0 THEN 'PASSED' ELSE 'FAILED' END               AS status,
  'Only: COMPLETED, PENDING, CANCELLED'                            AS expected_value,
  CONCAT(cnt, ' records with unexpected status')                   AS actual_value,
  CONCAT('Unexpected status count: ', cnt,
    ' — upstream system may have added a new status value')        AS message,
  DATE(@run_date)                                                  AS run_date,
  CURRENT_TIMESTAMP()                                              AS checked_at
FROM invalid_status;


-- ─────────────────────────────────────────────────────────────────────────────
-- UTILITY: Daily DQ summary — paste in BigQuery console to review results
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
  check_name,
  severity,
  status,
  expected_value,
  actual_value,
  message,
  checked_at
FROM `project.dataset.dq_check_log`
WHERE run_date = CURRENT_DATE()
ORDER BY
  CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,
  CASE status   WHEN 'FAILED'   THEN 1 ELSE 2 END;
