/*
  clustering_optimisation.sql
  ============================
  BigQuery clustering patterns and cost reduction techniques.

  WHAT IS CLUSTERING?
  After partitioning divides a table into date shards,
  clustering sorts the data WITHIN each partition by the columns you specify.
  BigQuery uses this sort order to skip blocks of data that don't match your filter.

  PARTITIONING vs CLUSTERING:
  - Partitioning eliminates entire partition shards (coarse grain)
  - Clustering eliminates data blocks within a partition (fine grain)
  - They work together — partition first, cluster second

  WHEN TO CLUSTER:
  - High-cardinality filter columns (region, status, product_id)
  - Columns frequently used in WHERE, JOIN ON, or GROUP BY
  - Up to 4 clustering columns per table (order matters — most selective first)

  COST IMPACT:
  In our Deutsche Bank migration, adding clustering to the main transaction table
  reduced average query bytes scanned by ~40% → direct cost reduction.
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- PATTERN 1: PARTITION + CLUSTER TOGETHER (most common production pattern)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `project.dataset.orders_optimised`
(
  order_id        STRING    NOT NULL,
  customer_id     STRING,
  product_id      STRING,
  order_amount    NUMERIC,
  order_status    STRING,
  order_date      DATE,
  region          STRING,
  product_category STRING
)
PARTITION BY order_date                            -- coarse filter: by day
CLUSTER BY region, order_status, product_category  -- fine filter: within each day
                                                   -- order: most commonly filtered first
OPTIONS (
  require_partition_filter = TRUE
);

/*
  QUERY THAT BENEFITS FROM BOTH:

    SELECT SUM(order_amount)
    FROM orders_optimised
    WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31'  -- uses partition
      AND region = 'SOUTH'                                   -- uses cluster col 1
      AND order_status = 'COMPLETED'                         -- uses cluster col 2

  BigQuery will:
    1. Skip all partitions outside Jan 2024
    2. Within Jan 2024, skip blocks where region != 'SOUTH'
    3. Within SOUTH blocks, skip rows where status != 'COMPLETED'
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- PATTERN 2: MATERIALIZED VIEW for repeated aggregations (Gold layer pattern)
-- Instead of running expensive aggregations on every dashboard query,
-- pre-compute them and let BigQuery auto-refresh when source changes.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS `project.dataset.mv_daily_revenue_by_region`
OPTIONS (
  enable_refresh = TRUE,
  refresh_interval_minutes = 60            -- BigQuery auto-refreshes every hour
)
AS
SELECT
  order_date,
  region,
  order_status,
  COUNT(order_id)          AS total_orders,
  SUM(order_amount)        AS total_revenue,
  AVG(order_amount)        AS avg_order_value
FROM `project.dataset.orders_optimised`
WHERE order_status = 'COMPLETED'
GROUP BY order_date, region, order_status;

/*
  Dashboard queries now hit the materialized view instead of the base table.
  BQ automatically serves from the MV cache when the query matches.
  Result: millisecond dashboard loads instead of full table scans.
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- PATTERN 3: QUERY COST ESTIMATION before running expensive queries
-- Always do this before running a new query on a large table
-- ─────────────────────────────────────────────────────────────────────────────

/*
  Step 1: In BigQuery console, click "More" → "Query settings" → enable dry run
  OR use bq CLI:

    bq query --dry_run --use_legacy_sql=false '
      SELECT SUM(order_amount)
      FROM project.dataset.orders_optimised
      WHERE order_date BETWEEN "2024-01-01" AND "2024-12-31"
        AND region = "SOUTH"
    '

  Output shows bytes that WILL be scanned → you can estimate cost before running.
  At $5/TB scanned, scanning 100GB = $0.50. Scanning 1TB = $5.00.
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- PATTERN 4: TABLE STATISTICS — understand your data distribution
-- Run before designing clustering to pick the right columns
-- ─────────────────────────────────────────────────────────────────────────────

-- Check cardinality of candidate clustering columns
SELECT
  'region'         AS column_name,
  COUNT(DISTINCT region)         AS distinct_values
FROM `project.dataset.orders_optimised`

UNION ALL

SELECT
  'order_status',
  COUNT(DISTINCT order_status)
FROM `project.dataset.orders_optimised`

UNION ALL

SELECT
  'product_category',
  COUNT(DISTINCT product_category)
FROM `project.dataset.orders_optimised`;

/*
  INTERPRETING RESULTS:
  - region: 4 distinct values → low cardinality → good first cluster column
    (eliminates large chunks of data quickly)
  - order_status: 3 values → very low cardinality → good second cluster column
  - product_category: 50 values → medium cardinality → good third cluster column

  AVOID clustering on:
  - order_id (millions of distinct values — clustering on unique IDs doesn't help)
  - free-text columns like customer_name
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- PATTERN 5: IDENTIFY expensive queries in your project
-- Use this to find queries causing cost spikes — run in BigQuery console
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
  job_id,
  user_email,
  ROUND(total_bytes_processed / POW(1024, 3), 2)    AS gb_processed,
  ROUND(total_bytes_billed    / POW(1024, 3), 2)    AS gb_billed,
  ROUND(total_slot_ms / 1000.0, 1)                  AS slot_seconds,
  creation_time,
  SUBSTR(query, 1, 100)                             AS query_preview    -- first 100 chars
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND job_type = 'QUERY'
  AND state    = 'DONE'
ORDER BY total_bytes_processed DESC
LIMIT 20;
