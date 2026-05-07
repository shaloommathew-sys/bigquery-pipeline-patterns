/*
  partitioning_strategies.sql
  ===========================
  BigQuery table partitioning patterns with explanations.

  WHY PARTITION?
  Without partitioning, every query scans the ENTIRE table.
  With partitioning, BigQuery skips irrelevant partitions entirely —
  this directly reduces cost (you pay per bytes scanned) and improves speed.

  BigQuery supports 3 types of partitioning:
    1. DATE / TIMESTAMP column partitioning  ← most common
    2. INTEGER RANGE partitioning
    3. Ingestion-time partitioning (_PARTITIONTIME)

  RULE OF THUMB:
  - Use DATE partitioning when you have a clear event/transaction date column
  - Use ingestion-time partitioning when source data has no reliable date column
  - Use range partitioning for integer IDs (e.g. customer_id ranges)
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- PATTERN 1: DATE COLUMN PARTITIONING
-- Best for: fact tables with a clear event date (orders, transactions, logs)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `project.dataset.orders_partitioned`
(
  order_id        STRING    NOT NULL,
  customer_id     STRING,
  product_id      STRING,
  order_amount    NUMERIC,
  order_status    STRING,
  order_date      DATE,                  -- this column drives the partition
  region          STRING,
  created_at      TIMESTAMP
)
PARTITION BY order_date                  -- one partition shard per day
OPTIONS (
  require_partition_filter = TRUE,       -- IMPORTANT: forces all queries to include
                                         -- a date filter — prevents accidental full scans
                                         -- that cost money
  partition_expiration_days = 365        -- auto-delete partitions older than 1 year
                                         -- useful for Bronze raw tables
);

/*
  QUERY PATTERN — always filter on the partition column:

  GOOD (scans only Jan 2024 partition — cheap):
    SELECT * FROM orders_partitioned
    WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31'

  BAD (scans entire table — expensive, will be REJECTED if require_partition_filter=TRUE):
    SELECT * FROM orders_partitioned
    WHERE customer_id = 'C001'
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- PATTERN 2: TIMESTAMP PARTITIONING (for event streams)
-- Best for: real-time/streaming tables where precision matters (Pub/Sub events)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `project.dataset.events_partitioned`
(
  event_id        STRING    NOT NULL,
  event_type      STRING,
  user_id         STRING,
  event_payload   JSON,
  event_timestamp TIMESTAMP             -- partition column (hourly or daily)
)
PARTITION BY DATE(event_timestamp)      -- partition by DATE extracted from TIMESTAMP
                                        -- note: BigQuery partitions on DATE granularity
                                        -- even when column is TIMESTAMP
OPTIONS (
  require_partition_filter = TRUE
);


-- ─────────────────────────────────────────────────────────────────────────────
-- PATTERN 3: INGESTION-TIME PARTITIONING
-- Best for: Bronze layer tables where source data has no reliable date column
-- BigQuery automatically assigns _PARTITIONTIME = load time
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `project.dataset.raw_events_ingestion_time`
(
  raw_payload     STRING,
  source_system   STRING,
  _ingestion_ts   TIMESTAMP
)
PARTITION BY _PARTITIONDATE              -- special pseudo-column — auto-assigned by BQ
OPTIONS (
  require_partition_filter = FALSE       -- FALSE for Bronze: sometimes you need full scans
                                         -- for backfill validation
);

/*
  QUERY PATTERN for ingestion-time tables:

    SELECT * FROM raw_events_ingestion_time
    WHERE _PARTITIONDATE = '2024-01-15'   -- filters on ingestion date, not event date
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- PATTERN 4: INTEGER RANGE PARTITIONING
-- Best for: dimension tables partitioned by ID ranges (customer segments)
-- Less common than date partitioning but useful for large dimension tables
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `project.dataset.customers_range_partitioned`
(
  customer_id     INT64     NOT NULL,    -- must be INTEGER for range partitioning
  customer_name   STRING,
  city            STRING,
  segment         STRING,
  created_date    DATE
)
PARTITION BY RANGE_BUCKET(
  customer_id,                           -- partition on this column
  GENERATE_ARRAY(0, 10000000, 500000)   -- creates buckets: 0-500k, 500k-1M, 1M-1.5M...
);

/*
  QUERY PATTERN:
    SELECT * FROM customers_range_partitioned
    WHERE customer_id BETWEEN 1000000 AND 1500000   -- scans only that bucket
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- UTILITY: Check partition metadata for any table
-- Run this to see how many rows/bytes are in each partition
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
  partition_id,
  total_rows,
  ROUND(total_logical_bytes / POW(1024, 3), 2) AS size_gb,
  last_modified_time
FROM `project.dataset.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'orders_partitioned'
ORDER BY partition_id DESC
LIMIT 30;
