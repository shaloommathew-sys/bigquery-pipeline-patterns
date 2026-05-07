/*
  scd_type2_bigquery.sql
  =======================
  SCD Type 2 implementation using BigQuery MERGE.

  WHAT IS SCD TYPE 2?
  Slowly Changing Dimension Type 2 tracks HISTORY of changes.
  Instead of overwriting the old value, we close the old record
  and insert a new active record.

  WHY IT MATTERS:
  Without SCD2, if a customer moves from Mumbai to Bangalore, you lose
  the fact that their January orders were placed from Mumbai.
  Analytics becomes wrong: "revenue by city" would show Bangalore
  for orders that actually came from Mumbai.

  WITH SCD2:
  You can query: "what city was customer C001 in on 2024-01-15?"
  by filtering: valid_from <= '2024-01-15' AND valid_to > '2024-01-15'

  COLUMNS ADDED:
  - valid_from   : date this version became active
  - valid_to     : date this version was closed (9999-12-31 = still active)
  - is_current   : boolean flag for easy "current state" queries
  - _scd_key     : surrogate key = business_key + valid_from (unique per version)
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- SETUP: Customer dimension table with SCD2 columns
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `project.dataset.dim_customer`
(
  _scd_key          STRING    NOT NULL,   -- surrogate key: customer_id + valid_from
  customer_id       STRING    NOT NULL,   -- business/natural key
  customer_name     STRING,
  city              STRING,
  state             STRING,
  customer_segment  STRING,
  valid_from        DATE      NOT NULL,   -- when this version became active
  valid_to          DATE      NOT NULL,   -- 9999-12-31 means still active
  is_current        BOOL      NOT NULL,   -- true = current active record
  _created_at       TIMESTAMP,
  _updated_at       TIMESTAMP
)
PARTITION BY valid_from                   -- partition by when the version started
CLUSTER BY customer_id, is_current        -- fast lookup of current records per customer
OPTIONS (
  description = 'Customer dimension with full SCD Type 2 history tracking'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- CORE MERGE: Apply SCD2 logic
-- Called by Airflow DAG on each daily run
-- ─────────────────────────────────────────────────────────────────────────────

MERGE `project.dataset.dim_customer` AS target

-- Source: incoming records from Silver layer (current state of each customer)
USING (
  SELECT
    customer_id,
    customer_name,
    city,
    state,
    customer_segment,
    DATE(@run_date)  AS effective_date
  FROM `project.dataset.customers_silver`
  WHERE _silver_updated_date = DATE(@run_date)   -- only today's changes
) AS source

ON  target.customer_id = source.customer_id
AND target.is_current  = TRUE                    -- only compare against the current version


-- ── Case 1: Record exists and something CHANGED ───────────────────────────
-- Close the current record by setting valid_to = yesterday and is_current = FALSE
WHEN MATCHED
  AND (
    target.city             != source.city
    OR target.state          != source.state
    OR target.customer_name  != source.customer_name
    OR target.customer_segment != source.customer_segment
  )
THEN UPDATE SET
  target.valid_to    = DATE_SUB(DATE(@run_date), INTERVAL 1 DAY),  -- close yesterday
  target.is_current  = FALSE,
  target._updated_at = CURRENT_TIMESTAMP()

-- ── Case 2: New customer (no existing record) ─────────────────────────────
-- Insert directly as a new current record
WHEN NOT MATCHED BY TARGET
THEN INSERT (
  _scd_key, customer_id, customer_name, city, state,
  customer_segment, valid_from, valid_to, is_current,
  _created_at, _updated_at
)
VALUES (
  CONCAT(source.customer_id, '_', CAST(source.effective_date AS STRING)),
  source.customer_id,
  source.customer_name,
  source.city,
  source.state,
  source.customer_segment,
  source.effective_date,
  DATE('9999-12-31'),           -- open-ended: still active
  TRUE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);

/*
  NOTE: MERGE handles closes but NOT the new version inserts for CHANGED records.
  BigQuery MERGE can't do both UPDATE (close old) and INSERT (open new) for the
  same matched row in one statement.

  SOLUTION: Run a second INSERT after the MERGE to add the new active versions:
*/

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2: Insert new active versions for CHANGED records
-- Runs immediately after the MERGE above
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO `project.dataset.dim_customer`
  (_scd_key, customer_id, customer_name, city, state,
   customer_segment, valid_from, valid_to, is_current, _created_at, _updated_at)

SELECT
  CONCAT(s.customer_id, '_', CAST(DATE(@run_date) AS STRING)) AS _scd_key,
  s.customer_id,
  s.customer_name,
  s.city,
  s.state,
  s.customer_segment,
  DATE(@run_date)          AS valid_from,
  DATE('9999-12-31')       AS valid_to,
  TRUE                     AS is_current,
  CURRENT_TIMESTAMP()      AS _created_at,
  CURRENT_TIMESTAMP()      AS _updated_at

FROM `project.dataset.customers_silver` s

-- Only insert for customers whose old record was just closed (CHANGED ones)
INNER JOIN `project.dataset.dim_customer` t
  ON  t.customer_id = s.customer_id
  AND t.is_current  = FALSE               -- was just closed by the MERGE above
  AND t.valid_to    = DATE_SUB(DATE(@run_date), INTERVAL 1 DAY);


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY PATTERNS — how to use the SCD2 table
-- ─────────────────────────────────────────────────────────────────────────────

-- Get CURRENT state of all customers (fast — uses cluster on is_current)
SELECT customer_id, customer_name, city, customer_segment
FROM `project.dataset.dim_customer`
WHERE is_current = TRUE;


-- Get customer state on a SPECIFIC DATE (point-in-time query)
-- Answers: "what city was C001 in on 2024-03-15?"
SELECT customer_id, customer_name, city
FROM `project.dataset.dim_customer`
WHERE customer_id = 'C001'
  AND valid_from <= DATE('2024-03-15')
  AND valid_to   >  DATE('2024-03-15');


-- Count versions per customer (how many times did they change?)
SELECT
  customer_id,
  COUNT(*) AS version_count,
  MIN(valid_from) AS first_seen,
  MAX(valid_from) AS latest_change
FROM `project.dataset.dim_customer`
GROUP BY customer_id
HAVING COUNT(*) > 1              -- only customers who changed at least once
ORDER BY version_count DESC;
