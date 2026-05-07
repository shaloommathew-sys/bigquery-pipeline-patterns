/*
  window_functions.sql
  ====================
  Advanced BigQuery window function patterns for data engineering.

  Window functions compute values ACROSS rows related to the current row
  WITHOUT collapsing them into a single output row (unlike GROUP BY).

  This is one of the most tested topics in Senior DE interviews at product companies.
  Patterns here cover: ranking, running totals, lag/lead, percentiles, sessionisation.
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- PATTERN 1: RANKING — top N per group
-- Classic interview question: "Find the top 3 orders by amount per region"
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
  order_id,
  region,
  order_amount,
  order_date,
  RANK()        OVER (PARTITION BY region ORDER BY order_amount DESC) AS rank_in_region,
  DENSE_RANK()  OVER (PARTITION BY region ORDER BY order_amount DESC) AS dense_rank_in_region,
  ROW_NUMBER()  OVER (PARTITION BY region ORDER BY order_amount DESC) AS row_num_in_region
FROM `project.dataset.orders_silver`
WHERE order_status = 'COMPLETED'
QUALIFY rank_in_region <= 3              -- QUALIFY filters on window function result
                                         -- BigQuery-specific: cleaner than a subquery
;

/*
  RANK vs DENSE_RANK vs ROW_NUMBER:
  If amounts are 100, 100, 80:
  - RANK:        1, 1, 3   (gap after tie)
  - DENSE_RANK:  1, 1, 2   (no gap)
  - ROW_NUMBER:  1, 2, 3   (always unique — arbitrary tiebreak)

  For "top N" problems: use RANK when ties should share rank, DENSE_RANK to avoid gaps.
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- PATTERN 2: RUNNING TOTALS and MOVING AVERAGES
-- Used in Gold layer to build cumulative revenue metrics
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
  order_date,
  region,
  daily_revenue,

  -- Cumulative revenue from start of year to this day
  SUM(daily_revenue) OVER (
    PARTITION BY region
    ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_revenue,

  -- 7-day rolling average (smooths out weekly seasonality)
  AVG(daily_revenue) OVER (
    PARTITION BY region
    ORDER BY order_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW   -- current day + 6 previous days
  ) AS rolling_7d_avg,

  -- Month-to-date revenue
  SUM(daily_revenue) OVER (
    PARTITION BY region, DATE_TRUNC(order_date, MONTH)
    ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS mtd_revenue

FROM (
  -- Pre-aggregate to daily level first, then apply windows
  SELECT
    order_date,
    region,
    SUM(order_amount) AS daily_revenue
  FROM `project.dataset.orders_silver`
  WHERE order_status = 'COMPLETED'
  GROUP BY order_date, region
);


-- ─────────────────────────────────────────────────────────────────────────────
-- PATTERN 3: LAG / LEAD — compare current row to previous/next
-- Used for: day-over-day change, detecting gaps, identifying sequences
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
  order_date,
  region,
  daily_revenue,

  -- Revenue from the previous day (LAG)
  LAG(daily_revenue, 1, 0) OVER (
    PARTITION BY region ORDER BY order_date
  ) AS prev_day_revenue,

  -- Day-over-day change %
  ROUND(
    SAFE_DIVIDE(
      daily_revenue - LAG(daily_revenue, 1) OVER (PARTITION BY region ORDER BY order_date),
      LAG(daily_revenue, 1) OVER (PARTITION BY region ORDER BY order_date)
    ) * 100,
    2
  ) AS dod_change_pct,

  -- Revenue from the NEXT day (LEAD) — useful for lookahead in forecasting
  LEAD(daily_revenue, 1) OVER (
    PARTITION BY region ORDER BY order_date
  ) AS next_day_revenue

FROM (
  SELECT order_date, region, SUM(order_amount) AS daily_revenue
  FROM `project.dataset.orders_silver`
  WHERE order_status = 'COMPLETED'
  GROUP BY order_date, region
);


-- ─────────────────────────────────────────────────────────────────────────────
-- PATTERN 4: PERCENTILES — bucket customers by spend
-- Used in customer segmentation: top 10%, mid tier, low tier
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
  customer_id,
  total_spend,
  NTILE(10) OVER (ORDER BY total_spend DESC)   AS spend_decile,  -- 1 = top 10%
  PERCENTILE_CONT(total_spend, 0.5)
    OVER ()                                    AS median_spend,
  PERCENTILE_CONT(total_spend, 0.9)
    OVER ()                                    AS p90_spend,
  CASE
    WHEN NTILE(10) OVER (ORDER BY total_spend DESC) = 1  THEN 'PLATINUM'
    WHEN NTILE(10) OVER (ORDER BY total_spend DESC) <= 3 THEN 'GOLD'
    WHEN NTILE(10) OVER (ORDER BY total_spend DESC) <= 6 THEN 'SILVER'
    ELSE 'BRONZE'
  END                                          AS customer_tier
FROM (
  SELECT customer_id, SUM(order_amount) AS total_spend
  FROM `project.dataset.orders_silver`
  WHERE order_status = 'COMPLETED'
  GROUP BY customer_id
);
