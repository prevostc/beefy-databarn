{{
  config(
    materialized='view',
    tags=['marts', 'revenue']
  )
}}

-- Mart model: Monthly revenue trends
-- Time-series revenue analysis for visualization and trend analysis
-- Aggregates revenue from BIFI buyback events by month
-- Includes month-over-month revenue growth metrics

WITH monthly_agg AS (
  SELECT
    toStartOfMonth(event_date) AS month,
    currency,
    SUM(revenue_amount) AS monthly_revenue,
    COUNT(*) AS monthly_event_count,
    COUNT(DISTINCT txn_hash) AS monthly_unique_transactions,
    AVG(revenue_amount) AS avg_revenue_per_event,
    SUM(bifi_amount) AS monthly_bifi_amount,
    AVG(bifi_price) AS avg_bifi_price
  FROM {{ ref('int_revenue_daily') }}
  GROUP BY
    month,
    currency
)
SELECT
  month,
  currency,
  monthly_revenue,
  monthly_event_count,
  monthly_unique_transactions,
  avg_revenue_per_event,
  monthly_bifi_amount,
  avg_bifi_price,
  -- Calculate month-over-month growth
  monthly_revenue - lagInFrame(monthly_revenue) OVER (
    PARTITION BY currency
    ORDER BY month
    ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
  ) AS mom_revenue_change
FROM monthly_agg
ORDER BY
  month DESC,
  currency

