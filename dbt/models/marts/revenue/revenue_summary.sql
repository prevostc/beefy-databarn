{{
  config(
    materialized='view',
    tags=['marts', 'revenue']
  )
}}

-- Mart model: Daily revenue summary
-- Business-ready aggregated revenue model for visualization and API consumption
-- Aggregates revenue from BIFI buyback events by day
-- Revenue is calculated as the sum of buyback_total values

SELECT
  event_date,
  currency,
  COUNT(*) AS event_count,
  SUM(revenue_amount) AS total_revenue,
  AVG(revenue_amount) AS avg_revenue_per_event,
  MIN(revenue_amount) AS min_revenue,
  MAX(revenue_amount) AS max_revenue,
  SUM(bifi_amount) AS total_bifi_amount,
  AVG(bifi_price) AS avg_bifi_price,
  COUNT(DISTINCT txn_hash) AS unique_transactions
FROM {{ ref('int_revenue_daily') }}
GROUP BY
  event_date,
  currency
ORDER BY
  event_date DESC,
  currency

