{{
  config(
    materialized='incremental',
    tags=['marts', 'tvl', 'stats'],
    engine='ReplacingMergeTree()',
    order_by=['date_hour', 'chain_id', 'product_address'],
    unique_key=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
    post_hook="OPTIMIZE TABLE {{ this }} FINAL",
  )
}}

{% if is_incremental() %}
  {% set max_date_sql %}
    select max(date_hour) as max_date_hour
    from {{ this }}
  {% endset %}
  {% set max_date_tbl = run_query(max_date_sql) %}
  {% set max_date = max_date_tbl.columns[0][0] %}
{% endif %}

-- Mart model: Timeseries of product stats (TVL, APY, breakdowns, price)
-- This model provides a complete timeseries of all product metrics by joining:
-- - TVL snapshots
-- - APY data
-- - APY breakdown (merged)
-- - LPS breakdown (merged)
-- - Price data (from LPS breakdown underlying_price)
--
-- Optimized for memory: Uses materialized intermediate tables and UNION ALL
-- instead of FULL OUTER JOINs to avoid memory-intensive join operations

SELECT
  p.product_type,
  p.beefy_key,
  p.display_name,
  p.is_active,
  p.platform_id,
  hs.*
FROM {{ ref('int_product_stats__unified_hourly') }} hs
INNER JOIN {{ ref('product') }} p
  ON hs.chain_id = p.chain_id
  AND hs.product_address = p.product_address
{% if is_incremental() %}
  WHERE hs.date_hour >= toDateTime('{{ max_date }}') - INTERVAL 15 DAY
    AND hs.date_hour <= toDateTime('{{ max_date }}') + INTERVAL 3 MONTH
{% endif %}