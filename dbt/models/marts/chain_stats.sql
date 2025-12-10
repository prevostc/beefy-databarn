{{
  config(
    materialized='incremental',
    tags=['marts', 'tvl', 'stats', 'chains'],
    engine='CoalescingMergeTree',
    order_by=['date_hour', 'chain_id'],
    on_schema_change='append_new_columns',
    post_hook=["OPTIMIZE TABLE {{ this }} DEDUPLICATE by date_hour, chain_id"],
  )
}}

{% if is_incremental() %}
  {% set max_date_sql %}
    select max(cast(date_hour as Nullable(DateTime('UTC')))) as max_date_hour
    from {{ this }}
  {% endset %}
  {% set max_date_tbl = run_query(max_date_sql) %}
  {% set max_date = max_date_tbl.columns[0][0] %}
  {% if max_date is none %}
    {% set max_date = '2021-07-31 19:30:00' %}
  {% endif %}
{% else %}
  {% set max_date = '2021-07-31 19:30:00' %}
{% endif %}

WITH cleaned_tvl_by_chain AS (
  SELECT
    network_id,
    date_time,
    tvl_usd,
    vault_tvl_usd,
    gov_tvl_usd,
    clm_tvl_usd
  FROM {{ ref('stg_beefy_db__tvl_by_chain') }}

  -- remove obviously erroneous data
  WHERE NOT (
    network_id = 1 AND
    date_time BETWEEN '2024-04-03' AND '2024-04-04'
    AND tvl_usd > {{ to_decimal('8000000000') }}
  )
)
SELECT
  c.chain_id,
  c.chain_name,
  c.beefy_key,
  c.beefy_enabled,
  toStartOfHour(t.date_time) as date_hour,
  argMax(t.tvl_usd, t.date_time) as tvl_usd,
  argMax(t.vault_tvl_usd, t.date_time) as vault_tvl_usd,
  argMax(t.gov_tvl_usd, t.date_time) as gov_tvl_usd,
  argMax(t.clm_tvl_usd, t.date_time) as clm_tvl_usd
FROM cleaned_tvl_by_chain t
INNER JOIN {{ ref('chain') }} c
  ON t.network_id = c.chain_id
{% if is_incremental() %}
  WHERE toStartOfHour(t.date_time) >= toDateTime('{{ max_date }}') - INTERVAL 15 DAY
{% endif %}
GROUP BY
  c.chain_id,
  c.chain_name,
  c.beefy_key,
  c.beefy_enabled,
  toStartOfHour(t.date_time)
