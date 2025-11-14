{{
  config(
    pre_hook=[
      "DROP TABLE IF EXISTS con_beefy_db__bifi_buyback",
      "CREATE OR REPLACE TABLE con_beefy_db__bifi_buyback (
        id Int32,
        block_number Int32,
        txn_timestamp DateTime64(6, 'UTC'),
        event_idx Int16,
        txn_hash String,
        bifi_amount Decimal256(20),
        bifi_price Decimal256(20),
        buyback_total Decimal256(20)
      ) ENGINE = PostgreSQL('{{ var(\"beefy_db_host\") }}:{{ var(\"beefy_db_port\") }}', '{{ var(\"beefy_db_name\") }}', 'bifi_buyback', '{{ var(\"beefy_db_user\") }}', '{{ var(\"beefy_db_password\") }}')"
    ],
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
  )
}}

SELECT
  id,
  block_number,
  txn_timestamp,
  event_idx,
  txn_hash,
  bifi_amount,
  bifi_price,
  buyback_total
FROM con_beefy_db__bifi_buyback
{% if is_incremental() %}
  WHERE block_number > (SELECT COALESCE(MAX(block_number), 0) FROM {{ this }})
{% endif %}

