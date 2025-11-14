{{
  config(
    pre_hook=[
      "DROP TABLE IF EXISTS con_beefy_db__harvests",
      "CREATE OR REPLACE TABLE con_beefy_db__harvests (
        chain_id Int64,
        block_number Int32,
        txn_idx Int16,
        event_idx Int16,
        txn_timestamp DateTime64(6, 'UTC'),
        txn_hash String,
        vault_id String,
        call_fee Decimal256(20),
        gas_fee Decimal256(20),
        platform_fee Decimal256(20),
        strategist_fee Decimal256(20),
        harvest_amount Decimal256(20),
        native_price Decimal256(20),
        want_price Decimal256(20),
        is_cowllector UInt8,
        strategist_address String
      ) ENGINE = PostgreSQL('{{ var(\"beefy_db_host\") }}:{{ var(\"beefy_db_port\") }}', '{{ var(\"beefy_db_name\") }}', 'harvests', '{{ var(\"beefy_db_user\") }}', '{{ var(\"beefy_db_password\") }}')"
    ],
    materialized='incremental',
    unique_key=['chain_id', 'block_number', 'txn_idx', 'event_idx'],
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    order_by=['txn_timestamp', 'chain_id', 'block_number', 'txn_idx', 'event_idx']
  )
}}

{% if is_incremental() %}
  {%- set max_timestamp_query -%}
    SELECT MAX(txn_timestamp) as max_txn_timestamp
    FROM {{ this }}
  {%- endset -%}
  
  {%- set max_timestamp_results = run_query(max_timestamp_query) -%}
  
  {%- if max_timestamp_results and max_timestamp_results.rows|length > 0 and max_timestamp_results.rows[0][0] -%}
    {%- set max_timestamp = max_timestamp_results.rows[0][0] -%}
  {%- else -%}
    {%- set max_timestamp = None -%}
  {%- endif -%}
{% endif %}

SELECT
  chain_id,
  block_number,
  txn_idx,
  event_idx,
  txn_timestamp,
  txn_hash,
  vault_id,
  call_fee,
  gas_fee,
  platform_fee,
  strategist_fee,
  harvest_amount,
  native_price,
  want_price,
  is_cowllector,
  strategist_address
FROM con_beefy_db__harvests h
{% if is_incremental() and max_timestamp %}
WHERE txn_timestamp >= '{{ max_timestamp }}' - INTERVAL 30 DAY
{% endif %}