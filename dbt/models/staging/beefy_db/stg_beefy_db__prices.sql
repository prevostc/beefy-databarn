{{
  config(
    pre_hook=[
      "DROP TABLE IF EXISTS con_beefy_db__prices",
      "CREATE OR REPLACE TABLE con_beefy_db__prices (
        oracle_id Int32,
        t DateTime64(6, 'UTC'),
        val Float64
      ) ENGINE = PostgreSQL('{{ var(\"beefy_db_host\") }}:{{ var(\"beefy_db_port\") }}', '{{ var(\"beefy_db_name\") }}', 'prices', '{{ var(\"beefy_db_user\") }}', '{{ var(\"beefy_db_password\") }}')"
    ],
    materialized='incremental',
    unique_key=['oracle_id', 't'],
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    order_by=['oracle_id', 't'],
  )
}}

{% if is_incremental() %}
  {%- set max_timestamp_query -%}
    SELECT MAX(t) as max_t
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
  oracle_id,
  t,
  val
FROM con_beefy_db__prices
{% if is_incremental() and max_timestamp %}
WHERE t >= '{{ max_timestamp }}' - INTERVAL 30 DAY
{% endif %}

