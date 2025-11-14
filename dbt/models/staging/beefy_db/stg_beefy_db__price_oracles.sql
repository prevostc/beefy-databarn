{{
  config(
    pre_hook=[
      "DROP TABLE IF EXISTS con_beefy_db__price_oracles",
      "CREATE OR REPLACE TABLE con_beefy_db__price_oracles (
        id Int32,
        oracle_id String,
        tokens Array(String)
      ) ENGINE = PostgreSQL('{{ var(\"beefy_db_host\") }}:{{ var(\"beefy_db_port\") }}', '{{ var(\"beefy_db_name\") }}', 'price_oracles', '{{ var(\"beefy_db_user\") }}', '{{ var(\"beefy_db_password\") }}')"
    ],
    materialized='table',
  )
}}

SELECT
  id,
  oracle_id,
  tokens
FROM con_beefy_db__price_oracles

