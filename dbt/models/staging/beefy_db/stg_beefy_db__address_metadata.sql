{{
  config(
    pre_hook=[
      "DROP TABLE IF EXISTS con_beefy_db__address_metadata",
      "CREATE OR REPLACE TABLE con_beefy_db__address_metadata (
        chain_id Int64,
        address String,
        is_contract UInt8,
        label String
      ) ENGINE = PostgreSQL('{{ var(\"beefy_db_host\") }}:{{ var(\"beefy_db_port\") }}', '{{ var(\"beefy_db_name\") }}', 'address_metadata', '{{ var(\"beefy_db_user\") }}', '{{ var(\"beefy_db_password\") }}')"
    ],
    materialized='table',
  )
}}

SELECT
  chain_id,
  address,
  is_contract,
  label
FROM con_beefy_db__address_metadata

