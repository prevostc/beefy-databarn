{{
  config(
    pre_hook=[
      "DROP TABLE IF EXISTS con_beefy_db__chains",
      "CREATE OR REPLACE TABLE con_beefy_db__chains (
        chain_id Int64,
        name String,
        beefy_name String,
        enabled UInt8
      ) ENGINE = PostgreSQL('{{ var(\"beefy_db_host\") }}:{{ var(\"beefy_db_port\") }}', '{{ var(\"beefy_db_name\") }}', 'chains', '{{ var(\"beefy_db_user\") }}', '{{ var(\"beefy_db_password\") }}')"
    ],
    materialized='table',
  )
}}

SELECT
  chain_id,
  name,
  beefy_name,
  enabled
FROM con_beefy_db__chains

