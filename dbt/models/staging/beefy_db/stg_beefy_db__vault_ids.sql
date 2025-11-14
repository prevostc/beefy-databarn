{{
  config(
    pre_hook=[
      "DROP TABLE IF EXISTS con_beefy_db__vault_ids",
      "CREATE OR REPLACE TABLE con_beefy_db__vault_ids (
        id Int32,
        vault_id String
      ) ENGINE = PostgreSQL('{{ var(\"beefy_db_host\") }}:{{ var(\"beefy_db_port\") }}', '{{ var(\"beefy_db_name\") }}', 'vault_ids', '{{ var(\"beefy_db_user\") }}', '{{ var(\"beefy_db_password\") }}')"
    ],
    materialized='table',
  )
}}

SELECT
  id,
  vault_id
FROM con_beefy_db__vault_ids

