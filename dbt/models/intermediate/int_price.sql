{{
  config(
    materialized='view',
    tags=['intermediate']
  )
}}

-- Intermediate model: Deduplicate price oracles by normalizing oracle_id
-- Removes '-rp' suffix from oracle_id strings to map duplicate oracles to the same base oracle
-- Example: 'pancake-cow-bsc-binance-life-wbnb-rp' -> 'pancake-cow-bsc-binance-life-wbnb'

WITH 
-- Join prices with normalized oracles to map both base and -rp oracle IDs to the same oracle_key
prices_with_oracle_key AS (
  SELECT
    o.oracle_id AS oracle_key,
    p.t AS date_time,
    toDecimal256(p.val, 20) AS price
  FROM {{ ref('stg_beefy_db_incremental__prices') }} p
  INNER JOIN {{ ref('stg_beefy_db_configs__price_oracles') }} o
    ON p.oracle_id = o.id
  WHERE NOT endsWith(o.oracle_id, '-rp') -- no need to keep -rp prices since they are duplicates of the base oracle
)

-- Filter out -rp prices since they are duplicates of the base oracle
SELECT
  oracle_key,
  date_time,
  price
FROM prices_with_oracle_key