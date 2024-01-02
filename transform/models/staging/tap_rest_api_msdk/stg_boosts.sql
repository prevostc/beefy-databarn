{{
  config(
    materialized='view'
  )
}}

with base as (
    select *
    from {{ source('tap_rest_api_msdk', 'beefy_boosts') }}
)

select
    id as boost_id,
    "chain",
    "poolId" as vault_id,
    "name",
    {{ hex_text_to_bytea('earnContractAddress') }} as contract_address,
    "earnedTokenDecimals" as reward_token_decimals,
    "earnedToken" as reward_token_symbol,
    {{ hex_text_to_bytea('earnedTokenAddress') }} as reward_token_address,
    "earnedOracleId" as reward_token_price_feed_key,
    status = 'closed' as eol,
    TO_TIMESTAMP("periodFinish") as eol_date

from base
