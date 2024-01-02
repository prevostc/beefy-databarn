{{
  config(
    materialized='view'
  )
}}

with base as (
    select *
    from {{ source('tap_rest_api_msdk', 'beefy_vaults') }}
)

select
    id as vault_id,
    name,
    chain,
    status = 'eol' as eol,
    (assets::jsonb) as assets,
    {{ hex_text_to_bytea('earnContractAddress') }} as contract_address,
    {{ hex_text_to_bytea('strategy') }} as strategy_address,
    "platformId" as platform,
    "strategyTypeId" as strategy_type,
    "token" as share_token_name,
    "tokenDecimals" as share_token_decimals,
    "earnedTokenAddress" as underlying_contract_address,
    "tokenDecimals" as underlying_decimals,
    "oracleId" as underlying_price_feed_key,
    jsonb_build_object(
        'optimism', {{ hex_text_to_bytea("bridged_optimism") }}
    ) as bridged_version_addresses

from base
