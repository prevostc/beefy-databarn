{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(etag) as etag,
  assumeNotNull(vault_id) as vault_id,
  assumeNotNull(date_time) as date_time,
  toFloat64(price) as price,
  {{ to_representation_evm_address_list('tokens') }} as tokens,
  {{ to_decimal_list('balances') }} as balances,
  {{ to_decimal('total_supply') }} as total_supply,
  {{ to_decimal('underlying_liquidity') }} as underlying_liquidity,
  {{ to_decimal_list('underlying_balances') }} as underlying_balances,
  {{ to_decimal('underlying_price') }} as underlying_price
FROM {{ source('dlt', 'beefy_api_snapshots___lps_breakdown') }}

