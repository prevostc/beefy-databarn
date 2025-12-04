{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(chain_id as Int64) as network_id,
  ifNull(name, 'Unknown') as name,
  cast(chain_key as String) as chain_key,
  {{ to_str_list('rpc') }} as rpc,
  explorer_url,
  {{ evm_address('multicall3_address') }} as multicall3_address,
  {{ evm_address('app_multicall_contract_address') }} as app_multicall_contract_address,
  JSONExtractString(native, 'symbol') as native_token_symbol,
  JSONExtractString(native, 'oracleId') as native_token_oracle_id,
  toInt64(JSONExtractString(native, 'decimals')) as native_token_decimals,
  JSONExtractString(gas, 'type') as gas_type,
  case when eol is null then null else toDateTime(toInt64(eol)) end as eol_date_time,
  toBool(disabled) as ui_disabled,
  toBool(new) as ui_new
FROM {{ source('dlt', 'github_files___beefy_ui_chains') }}

