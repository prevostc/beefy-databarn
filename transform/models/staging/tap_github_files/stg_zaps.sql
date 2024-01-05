{{
  config(
    materialized='view'
  )
}}

with zap_v3 as (
    select *
    from {{ source('tap_github_files', 'beefy_zap_v3') }}
)

select
    "chainId" as "chain",
    {{ hex_text_to_bytea('router') }} as router_contract_address,
    {{ hex_text_to_bytea('manager') }} as manager_contract_address
from zap_v3
