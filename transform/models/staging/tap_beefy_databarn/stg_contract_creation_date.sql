{{
  config(
    materialized='view'
  )
}}

with base as (
    select *
    from {{ source('tap_beefy_databarn', 'contract_creation_date') }}
)

select
  chain,
   {{ hex_text_to_bytea("contract_address") }} as contract_address,
  block_number,
  block_datetime

from base
