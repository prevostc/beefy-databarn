{{
  config(
    materialized='view'
  )
}}

with

vaults as (

    select * from {{ ref('int_active_vaults' ) }}

),

boosts as (

    select * from {{ ref('int_active_boosts' ) }}

)

select
    chain,
    {{ bytea_to_hex_text("contract_address") }} as contract_address
from vaults

union all

select
    chain,
    {{ bytea_to_hex_text("contract_address") }}
from boosts
