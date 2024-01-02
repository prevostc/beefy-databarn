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
    contract_address,
    chain
from vaults

union all

select
    contract_address,
    chain
from boosts
