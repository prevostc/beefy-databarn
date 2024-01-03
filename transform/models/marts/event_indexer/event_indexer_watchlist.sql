{{
  config(
    materialized='view'
  )
}}

with

vaults as (

    select * from {{ ref('stg_vaults' ) }}

),

boosts as (

    select * from {{ ref('int_is_active_boosts' ) }}

)

select
    contract_address,
    chain,
    '{"erc20:transfer"}'::text [] as events
from vaults
where is_active

union all

select
    contract_address,
    chain,
    '{"erc20:transfer"}'::text [] as events
from boosts
where is_active
