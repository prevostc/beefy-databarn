{{
  config(
    materialized='view'
  )
}}

with boosts as (
    select *
    from {{ ref('stg_boosts') }}
),

active_vaults as (
    select *
    from {{ ref('int_is_active_vaults') }}
    where is_active
),

active_chains as (
    select chain
    from {{ ref('stg_chains') }}
    where is_active
)

select
    *,
    not eol
    and eol_date >= now()
    and vault_id in (select vault_id from active_vaults) 
    and chain in (select chain from active_chains) as is_active
from boosts
