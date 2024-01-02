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
    from {{ ref('int_active_vaults') }}
)

select *
from boosts
where
    not eol
    and eol_date >= now()
    and vault_id in (select vault_id from active_vaults)
