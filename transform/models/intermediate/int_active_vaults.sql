{{
  config(
    materialized='view'
  )
}}

with vaults as (
    select *
    from {{ ref('stg_vaults') }}
)

select *
from vaults
where not eol
