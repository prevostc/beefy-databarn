{{
  config(
    materialized='table',
    tags=['dimension', 'platforms']
  )
}}

-- Dimension table: Platform reference data
-- This table provides platform metadata for joining with fact tables
-- Small reference table, materialized as table for performance

SELECT
  id as platform_id,
  name as platform_name,
  website,
  twitter,
  documentation,
  description,
  type as platform_type,
  risks
FROM {{ ref('stg_github_files___beefy_platforms') }}

