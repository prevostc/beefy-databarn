{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(id) as id,
  assumeNotNull(name) as name,
  website,
  twitter,
  documentation,
  description,
  type,
  JSONExtract(coalesce(risks, '[]'), 'Array(String)') as risks
FROM {{ source('dlt', 'github_files___beefy_platforms') }}

