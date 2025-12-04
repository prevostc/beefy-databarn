{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(id as String) as id,
  ifNull(name, 'Unknown') as name,
  website,
  twitter,
  documentation,
  description,
  type,
  JSONExtract(coalesce(risks, '[]'), 'Array(String)') as risks
FROM {{ source('dlt', 'github_files___beefy_platforms') }}

