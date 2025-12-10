{% test expect_column_max_value(model, column_name, max_value) %}

-- Test that a column value does not exceed a maximum threshold
-- Handles NULL values by allowing them (NULL values pass the test)
--
-- Parameters:
--   column_name: The column to check (automatically passed by dbt when test is on a column)
--   max_value: The maximum allowed value (inclusive)

{%- set column_name_quoted = adapter.quote(column_name) if column_name else 'col' %}

with validation_errors as (
    select
        {{ column_name_quoted }}
    from {{ model }}
    where {{ column_name_quoted }} IS NOT NULL
      and {{ column_name_quoted }} > {{ max_value }}
)

select *
from validation_errors

{% endtest %}
