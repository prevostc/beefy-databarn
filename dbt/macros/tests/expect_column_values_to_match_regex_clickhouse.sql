{% test expect_column_values_to_match_regex_clickhouse(model, column_name, regex) %}

{%- set column_name_quoted = adapter.quote(column_name) if column_name else 'col' %}

with validation_errors as (
    select
        {{ column_name_quoted }}
    from {{ model }}
    where not match({{ format_hex(column_name_quoted) }}, '{{ regex }}')
)

select *
from validation_errors

{% endtest %}

