{% test expect_exactly_one_row(model, row_condition=None) %}

-- Test that a model returns exactly one row
-- This is useful for aggregation/summary models that should always return a single row

with row_count as (
    select count(*) as cnt
    from {{ model }}
    {% if row_condition %}
    where {{ row_condition }}
    {% endif %}
),
validation_errors as (
    select cnt
    from row_count
    where cnt != 1
)

select *
from validation_errors

{% endtest %}

