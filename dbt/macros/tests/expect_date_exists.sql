{% test expect_date_exists(model, expression, days_ago=1) %}

-- Test that a specific date exists in the model
-- By default, checks if yesterday's date exists (days_ago=1)
-- Can be configured to check for any number of days ago
--
-- Parameters:
--   expression: The expression to check (e.g. toDate(txn_timestamp))
--   days_ago: Number of days ago to check (default: 1, i.e., yesterday)

with date_check as (
    select count(*) as date_count
    from {{ model }}
    where {{ expression }} = today() - {{ days_ago }}
),
validation_errors as (
    select
        today() - {{ days_ago }} as expected_date,
        date_count
    from date_check
    where date_count = 0
)

select *
from validation_errors

{% endtest %}

