{% test row_count_drop_threshold(model, compare_model, max_drop_percentage=0.1) %}

  {{ config(fail_calc = 'sum(coalesce(drop_exceeds_threshold, 0))') }}

  {#-- Prevent querying of db in parsing mode --#}
  {%- if not execute -%}
      {{ return('') }}
  {% endif %}

  with source_counts as (
    select count(*) as source_count
    from {{ compare_model }}
  ),
  target_counts as (
    select count(*) as target_count
    from {{ model }}
  ),
  comparison as (
    select
      s.source_count,
      t.target_count,
      s.source_count - t.target_count as rows_dropped,
      case
        when s.source_count > 0 then (s.source_count - t.target_count) * 100.0 / s.source_count
        else 0
      end as drop_percentage,
      case
        when s.source_count = 0 then 0  -- No source rows, can't calculate drop
        when t.target_count > s.source_count then 1  -- Target has more rows than source (shouldn't happen)
        when (s.source_count - t.target_count) * 100.0 / s.source_count > {{ max_drop_percentage }} then 1
        else 0
      end as drop_exceeds_threshold
    from source_counts s
    cross join target_counts t
  )

  select *
  from comparison
  where drop_exceeds_threshold > 0

{% endtest %}

