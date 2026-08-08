{% macro fabric__test_fewer_rows_than(model, compare_model, group_by_columns = []) %}

{{ config(fail_calc = 'sum(coalesce(row_count_delta, 0))') }}

{% if group_by_columns|length() > 0 %}
  {% set select_gb_cols = group_by_columns|join(', ') + ', ' %}
  {% set join_gb_cols %}
    {% for c in group_by_columns %}
      {% if not loop.first %}and {% endif %}a.{{c}} = b.{{c}}
    {% endfor %}
  {% endset %}
  {% set groupby_gb_cols = 'group by ' + group_by_columns|join(',') %}
{% else %}
  {% set join_gb_cols = '1 = 1' %}
{% endif %}

with a as (

    select
      {{ select_gb_cols }}
      count(*) as count_our_model
    from {{ model }}
    {{ groupby_gb_cols }}

),
b as (

    select
      {{ select_gb_cols }}
      count(*) as count_comparison_model
    from {{ compare_model }}
    {{ groupby_gb_cols }}

),
counts as (

    select

        {% for c in group_by_columns -%}
          a.{{c}} as {{c}}_a,
          b.{{c}} as {{c}}_b,
        {% endfor %}

        count_our_model,
        count_comparison_model
    from a
    full join b
    on {{ join_gb_cols }}

),
final as (

    select *,
        {#- Upstream uses GREATEST(). T-SQL has no GREATEST; emulated with CASE. #}
        case
            when coalesce(count_our_model, 0) > coalesce(count_comparison_model, 0) then (coalesce(count_our_model, 0) - coalesce(count_comparison_model, 0))
            when coalesce(count_our_model, 0) = coalesce(count_comparison_model, 0) then 1
            else 0
        end as row_count_delta
    from counts

)

select * from final

{% endmacro %}
