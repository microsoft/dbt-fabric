{% macro fabric__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}

  with test_main_sql as (
  {{ main_sql }}
  ),
  dbt_internal_test as (
    select {{ "top (" ~ limit ~ ')' if limit != none }} * from test_main_sql
  )
  select
    {{ fail_calc }} as failures,
    case when {{ fail_calc }} {{ warn_if }}
      then 'true' else 'false' end as should_warn,
    case when {{ fail_calc }} {{ error_if }}
      then 'true' else 'false' end as should_error
  from dbt_internal_test

{%- endmacro %}

{% macro fabric__generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
