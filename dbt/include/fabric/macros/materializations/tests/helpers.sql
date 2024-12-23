{% macro get_fabric_test_sql(database, schema, main_sql, fail_calc, warn_if, error_if, limit) -%}

  {% set testview %}
    [{{ schema }}.testview_{{ range(1300, 19000) | random }}]
  {% endset %}

  {% set sql = main_sql.replace("'", "''")%}
  {{ get_use_database_sql(database) }}
  EXEC('create view {{testview}} as {{ sql }};')
  select
    {{ fail_calc }} as failures,
    case when {{ fail_calc }} {{ warn_if }}
      then 'true' else 'false' end as should_warn,
    case when {{ fail_calc }} {{ error_if }}
      then 'true' else 'false' end as should_error
  from (
    select {{ "top (" ~ limit ~ ')' if limit != none }} * from {{testview}}
  ) dbt_internal_test;

  {{ get_use_database_sql(database) }}
  EXEC('drop view {{testview}};')
{%- endmacro %}

{% macro fabric__generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
