{% macro fabric__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}

  With dbt_internal_test AS (
    {{ main_sql }}
  )
  select
    COUNT(*) AS failures,
    CASE WHEN COUNT(*) != 0 THEN 'true' ELSE 'false' END AS should_warn,
    CASE WHEN COUNT(*) != 0 THEN 'true' ELSE 'false' END AS should_error
    FROM dbt_internal_test

{%- endmacro %}
