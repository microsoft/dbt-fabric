{% macro fabric__validate_sql(sql) -%}
  {% call statement('validate_sql') -%}
    SET SHOWPLAN_ALL ON;
    {{ sql }}
    SET SHOWPLAN_ALL OFF;
  {% endcall %}
  {{ return(load_result('validate_sql')) }}
{% endmacro %}