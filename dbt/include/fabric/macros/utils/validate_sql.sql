{% macro fabric__validate_sql(sql) -%}
  {% call statement('set_showplan_on', auto_begin=False) -%}
    SET SHOWPLAN_XML ON;
  {% endcall %}
  {% call statement('run_sql', auto_begin=False) -%}
    {{ sql }}
  {% endcall %}
  {% call statement('set_showplan_off', auto_begin=False) -%}
    SET SHOWPLAN_XML OFF;
  {% endcall %}
  {{ return(load_result('run_sql')) }}
{% endmacro %}
