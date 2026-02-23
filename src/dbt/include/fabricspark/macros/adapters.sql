{% macro fabricspark__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    show schemas
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}