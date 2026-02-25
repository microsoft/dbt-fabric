{% macro fabricspark__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    show schemas
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro spark__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
      drop {{ relation.type if relation.type != "materialized_view" else "materialized lake view" }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}