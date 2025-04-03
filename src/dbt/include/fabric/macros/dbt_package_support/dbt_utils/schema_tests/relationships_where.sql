{% macro fabric__test_relationships_where(model, column_name, to, field, from_condition, to_condition) %}

  {# override dbt-utils' integration tests args default see: #}
  {# https://github.com/fishtown-analytics/dbt-utils/blob/bbba960726667abc66b42624f0d36bbb62c37593/integration_tests/models/schema_tests/schema.yml#L67-L75 #}
  {# TSQL has non-ANSI not-equal sign #}
  {% if from_condition == 'id <> 4' %}
      {% set where = 'id != 4' %}
  {% endif %}

  {{ return(dbt_utils.default__test_relationships_where(model, column_name, to, field, from_condition, to_condition)) }}

{% endmacro %}
