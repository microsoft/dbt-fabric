{% macro build_columns_constraints(relation) %}
    {{ return(adapter.dispatch('build_columns_constraints', 'dbt')(relation)) }}
{% endmacro %}

{% macro fabric__build_columns_constraints(relation) %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
    {%- set raw_column_constraints = adapter.render_raw_columns_constraints(raw_columns=model['columns']) -%}
    (
      {% for c in raw_column_constraints -%}
        {{ c }}{{ "," if not loop.last }}
      {% endfor %}
    )
{% endmacro %}

{% macro build_model_constraints(relation) %}
    {{ return(adapter.dispatch('build_model_constraints', 'dbt')(relation)) }}
{% endmacro %}

{% macro fabric__build_model_constraints(relation) %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
    {%- set raw_model_constraints = adapter.render_raw_model_constraints(raw_constraints=model['constraints']) -%}
    {% for c in raw_model_constraints -%}
      {% set alter_table_script %}
        alter table {{ relation.include(database=False) }} {{c}};
      {%endset%}
      {% call statement('alter_table_add_constraint') -%}
        {{alter_table_script}}
      {%- endcall %}
    {% endfor -%}
{% endmacro %}

{% macro reconcile_model_constraints(relation, refresh_plan) %}
    {{ return(adapter.dispatch('reconcile_model_constraints', 'dbt')(
        relation,
        refresh_plan
    )) }}
{% endmacro %}

{% macro fabric__reconcile_model_constraints(relation, refresh_plan) %}
  {% set constraints_to_drop = refresh_plan['constraints_to_drop'] %}
  {% set constraint_add_sql = refresh_plan['constraint_add_sql'] %}
  {% if constraints_to_drop or constraint_add_sql %}
    {% call statement('reconcile_model_constraints') -%}
      {{ get_use_database_sql(relation.database) }}
      {% for constraint_name in constraints_to_drop %}
        ALTER TABLE {{ relation.include(database=False) }}
        DROP CONSTRAINT {{ adapter.quote(constraint_name) }};
      {% endfor %}
      {% for rendered_constraint in constraint_add_sql %}
        ALTER TABLE {{ relation.include(database=False) }}
        {{ rendered_constraint }};
      {% endfor %}
    {%- endcall %}
  {% endif %}
{% endmacro %}
