{% macro fabric_full_refresh_table(target_relation, existing_relation, compiled_code, language) %}
  {{ return(adapter.dispatch('full_refresh_table', 'dbt')(
      target_relation,
      existing_relation,
      compiled_code,
      language
  )) }}
{% endmacro %}

{% macro fabric__full_refresh_table(target_relation, existing_relation, compiled_code, language) %}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}

  {%- set backup_relation_type = (
      'table' if existing_relation is none else existing_relation.type
  ) -%}
  {%- set backup_relation = make_backup_relation(
      target_relation,
      backup_relation_type
  ) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {%- if existing_relation is not none and existing_relation.type == 'table' -%}
    {%- set refresh_plan = adapter.get_table_refresh_plan(
        existing_relation,
        compiled_code,
        config.get('cluster_by'),
        model['constraints']
    ) -%}
  {%- elif existing_relation is not none -%}
    {%- set refresh_plan = {
        'action': 'legacy_replace',
        'reason': 'relation type changed',
        'column_names': [],
        'constraints_to_drop': [],
        'constraints_to_add': [],
        'constraint_add_sql': []
    } -%}
  {%- else -%}
    {%- set refresh_plan = {
        'action': 'replace',
        'reason': 'target table does not exist',
        'column_names': [],
        'constraints_to_drop': [],
        'constraints_to_add': [],
        'constraint_add_sql': []
    } -%}
  {%- endif -%}

  {{ log(
      'Fabric full refresh for ' ~ target_relation ~ ': '
      ~ refresh_plan['action'] ~ ' (' ~ refresh_plan['reason'] ~ ')',
      info=True
  ) }}

  {% if refresh_plan['action'] == 'reload' %}
    {% call statement('main', language=language, auto_begin=False) -%}
      {{ fabric_atomic_reload_sql(
          target_relation,
          compiled_code,
          refresh_plan['column_names']
      ) }}
    {%- endcall %}
  {% elif refresh_plan['action'] == 'replace' %}
    {% set create_sql = create_table_as(
        False,
        intermediate_relation,
        compiled_code,
        language
    ) %}
    {% call statement('main', language=language, auto_begin=False) -%}
      {{ fabric_atomic_replace_sql(
          target_relation,
          existing_relation,
          intermediate_relation,
          create_sql
      ) }}
    {%- endcall %}
    {% do create_indexes(target_relation) %}
  {% else %}
    {% call statement('main', language=language) -%}
      {{ create_table_as(False, intermediate_relation, compiled_code, language) }}
    {%- endcall %}
    {% do create_indexes(intermediate_relation) %}
    {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {{ adapter.rename_relation(intermediate_relation, target_relation) }}
    {{ adapter.drop_relation(backup_relation) }}
  {% endif %}

  {{ return(refresh_plan) }}
{% endmacro %}

{% macro fabric_atomic_reload_sql(target_relation, compiled_code, column_names) %}
  {{ return(adapter.dispatch('atomic_reload_sql', 'dbt')(
      target_relation,
      compiled_code,
      column_names
  )) }}
{% endmacro %}

{% macro fabric__atomic_reload_sql(target_relation, compiled_code, column_names) %}
  {%- set quoted_columns = [] -%}
  {%- for column_name in column_names -%}
    {%- do quoted_columns.append(adapter.quote(column_name)) -%}
  {%- endfor -%}
  {{ get_use_database_sql(target_relation.database) }}
  BEGIN TRY
    BEGIN TRANSACTION;
    TRUNCATE TABLE {{ target_relation.include(database=False) }};
    INSERT INTO {{ target_relation.include(database=False) }}
      ({{ quoted_columns | join(', ') }})
    {{ compiled_code }};
    COMMIT TRANSACTION;
  END TRY
  BEGIN CATCH
    IF @@TRANCOUNT > 0
      ROLLBACK TRANSACTION;
    THROW;
  END CATCH;
{% endmacro %}

{% macro fabric_atomic_replace_sql(
    target_relation,
    existing_relation,
    intermediate_relation,
    create_sql
) %}
  {{ return(adapter.dispatch('atomic_replace_sql', 'dbt')(
      target_relation,
      existing_relation,
      intermediate_relation,
      create_sql
  )) }}
{% endmacro %}

{% macro fabric__atomic_replace_sql(
    target_relation,
    existing_relation,
    intermediate_relation,
    create_sql
) %}
  {{ get_use_database_sql(target_relation.database) }}
  BEGIN TRY
    BEGIN TRANSACTION;
    {{ create_sql }}
    {% if existing_relation is not none %}
      DROP {{ existing_relation.type }} {{ existing_relation.include(database=False) }};
    {% endif %}
    EXEC sp_rename
      '{{ intermediate_relation.include(database=False) | replace("'", "''") }}',
      '{{ target_relation.identifier | replace("'", "''") }}';
    COMMIT TRANSACTION;
  END TRY
  BEGIN CATCH
    IF @@TRANCOUNT > 0
      ROLLBACK TRANSACTION;
    THROW;
  END CATCH;
{% endmacro %}
