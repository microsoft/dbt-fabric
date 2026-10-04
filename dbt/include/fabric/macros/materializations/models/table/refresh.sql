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

  {%- set contract_config = config.get('contract') -%}
  {%- set identity_column = adapter.get_identity_column(model['columns'], contract_config.enforced) -%}
  {{ fabric_assert_identity_supported(identity_column) }}

  {%- if existing_relation is not none and existing_relation.type == 'table' -%}
    {%- set refresh_plan = adapter.get_table_refresh_plan(
        existing_relation,
        compiled_code,
        config.get('cluster_by'),
        model['constraints'],
        model['columns'],
        contract_config.enforced
    ) -%}
  {%- elif existing_relation is not none -%}
    {%- set refresh_plan = {
        'action': 'legacy_replace',
        'reason': 'relation type changed',
        'column_names': [],
        'identity_column': identity_column,
        'constraints_to_drop': [],
        'constraints_to_add': [],
        'constraint_add_sql': []
    } -%}
  {%- else -%}
    {%- set refresh_plan = {
        'action': 'replace',
        'reason': 'target table does not exist',
        'column_names': [],
        'identity_column': identity_column,
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
    {% call statement('main', language=language) -%}
      {{ fabric_atomic_reload_sql(
          target_relation,
          compiled_code,
          refresh_plan['column_names'],
          refresh_plan['identity_column']
      ) }}
    {%- endcall %}
  {% elif refresh_plan['action'] == 'replace' %}
    {% set create_sql = create_table_as(
        False,
        intermediate_relation,
        compiled_code,
        language
    ) %}
    {% call statement('main', language=language) -%}
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

{% macro fabric_atomic_reload_sql(target_relation, compiled_code, column_names, identity_column=none) %}
  {{ return(adapter.dispatch('atomic_reload_sql', 'dbt')(
      target_relation,
      compiled_code,
      column_names,
      identity_column
  )) }}
{% endmacro %}

{% macro fabric__atomic_reload_sql(target_relation, compiled_code, column_names, identity_column=none) %}
  {%- set insert_columns = fabric_identity_insert_columns(column_names, identity_column) -%}
  {#-
      compiled_code may start with a leading CTE (`WITH cte AS (...) SELECT ...`). T-SQL
      only allows WITH as the first clause of a statement, so
      `INSERT INTO t (...) WITH cte AS (...) SELECT ...` is a syntax error, and a CTE
      cannot be nested inside a derived-table subquery either. Routing the model SQL
      through a disposable view sidesteps both restrictions: the view body can start
      with WITH, and the INSERT then selects from the view by name.
  -#}
  {%- set tmp_vw_relation = target_relation.incorporate(
      path={"identifier": target_relation.identifier ~ '__dbt_reload_vw'}, type='view'
  ) -%}
  {{ get_use_database_sql(target_relation.database) }}
  {% do adapter.drop_relation(tmp_vw_relation) %}
  {{ get_create_view_as_sql(tmp_vw_relation, compiled_code) }}
  TRUNCATE TABLE {{ target_relation.include(database=False) }};
  {% if identity_column is not none and identity_column.mode == 'insert' %}
  SET IDENTITY_INSERT {{ target_relation.include(database=False) }} ON;
  {% endif %}
  INSERT INTO {{ target_relation.include(database=False) }}
    ({{ insert_columns }})
  SELECT {{ insert_columns }} FROM {{ tmp_vw_relation.include(database=False) }};
  {% if identity_column is not none and identity_column.mode == 'insert' %}
  SET IDENTITY_INSERT {{ target_relation.include(database=False) }} OFF;
  {#- Fabric Data Warehouse only supports the bare RESEED form: it computes the next
      value internally and rejects an explicit new_reseed_value (unlike
      SQL Server/Synapse). See:
      https://learn.microsoft.com/en-us/fabric/data-warehouse/identity#reseed-identity-values-with-dbcc-checkident -#}
  DBCC CHECKIDENT ('{{ target_relation.include(database=False) | replace("'", "''") }}', RESEED);
  {% endif %}
  DROP VIEW IF EXISTS {{ tmp_vw_relation.include(database=False) }};
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
  {{ create_sql }}
  {% if existing_relation is not none %}
    DROP {{ existing_relation.type }} {{ existing_relation.include(database=False) }};
  {% endif %}
  EXEC sp_rename
    '{{ intermediate_relation.include(database=False) | replace("'", "''") }}',
    '{{ target_relation.identifier | replace("'", "''") }}';
{% endmacro %}
