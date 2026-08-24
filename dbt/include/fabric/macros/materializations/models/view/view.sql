{% materialization view, adapter='fabric' -%}

  {%- set target_relation = this.incorporate(type='view') -%}
  {%- set existing_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) -%}

  {%- set backup_relation = none %}
  {% if (existing_relation != none and existing_relation.type == "table") %}
      {%- set backup_relation = make_backup_relation(target_relation, 'table') -%}
  {% elif (existing_relation != none and existing_relation.type == "view") %}
      {%- set backup_relation = make_backup_relation(target_relation, 'view') -%}
  {% endif %}

  {% if backup_relation is not none %}
    {%- set preexisting_backup_relation = adapter.get_relation(
        database=backup_relation.database,
        schema=backup_relation.schema,
        identifier=backup_relation.identifier
    ) -%}
    {% if preexisting_backup_relation is not none %}
      {% do adapter.drop_relation(preexisting_backup_relation) %}
      {% do adapter.commit() %}
    {% endif %}
  {% endif %}

  {% set grant_config = config.get('grants') %}
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is not none %}
    {{ adapter.rename_relation(existing_relation, backup_relation) }}
  {% endif %}

  -- build model
  {% call statement('main') -%}
    {{ get_create_view_as_sql(target_relation, sql) }}
  {%- endcall %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% if backup_relation is not none %}
    {% do adapter.drop_relation(backup_relation) %}
  {% endif %}

  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}
