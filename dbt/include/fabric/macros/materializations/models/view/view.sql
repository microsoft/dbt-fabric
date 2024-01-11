{% materialization view, adapter='fabric' -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}

  -- make back up relation
  {%- set backup_relation_type = 'view' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}

  {% set grant_config = config.get('grants') %}
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- drop target relation if exists already in the database
  {{ drop_relation_if_exists(backup_relation) }}

  {%- set relation = fabric__get_relation_without_caching(target_relation) %}
    {% if relation|length > 0 %}
        {{ adapter.rename_relation(target_relation, backup_relation) }}
    {% endif %}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {{ get_create_view_as_sql(target_relation, sql) }}
  {%- endcall %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ drop_relation_if_exists(backup_relation) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}
