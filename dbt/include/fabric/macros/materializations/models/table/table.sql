{% materialization table, adapter='fabric' %}

  -- Create target relation
  {%- set target_relation = this.incorporate(type='table') %}
  -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}
  {%- set backup_relation = make_backup_relation(target_relation, 'table') -%}
  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(backup_relation) }}
  -- Rename target relation as backup relation
  {%- set relation = fabric__get_relation_without_caching(target_relation) %}
  {% if relation|length > 0 %}
    {{ adapter.rename_relation(target_relation, backup_relation) }}
  {% endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {{ get_create_table_as_sql(False, target_relation, sql) }}
  {%- endcall %}

  -- cleanup
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
  {% do persist_docs(target_relation, model) %}
  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the foreign key references if exists
  {{ drop_fk_indexes_on_table(backup_relation) }}
  -- drop existing/backup relation after the commit
  {{ drop_relation_if_exists(backup_relation) }}
  -- Add constraints including FK relation.
  {{ fabric__build_model_constraints(target_relation) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
