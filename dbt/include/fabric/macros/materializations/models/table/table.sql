{% materialization table, adapter='fabric' %}

  -- Load target relation
  {%- set target_relation = this.incorporate(type='table') %}
  -- Load existing relation
  {%- set relation = fabric__get_relation_without_caching(this) %}

  {% set existing_relation = none %}
  {% if (relation|length == 1) %}
    {% set existing_relation = get_or_create_relation(relation[0][0], relation[0][2] , relation[0][1] , relation[0][3])[1] %}
  {% endif %}

  {%- set backup_relation = none %}
  {{log("Existing Relation type is "~ existing_relation.type)}}
  {% if (existing_relation != none and existing_relation.type == "table") %}
      {%- set backup_relation = make_backup_relation(target_relation, 'table') -%}
  {% elif (existing_relation != none and existing_relation.type == "view") %}
      {%- set backup_relation = make_backup_relation(target_relation, 'view') -%}
  {% endif %}

  {% if (existing_relation != none) %}
    -- drop the temp relations if they exist already in the database
    {{ drop_relation_if_exists(backup_relation) }}
    -- Rename target relation as backup relation
    {{ adapter.rename_relation(existing_relation, backup_relation) }}
  {% endif %}

  -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}

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

  {% if (backup_relation != none) %}
    -- finally, drop the foreign key references if exists
    {{ drop_fk_indexes_on_table(backup_relation) }}
    -- drop existing/backup relation after the commit
    {{ drop_relation_if_exists(backup_relation) }}
   {% endif %}
  -- Add constraints including FK relation.
  {{ fabric__build_model_constraints(target_relation) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
