{% materialization table, adapter='fabric' %}

  -- Load target relation
  {%- set target_relation = this.incorporate(type='table') %}
  {%- set existing_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) -%}

  {%- set backup_relation = none %}
  {% if (existing_relation != none and existing_relation.type == "table") %}
      {%- set backup_relation = make_backup_relation(target_relation, 'table') -%}
  {% elif (existing_relation != none and existing_relation.type == "view") %}
      {%- set backup_relation = make_backup_relation(target_relation, 'view') -%}
  {% endif %}

  {% if (existing_relation != none) %}
    -- drop the temp relations if they exist already in the database
    {% do adapter.drop_relation(backup_relation) %}
    -- Rename target relation as backup relation
    {{ adapter.rename_relation(existing_relation, backup_relation) }}
  {% endif %}

  -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- naming a temp relation
  {% set tmp_relation = target_relation.incorporate(path={"identifier": target_relation.identifier ~ '__dbt_tmp_vw'}, type='view')-%}

  -- Fabric & Synapse adapters use temp relation because of lack of CTE support for CTE in CTAS, Insert
  -- drop temp relation if exists
  {% do adapter.drop_relation(tmp_relation) %}

  -- build model
  {% call statement('main') -%}
    {{ get_create_table_as_sql(False, target_relation, sql) }}
  {%- endcall %}

  -- drop temp relation if exists
  {% do adapter.drop_relation(tmp_relation) %}

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
    {% do adapter.drop_relation(backup_relation) %}
   {% endif %}

  -- Add constraints including FK relation.
  {{ build_model_constraints(target_relation) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
