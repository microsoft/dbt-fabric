{% materialization view, adapter='fabric' -%}

  {%- set target_relation = this.incorporate(type='view') -%}
  {{log("Target Relation "~target_relation)}}

  {%- set relation = fabric__get_relation_without_caching(this) %}
  {% set existing_relation = none %}
  {% if (relation|length == 1) %}
    {% set existing_relation = get_or_create_relation(relation[0][0], relation[0][2] , relation[0][1] , relation[0][3])[1] %}
  {% endif %}
  {{log("Existing Relation "~existing_relation)}}

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

  {% set grant_config = config.get('grants') %}
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

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
  {% if (backup_relation != none) %}
    {{ drop_relation_if_exists(backup_relation) }}
  {% endif %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}
