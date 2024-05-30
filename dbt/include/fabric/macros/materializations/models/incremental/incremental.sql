{% materialization incremental, adapter='fabric' -%}

  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {% set target_relation = this.incorporate(type='table') %}
  {%- set relation = load_cached_relation(this) -%}

  {%- set existing_relation = none %}
  {% if relation.type ==  'table' %}
    {% set existing_relation = target_relation %}
  {% elif relation.type ==  'view' %}
    {% set existing_relation = get_or_create_relation(relation.database, relation.schema, relation.identifier, relation.type)[1] %}
  {% endif %}

  -- configs
  {%- set unique_key = config.get('unique_key') -%}
  {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
  {%- set temp_relation = make_temp_relation(target_relation)-%}

  {% set grant_config = config.get('grants') %}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- naming a temp relation
  {% set tmp_relation_view = target_relation.incorporate(path={"identifier": target_relation.identifier ~ '__dbt_tmp_vw'}, type='view')-%}

  -- Fabric & Synapse adapters use temp relation because of lack of CTE support for CTE in CTAS, Insert
  -- drop temp relation if exists
  {% do adapter.drop_relation(tmp_relation_view) %}

  {% if existing_relation is none %}
    {%- call statement('main') -%}
      {{ get_create_table_as_sql(False, target_relation, sql)}}
    {%- endcall -%}

  {% elif existing_relation.is_view %}
    {#-- Can't overwrite a view with a table - we must drop --#}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
    {% do adapter.drop_relation(existing_relation) %}

    {%- call statement('main') -%}
      {{ get_create_table_as_sql(False, target_relation, sql)}}
    {%- endcall -%}

  {% elif full_refresh_mode %}
    {%- call statement('main') -%}
      {{ get_create_table_as_sql(False, target_relation, sql)}}
    {%- endcall -%}

  {% else %}
    {%- call statement('create_tmp_relation') -%}
      {{ get_create_table_as_sql(True, temp_relation, sql)}}
    {%- endcall -%}
    {% do adapter.expand_target_column_types(
              from_relation=temp_relation,
              to_relation=target_relation) %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) %}
    {% set strategy_arg_dict = ({'target_relation': target_relation, 'temp_relation': temp_relation, 'unique_key': unique_key, 'dest_columns': dest_columns, 'incremental_predicates': incremental_predicates }) %}
    {%- call statement('main') -%}
      {{ strategy_sql_macro_func(strategy_arg_dict) }}
    {%- endcall -%}
  {% endif %}

  {% do adapter.drop_relation(tmp_relation_view) %}
  {% do adapter.drop_relation(temp_relation) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% set target_relation = target_relation.incorporate(type='table') %}
  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
  {% do persist_docs(target_relation, model) %}
  {% do adapter.commit() %}
  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
