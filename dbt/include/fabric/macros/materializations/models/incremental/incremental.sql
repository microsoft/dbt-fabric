
{% materialization incremental, adapter='fabric' -%}

  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {% set target_relation = this.incorporate(type='table') %}
  {%- set relations_list = fabric__get_relation_without_caching(target_relation) -%}

  {%- set existing_relation = none %}
  {% if (relations_list|length == 1) and (relations_list[0][2] == target_relation.schema)
        and (relations_list[0][1] ==  target_relation.identifier) and  (relations_list[0][3] ==  target_relation.type)%}
    {% set existing_relation = target_relation %}
  {% elif (relations_list|length == 1) and (relations_list[0][2] == target_relation.schema)
        and (relations_list[0][1] ==  target_relation.identifier) and  (relations_list[0][3] !=  target_relation.type) %}
      {% set existing_relation = get_or_create_relation(relations_list[0][0], relations_list[0][2] , relations_list[0][1] , relations_list[0][3])[1] %}
  {% endif %}

  {{ log("Full refresh mode" ~ full_refresh_mode)}}
  {{ log("existing relation : "~existing_relation ~ " type  "~ existing_relation.type ~ " is view?  "~existing_relation.is_view)  }}
  {{ log("target relation: " ~target_relation ~ " type  "~ target_relation.type ~ " is view?  "~target_relation.is_view) }}

  -- configs
  {%- set unique_key = config.get('unique_key') -%}
  {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
  {%- set temp_relation = make_temp_relation(target_relation)-%}

  {% set grant_config = config.get('grants') %}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}

    {%- call statement('main') -%}
      {{ fabric__create_table_as(False, target_relation, sql)}}
    {%- endcall -%}

  {% elif existing_relation.is_view %}

    {#-- Can't overwrite a view with a table - we must drop --#}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
    {{ drop_relation_if_exists(existing_relation) }}
    {%- call statement('main') -%}
      {{ fabric__create_table_as(False, target_relation, sql)}}
    {%- endcall -%}

  {% elif full_refresh_mode %}

    {%- call statement('main') -%}
      {{ fabric__create_table_as(False, target_relation, sql)}}
    {%- endcall -%}

  {% else %}

    {%- call statement('create_tmp_relation') -%}
      {{ fabric__create_table_as(True, temp_relation, sql)}}
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

  {% do drop_relation_if_exists(temp_relation) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set target_relation = target_relation.incorporate(type='table') %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}
  {% do adapter.commit() %}
  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
