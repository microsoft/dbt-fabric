{% materialization table, adapter='fabric' %}

  {%- set language = model['language'] -%}

  {# Load target relation #}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_cached_relation(this) -%}

  {# Making an intermediate relation #}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}

  {# Cleanup backup_relations left over from older versions of dbt-fabric. Consider removing for performance reasons in the future.#}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {# Drop intermediate relation if it exists before materializing intermediate relation. Consider removing for performance reasons in the future - as the intermediate relation no longer is ever committed with that name. #}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {# `BEGIN` happens here: #}
  {% call statement('transaction', language=language, autobegin=True) -%}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {# build model #}
    {% call statement('main', language=language) -%}
      {{ create_table_as(False, intermediate_relation, compiled_code, language) }}
    {% endcall %}

    {% do create_indexes(intermediate_relation) %}

    {% if existing_relation is not none %}
      {{ adapter.drop_relation(existing_relation) }}
    {% endif %}

    {# Renaming intermediate relation as main relation #}
    {{ adapter.rename_relation(intermediate_relation, target_relation) }}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {% set grant_config = config.get('grants') %}
    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {% do persist_docs(target_relation, model) %}
    {# `COMMIT` happens here #}
    {{ adapter.commit() }}
  {% endcall %}

  {# Add constraints including FK relation. #}
  {{ build_model_constraints(target_relation) }}
  {{ create_or_update_statistics(target_relation) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
