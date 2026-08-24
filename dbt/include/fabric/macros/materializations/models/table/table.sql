{% materialization table, adapter='fabric' %}

  {%- set language = model['language'] -%}

  {# Load target relation #}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_cached_relation(this) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {# `BEGIN` happens here: #}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set refresh_plan = fabric_full_refresh_table(
      target_relation,
      existing_relation,
      compiled_code,
      language
  ) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set grant_config = config.get('grants') %}
  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {# Add or reconcile constraints including FK relations. #}
  {% if refresh_plan['action'] == 'reload' %}
    {{ reconcile_model_constraints(target_relation, refresh_plan) }}
  {% else %}
    {{ build_model_constraints(target_relation) }}
  {% endif %}
  {{ create_or_update_statistics(
      target_relation,
      existing_table=(refresh_plan['action'] == 'reload')
  ) }}

  {# `COMMIT` happens here #}
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
