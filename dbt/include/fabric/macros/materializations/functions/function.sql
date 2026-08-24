{% materialization function, adapter='fabric', supported_languages=['sql', 'python'] %}
    {% set existing_relation = load_cached_relation(this) %}
    {% set target_relation = this.incorporate(type=this.Function) %}
    {% set grant_config = config.get('grants') %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set function_config = this.get_function_config(model) %}
    {% set macro_name = this.get_function_macro_name(function_config) %}
    {% set _dispatch = adapter.dispatch %}
    {% set build_sql = _dispatch(macro_name, 'dbt')(target_relation) %}

    {% call statement(name='main') %}
        {{ build_sql }}
    {% endcall %}

    {% set should_revoke = should_revoke(
        existing_relation,
        full_refresh_mode=True
    ) %}
    {% do apply_grants(
        target_relation,
        grant_config,
        should_revoke=should_revoke
    ) %}
    {% do persist_docs(target_relation, model) %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {% do adapter.commit() %}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
