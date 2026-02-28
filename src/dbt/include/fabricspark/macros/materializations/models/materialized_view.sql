{% materialization materialized_view, default %}
    {% set existing_relation = load_cached_relation(this) %}
    {% set target_relation = this.incorporate(type=this.MaterializedView) %}
    {% set intermediate_relation = make_intermediate_relation(target_relation) %}
    {% if existing_relation is not none %}
        {% set backup_relation = make_backup_relation(target_relation, existing_relation.type) %}
    {% else %}
        {% set backup_relation = none %}
    {% endif %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    {% set full_refresh_mode = should_full_refresh() %}

    {# determine the scenario we're in: create, replace, or refresh #}
    {% if existing_relation is not none %}
        {% set has_configuration_changes = get_materialized_view_configuration_changes(existing_relation, config) %}
    {% else %}
        {% set has_configuration_changes = false %}
    {% endif %}

    {% if existing_relation is none %}
        {% set build_sql = get_create_materialized_view_as_sql(target_relation, sql) %}
        {% set needs_swap = false %}
    {% elif full_refresh_mode or not existing_relation.is_materialized_view or has_configuration_changes %}
        {# full refresh, type mismatch, or config changed: drop and replace #}
        {% set build_sql = get_replace_sql(existing_relation, intermediate_relation, sql) %}
        {% set needs_swap = true %}
    {% else %}
        {# no changes: refresh is a no-op in terms of SQL #}
        {% set build_sql = refresh_materialized_view(target_relation) %}
        {% set needs_swap = false %}
    {% endif %}
    
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set grant_config = config.get('grants') %}

    {% call statement(name="main") %}
        {{ build_sql }}
    {% endcall %}

    {# Atomic swap: rename existing → backup, intermediate → target #}
    {% if needs_swap %}
        {% if backup_relation is not none %}
            {{ adapter.rename_relation(existing_relation, backup_relation) }}
        {% endif %}
        {{ adapter.rename_relation(intermediate_relation, target_relation) }}
    {% endif %}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {{ drop_relation_if_exists(backup_relation) }}
    {{ drop_relation_if_exists(intermediate_relation) }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}