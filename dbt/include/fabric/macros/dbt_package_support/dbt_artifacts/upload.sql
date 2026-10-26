{% macro fabric__insert_into_metadata_table(relation, fields, content) %}
    {% do run_query("insert into " ~ relation ~ " " ~ fields ~ "\n" ~ content) %}
{% endmacro %}

{% macro fabric__get_invocations_dml_sql(invocation_args=invocation_args_dict) %}
    {{ return(dbt_artifacts.sqlserver__get_invocations_dml_sql(invocation_args)) }}
{% endmacro %}

{% macro fabric__get_exposures_dml_sql(exposures) %}
    {{ return(dbt_artifacts.sqlserver__get_exposures_dml_sql(exposures)) }}
{% endmacro %}

{% macro fabric__get_models_dml_sql(models) %}
    {{ return(dbt_artifacts.sqlserver__get_models_dml_sql(models)) }}
{% endmacro %}

{% macro fabric__get_seeds_dml_sql(seeds) %}
    {{ return(dbt_artifacts.sqlserver__get_seeds_dml_sql(seeds)) }}
{% endmacro %}

{% macro fabric__get_snapshots_dml_sql(snapshots) %}
    {{ return(dbt_artifacts.sqlserver__get_snapshots_dml_sql(snapshots)) }}
{% endmacro %}

{% macro fabric__get_sources_dml_sql(sources) %}
    {{ return(dbt_artifacts.sqlserver__get_sources_dml_sql(sources)) }}
{% endmacro %}

{% macro fabric__get_tests_dml_sql(tests) %}
    {{ return(dbt_artifacts.sqlserver__get_tests_dml_sql(tests)) }}
{% endmacro %}

{% macro fabric__get_model_executions_dml_sql(models) %}
    {{ return(fabric_artifacts_execution_sql(models, "model")) }}
{% endmacro %}

{% macro fabric__get_seed_executions_dml_sql(seeds) %}
    {{ return(fabric_artifacts_execution_sql(seeds, "seed")) }}
{% endmacro %}

{% macro fabric__get_snapshot_executions_dml_sql(snapshots) %}
    {{ return(fabric_artifacts_execution_sql(snapshots, "snapshot")) }}
{% endmacro %}

{% macro fabric__get_test_executions_dml_sql(tests) %}
    {{ return(fabric_artifacts_execution_sql(tests, "test")) }}
{% endmacro %}

{% macro fabric_artifacts_literal(value) -%}
    {{ return("null" if value is none else "'" ~ (value | string | replace("'", "''")) ~ "'") }}
{%- endmacro %}

{% macro fabric_artifacts_execution_sql(nodes, resource_type) %}
    {% if not nodes %}{{ return("") }}{% endif %}
    {% set column_count = 13 if resource_type == "test" else 16 %}
    select * from (values
    {% for node in nodes %}
        {% set compiled = node.timing | selectattr("name", "equalto", "compile") | first | default({}) %}
        {% set executed = node.timing | selectattr("name", "equalto", "execute") | first | default({}) %}
        {% set full_refresh = node.node.config.full_refresh %}
        {% if full_refresh is none %}{% set full_refresh = flags.FULL_REFRESH %}{% endif %}
        (
            {{ fabric_artifacts_literal(invocation_id) }},
            {{ fabric_artifacts_literal(node.node.unique_id) }},
            {{ fabric_artifacts_literal(run_started_at) }},
            {{ 1 if full_refresh else 0 }},
            {{ fabric_artifacts_literal(node.thread_id) }},
            {{ fabric_artifacts_literal(node.status) }},
            {{ fabric_artifacts_literal(compiled.started_at | default(none)) }},
            {{ fabric_artifacts_literal(executed.completed_at | default(none)) }},
            {{ fabric_artifacts_literal(node.execution_time) }},
            {{ fabric_artifacts_literal(node.adapter_response.rows_affected | default(none)) }},
            {% if resource_type == "test" %}
                {{ fabric_artifacts_literal(none if node.failures == "" else node.failures) }},
            {% else %}
                {{ fabric_artifacts_literal(node.node.config.materialized) }},
                {{ fabric_artifacts_literal(node.node.schema) }},
                {{ fabric_artifacts_literal(node.node.name) }},
                {{ fabric_artifacts_literal(node.node.alias) }},
            {% endif %}
            {{ fabric_artifacts_literal(node.message) }},
            {{ fabric_artifacts_literal(tojson(node.adapter_response)) }}
        ){% if not loop.last %},{% endif %}
    {% endfor %}
    ) v(
        {% for column in range(column_count) %}
            [c{{ column }}]{% if not loop.last %},{% endif %}
        {% endfor %}
    )
{% endmacro %}
