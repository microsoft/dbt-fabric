{% macro build_cluster_by_clause(temporary) %}
    {{ return(adapter.dispatch('build_cluster_by_clause', 'dbt')(temporary)) }}
{% endmacro %}

{% macro fabric__build_cluster_by_clause(temporary) %}
    {%- if not temporary -%}
        {%- set cluster_by = config.get('cluster_by') -%}
        {%- if cluster_by is not none -%}
            {%- if cluster_by is string -%}
                {%- set cluster_by = [cluster_by] -%}
            {%- endif -%}
            {%- set quoted_columns = [] -%}
            {%- for col in cluster_by -%}
                {%- do quoted_columns.append('[' ~ col | replace(']', ']]') ~ ']') -%}
            {%- endfor -%}
            WITH (CLUSTER BY ({{ quoted_columns | join(', ') }}))
        {%- endif -%}
    {%- endif -%}
{% endmacro %}


{% macro fabric__create_table_as(temporary, relation, compiled_code, language='sql') -%}
    {%- if language != 'sql' -%}
        {% do exceptions.raise_compiler_error("fabric__create_table_as macro didn't get supported language, it got %s" % language) %}
    {%- endif -%}
    {% set contract_config = config.get('contract') %}

    {% if contract_config.enforced %}

        CREATE TABLE {{relation}}
        {{ build_columns_constraints(relation) }}
        {{ get_assert_columns_equivalent(compiled_code)  }}
        {{ build_cluster_by_clause(temporary) }}

        {% set listColumns %}
            {% for column in model['columns'] %}
                {{ "["~column | replace(']', ']]')~"]" }}{{ ", " if not loop.last }}
            {% endfor %}
        {%endset%}

        {% set tmp_vw_relation = relation.incorporate(path={"identifier": relation.identifier ~ '__dbt_tmp_vw'}, type='view')-%}
        {% do adapter.drop_relation(tmp_vw_relation) %}
        {{ get_create_view_as_sql(tmp_vw_relation, compiled_code) }}

        INSERT INTO {{relation}} ({{listColumns}})
        SELECT {{listColumns}} FROM {{tmp_vw_relation}};
        DROP VIEW IF EXISTS {{ tmp_vw_relation.include(database=False) }};
    {%- else %}

        CREATE TABLE {{relation}}
        {{ build_cluster_by_clause(temporary) }}
        AS {{compiled_code}}

    {% endif %}
{% endmacro %}
