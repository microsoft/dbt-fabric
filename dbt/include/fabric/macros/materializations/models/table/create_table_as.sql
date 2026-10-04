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
    {% set identity_column = adapter.get_identity_column(model['columns'], contract_config.enforced) %}
    {{ fabric_assert_identity_supported(identity_column) }}

    {% if contract_config.enforced %}

        CREATE TABLE {{relation}}
        {{ build_columns_constraints(relation) }}
        {{ get_assert_columns_equivalent(compiled_code)  }}
        {{ build_cluster_by_clause(temporary) }}

        {% set all_column_names = [] %}
        {% for column in model['columns'] %}
            {% do all_column_names.append(column) %}
        {% endfor %}
        {% set listColumns = fabric_identity_insert_columns(all_column_names, identity_column) %}

        {% set tmp_vw_relation = relation.incorporate(path={"identifier": relation.identifier ~ '__dbt_tmp_vw'}, type='view')-%}
        {% do adapter.drop_relation(tmp_vw_relation) %}
        {{ get_create_view_as_sql(tmp_vw_relation, compiled_code) }}

        {% if identity_column is not none and identity_column.mode == 'insert' %}
        SET IDENTITY_INSERT {{ relation }} ON;
        {% endif %}
        INSERT INTO {{relation}} ({{listColumns}})
        SELECT {{listColumns}} FROM {{tmp_vw_relation}};
        {% if identity_column is not none and identity_column.mode == 'insert' %}
        SET IDENTITY_INSERT {{ relation }} OFF;
        {#- Fabric Data Warehouse only supports the bare RESEED form: it computes the
            next value internally and rejects an explicit new_reseed_value (unlike
            SQL Server/Synapse). See:
            https://learn.microsoft.com/en-us/fabric/data-warehouse/identity#reseed-identity-values-with-dbcc-checkident -#}
        DBCC CHECKIDENT ('{{ relation | replace("'", "''") }}', RESEED);
        {% endif %}
        DROP VIEW IF EXISTS {{ tmp_vw_relation.include(database=False) }};
    {%- else %}

        CREATE TABLE {{relation}}
        {{ build_cluster_by_clause(temporary) }}
        AS {{compiled_code}}

    {% endif %}
{% endmacro %}
