{% macro fabric__create_table_as(temporary, relation, sql) -%}
    {% set query_label = apply_label() %}
    {% set tmp_vw_relation = relation.incorporate(path={"identifier": relation.identifier ~ '__dbt_tmp_vw'}, type='view')-%}
    {% do adapter.drop_relation(tmp_vw_relation) %}
    {{ get_create_view_as_sql(tmp_vw_relation, sql) }}

    {# Build CLUSTER BY clause - only for non-temporary tables #}
    {% set cluster_by_clause = fabric__build_cluster_by_clause(temporary) %}

    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}

        CREATE TABLE {{relation}}
        {{ build_columns_constraints(relation) }}
        {{ cluster_by_clause }}
        {{ get_assert_columns_equivalent(sql)  }}
        {% set listColumns %}
            {% for column in model['columns'] %}
                {{ "["~column~"]" }}{{ ", " if not loop.last }}
            {% endfor %}
        {%endset%}

        {% if not adapter.behavior.empty.no_warn %}
            INSERT INTO {{relation}} ({{listColumns}})
            SELECT {{listColumns}} FROM {{tmp_vw_relation}} {{ query_label }}
        {% endif %}

    {%- else %}
        {%- set query_label_option = query_label.replace("'", "''") -%}
        {% if adapter.behavior.empty.no_warn %}
            EXEC('CREATE TABLE {{relation}} {{ cluster_by_clause }} AS SELECT * FROM {{tmp_vw_relation}} WHERE 0=1 {{ query_label_option }}');
        {% else %}
            EXEC('CREATE TABLE {{relation}} {{ cluster_by_clause }} AS SELECT * FROM {{tmp_vw_relation}} {{ query_label_option }}');
        {% endif %}
    {% endif %}

{% endmacro %}

{#
    Builds a WITH (CLUSTER BY (...)) clause for Fabric Data Warehouse tables.
    See: https://learn.microsoft.com/en-us/fabric/data-warehouse/data-clustering

    Limitations enforced:
      - Maximum of 4 columns allowed in CLUSTER BY.
      - Skipped for temporary tables (clustering is only applied at final table creation).
      - Clustering must be defined at table creation; it cannot be added to existing tables via ALTER.
      - Supported column types: bigint, int, smallint, decimal, numeric, float, real,
        date, datetime2, time, char, varchar. Columns of unsupported types (bit, varchar(max),
        varbinary, uniqueidentifier) cannot be used in CLUSTER BY.
      - IDENTITY columns cannot be used with CLUSTER BY.

    Config usage:
      - Model level:  {{ config(cluster_by=['col1', 'col2']) }}
      - Project level: +cluster_by: ['col1', 'col2']
      - Single column shorthand: {{ config(cluster_by='col1') }}
#}
{% macro fabric__build_cluster_by_clause(temporary) %}
    {%- set cluster_by = config.get('cluster_by') -%}
    {%- if cluster_by is not none and not temporary -%}
        {%- if cluster_by is string -%}
            {%- set cluster_by = [cluster_by] -%}
        {%- endif -%}

        {%- if cluster_by | length < 1 -%}
            {%- do exceptions.raise_compiler_error(
                "CLUSTER BY requires at least one column."
            ) -%}
        {%- endif -%}

        {%- if cluster_by | length > 4 -%}
            {%- do exceptions.raise_compiler_error(
                "Fabric Data Warehouse supports a maximum of 4 columns in CLUSTER BY. Got "
                ~ cluster_by | length ~ " columns: " ~ cluster_by | join(', ') ~ "."
            ) -%}
        {%- endif -%}

        WITH (CLUSTER BY ({{ cluster_by | join(', ') }}))
    {%- endif -%}
{% endmacro %}
