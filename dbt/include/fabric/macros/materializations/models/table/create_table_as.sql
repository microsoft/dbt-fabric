{% macro fabric__create_table_as(temporary, relation, sql) -%}

    -- Make temp view relation
    {% set tmp_vw_relation = relation.incorporate(path={"identifier": relation.identifier ~ '__dbt_tmp_vw'}, type='view')-%}

    -- drop temp relation if exists
    {% do adapter.drop_relation(tmp_vw_relation) %}

    -- Fabric & Synapse adapters use temp relation because of lack of CTE support for CTE in CTAS, Insert
    {{ get_create_view_as_sql(tmp_vw_relation, sql) }}

    -- Temp relation will come in use when contract is enforced or not
    -- Making a temp relation
    {% set temp_relation = make_temp_relation(relation, '__dbt_temp') %}

    -- Dropping a temp relation if it exists
    {% do adapter.drop_relation(temp_relation) %}

    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}

        CREATE TABLE {{temp_relation}}
        {{ build_columns_constraints(relation) }}
        {{ get_assert_columns_equivalent(sql)  }}
        {% set listColumns %}
            {% for column in model['columns'] %}
                {{ "["~column~"]" }}{{ ", " if not loop.last }}
            {% endfor %}
        {%endset%}

        INSERT INTO {{temp_relation}} ({{listColumns}})
        SELECT {{listColumns}} FROM {{tmp_vw_relation}};

    {%- else %}
        -- CTAS
        {{ log("In CTAS") }}
        {# EXEC('CREATE TABLE {{temp_relation}} AS SELECT * FROM {{tmp_vw_relation}} }}'); #}
        SELECT 1
        EXEC('CREATE TABLE [{{temp_relation.database}}].[{{temp_relation.schema}}].[{{temp_relation.identifier}}] AS (SELECT * FROM [{{tmp_vw_relation.database}}].[{{tmp_vw_relation.schema}}].[{{tmp_vw_relation.identifier}}]);');
    {% endif %}

    -- making a backup relation, this will come in use when contract is enforced or not
    {%- set backup_relation = make_backup_relation(relation, 'table') -%}

    -- Dropping a temp relation if it exists
    {% do adapter.drop_relation(backup_relation) %}

    {%- set check_if_relation_exists = adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) -%}
    {% if check_if_relation_exists is not none %}

        {{ adapter.rename_relation(relation, backup_relation) }}

        -- Renaming temp relation as main relation
        {{ adapter.rename_relation(temp_relation, relation) }}

        -- Drop backup relation
        {% do adapter.drop_relation(backup_relation) %}

    {%- else %}

        -- Renaming temp relation as main relation
        {{ adapter.rename_relation(temp_relation, relation) }}

        -- Dropping temp relation
        {% do adapter.drop_relation(temp_relation) %}

    {% endif %}

    -- Dropping temp view relation
    {% do adapter.drop_relation(tmp_vw_relation)%}
{% endmacro %}
