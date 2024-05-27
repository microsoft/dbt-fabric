{% macro fabric__create_table_as(temporary, relation, sql) -%}

    {% set tmp_relation = relation.incorporate(path={"identifier": relation.identifier ~ '__dbt_tmp_vw'}, type='view')-%}
    {{ get_create_view_as_sql(tmp_relation, sql) }}

    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}

        CREATE TABLE [{{relation.database}}].[{{relation.schema}}].[{{relation.identifier}}]
        {{ build_columns_constraints(relation) }}
        {{ get_assert_columns_equivalent(sql)  }}
        {% set listColumns %}
            {% for column in model['columns'] %}
                {{ "["~column~"]" }}{{ ", " if not loop.last }}
            {% endfor %}
        {%endset%}

        INSERT INTO [{{relation.database}}].[{{relation.schema}}].[{{relation.identifier}}]
        ({{listColumns}}) SELECT {{listColumns}} FROM [{{tmp_relation.database}}].[{{tmp_relation.schema}}].[{{tmp_relation.identifier}}];

    {%- else %}
        EXEC('CREATE TABLE [{{relation.database}}].[{{relation.schema}}].[{{relation.identifier}}] AS (SELECT * FROM [{{tmp_relation.database}}].[{{tmp_relation.schema}}].[{{tmp_relation.identifier}}]);');
    {% endif %}
    {% do adapter.drop_relation(tmp_relation)%}
{% endmacro %}
