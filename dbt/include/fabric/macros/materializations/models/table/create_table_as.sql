{% macro fabric__create_table_as(temporary, relation, sql) -%}

   {% set tmp_relation = relation.incorporate(
   path={"identifier": relation.identifier.replace("#", "") ~ '_temp_view'},
   type='view')-%}
   {% do run_query(drop_relation_if_exists(tmp_relation)) %}

   {% set contract_config = config.get('contract') %}

    {{ create_view_as(tmp_relation, sql) }}
    {% if contract_config.enforced %}

        CREATE TABLE [{{relation.database}}].[{{relation.schema}}].[{{relation.identifier}}]
        {{ fabric__build_columns_constraints(relation) }}
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

    {{ drop_relation_if_exists(tmp_relation) }}

{% endmacro %}
