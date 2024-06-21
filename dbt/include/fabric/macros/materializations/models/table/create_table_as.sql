{% macro fabric__create_table_as(temporary, relation, sql) -%}

    {% set tmp_vw_relation = relation.incorporate(path={"identifier": relation.identifier ~ '__dbt_tmp_vw'}, type='view')-%}
    {% do adapter.drop_relation(tmp_vw_relation) %}
    {{ get_create_view_as_sql(tmp_vw_relation, sql) }}

    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}

        CREATE TABLE {{relation}}
        {{ build_columns_constraints(relation) }}
        {{ get_assert_columns_equivalent(sql)  }}
        {% set listColumns %}
            {% for column in model['columns'] %}
                {{ "["~column~"]" }}{{ ", " if not loop.last }}
            {% endfor %}
        {%endset%}

        INSERT INTO {{relation}} ({{listColumns}})
        SELECT {{listColumns}} FROM {{tmp_vw_relation}};

    {%- else %}
        EXEC('CREATE TABLE {{relation}} AS SELECT * FROM {{tmp_vw_relation}};');
    {% endif %}
{% endmacro %}
