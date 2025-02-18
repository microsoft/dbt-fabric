{% macro fabric__create_table_as(temporary, relation, sql) -%}

    {% set query_label = apply_label() %}
    {% set contract_config = config.get('contract') %}

    {% if sql.strip().lower().startswith('with') and contract_config.enforced %}

        {{ exceptions.raise_compiler_error(
            "As contract is enforced and the model is using CTE, INSERT INTO is not supported with CTE. Either do not enforce contract or change the model"
        ) }}

    {%- elif not sql.strip().lower().startswith('with') and contract_config.enforced %}

        CREATE TABLE {{relation}}
        {{ build_columns_constraints(relation) }}
        {{ get_assert_columns_equivalent(sql)  }}

        {% set listColumns %}
            {% for column in model['columns'] %}
                {{ "["~column~"]" }}{{ ", " if not loop.last }}
            {% endfor %}
        {%endset%}

        {% set tmp_vw_relation = relation.incorporate(path={"identifier": relation.identifier ~ '__dbt_tmp_vw'}, type='view')-%}
        {% do adapter.drop_relation(tmp_vw_relation) %}
        {{ get_create_view_as_sql(tmp_vw_relation, sql) }}

        INSERT INTO {{relation}} ({{listColumns}})
        SELECT {{listColumns}} FROM {{tmp_vw_relation}} {{ query_label }}

    {%- else %}

        {%- set query_label_option = query_label.replace("'", "''") -%}
        {%- set sql_with_quotes = sql.replace("'", "''") -%}
        EXEC('CREATE TABLE {{relation}} AS {{sql_with_quotes}} {{ query_label_option }}');

    {% endif %}
{% endmacro %}
