{% macro check_for_nested_cte(sql) %}
    {% if execute %}  {# Ensure this runs only at execution time #}
        {% set cleaned_sql = sql | lower | replace("\n", " ") %}  {# Convert to lowercase and remove newlines #}
        {% set cte_count = cleaned_sql.count("with ") %}  {# Count occurrences of "WITH " #}
        {% if cte_count > 1 %}
            {{ return(True) }}
        {% else %}
            {{ return(False) }}  {# No nested CTEs found #}
        {% endif %}
    {% else %}
        {{ return(False) }}  {# Return False during parsing #}
    {% endif %}
{% endmacro %}


{% macro fabric__create_table_as(temporary, relation, sql) -%}

    {% set query_label = apply_label() %}
    {% set contract_config = config.get('contract') %}
    {% set is_nested_cte = check_for_nested_cte(sql) %}

    {% if is_nested_cte and contract_config.enforced %}

        {{ exceptions.raise_compiler_error(
            "Since the contract is enforced and the model contains a nested CTE, Fabric DW uses CREATE TABLE + INSERT to load data.
            INSERT INTO is not supported with nested CTEs. To resolve this, either disable contract enforcement or modify the model."
        ) }}

    {%- elif not is_nested_cte and contract_config.enforced %}

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
