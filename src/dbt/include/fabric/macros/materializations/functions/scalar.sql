{% macro fabric__scalar_function_sql(target_relation) %}
    {{ get_use_database_sql(target_relation.database) }}
    EXEC('
        {{ scalar_function_create_replace_signature_sql(target_relation).replace("'", "''") }}
        {{ scalar_function_body_sql().replace("'", "''") }}
    ');
{% endmacro %}

{% macro fabric__formatted_scalar_function_args_sql() %}
    {% set args = [] %}
    {% for arg in model.arguments -%}
        {%- set arg_str = '@' ~ arg.name ~ ' ' ~ arg.data_type -%}
        {%- if arg.default_value is not none -%}
            {% set arg_str = arg_str ~ ' = ' ~ arg.default_value %}
        {%- endif -%}
        {%- do args.append(arg_str) -%}
    {%- endfor %}
    {{ args | join(', ') }}
{% endmacro %}

{% macro fabric__scalar_function_create_replace_signature_sql(target_relation) %}
    CREATE OR ALTER FUNCTION {{ target_relation.include(database=False) }}
    ({{ formatted_scalar_function_args_sql()}})
    RETURNS {{ model.returns.data_type }}
    AS
{% endmacro %}

{% macro fabric__scalar_function_body_sql() %}
    BEGIN
       RETURN ({{ model.compiled_code }});
    END
{% endmacro %}