{% macro fabric__get_incremental_default_sql(arg_dict) %}

    {% if arg_dict["unique_key"] %}
        -- Delete + Insert Strategy, calls get_delete_insert_merge_sql
        {% do return(get_incremental_delete_insert_sql(arg_dict)) %}
    {% else %}
        -- Incremental Append will insert data into target table.
        {% do return(get_incremental_append_sql(arg_dict)) %}
    {% endif %}

{% endmacro %}

{% macro fabric__get_incremental_merge_sql(arg_dict) %}

    {%- set target = arg_dict["target_relation"] -%}
    {%- set source = arg_dict["temp_relation"] -%}
    {%- set unique_key = arg_dict["unique_key"] -%}
    {%- set dest_columns = arg_dict["dest_columns"] -%}
    {%- set incremental_predicates = [] if arg_dict.get('incremental_predicates') is none else arg_dict.get('incremental_predicates') -%}

    {% do return(fabric__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates)) %}

{% endmacro %}
