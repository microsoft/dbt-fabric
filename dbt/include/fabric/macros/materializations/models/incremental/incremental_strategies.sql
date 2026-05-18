{% macro fabric__get_incremental_default_sql(arg_dict) %}

    {% if arg_dict["unique_key"] %}
        -- Delete + Insert Strategy, calls get_delete_insert_merge_sql
        {% do return(get_incremental_merge_sql(arg_dict)) %}
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
    {%- set incremental_predicates = arg_dict.get("incremental_predicates") -%}
    {%- set delete_condition = arg_dict.get("delete_condition") -%}
    {%- set delete_not_matched_by_source = arg_dict.get("delete_not_matched_by_source", false) -%}

    {% if delete_not_matched_by_source %}
        {{ fabric__get_merge_delete_not_matched_sql(target, source, unique_key, dest_columns, incremental_predicates) }}
    {% else %}
        {{ fabric__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) }}
        {% if delete_condition %}
            {{ fabric__get_merge_delete_condition_sql(target, source, unique_key, delete_condition) }}
        {% endif %}
    {% endif %}

{% endmacro %}
