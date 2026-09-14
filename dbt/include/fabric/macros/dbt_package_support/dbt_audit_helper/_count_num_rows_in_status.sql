{#- T-SQL rejects `count(distinct a, b) over (...)`: COUNT(DISTINCT) takes a
    single argument and is not allowed as a window function. The package ships a
    distinct-free equivalent for exactly this case, which postgres and databricks
    also dispatch to. -#}
{% macro fabric___count_num_rows_in_status() %}
    {{ audit_helper._count_num_rows_in_status_without_distinct_window_func() }}
{% endmacro %}
