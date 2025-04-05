{% macro fabric__generate_series(upper_bound) %}
    select value as generated_number
    from generate_series(1, {{ upper_bound }})
{% endmacro %}
