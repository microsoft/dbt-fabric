{% macro fabric__get_limit_sql(sql, limit) %}

    {% if limit == -1 or limit is none %}
        with model_limit_subq as (
            {{ sql }}
        )
        select *
        from model_limit_subq;
    {% else -%}
        with model_limit_subq as (
            {{ sql }}
        )
        select top {{ limit }} *
        from model_limit_subq;
    {%- endif -%}
{% endmacro %}
