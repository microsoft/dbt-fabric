{% macro fabric__get_limit_sql(sql, limit) %}

    {% if limit == -1 or limit is none %}
        {% if sql.strip().lower().startswith('with') %}
            {{ sql }}
        {% else -%}
            select *
            from (
                {{ sql }}
            ) as model_limit_subq
        {%- endif -%}
    {% else -%}
        {% if sql.strip().lower().startswith('with') %}
            {{ sql }} order by (select null)
            offset 0 rows fetch first {{ limit }} rows only
        {% else -%}
            select *
            from (
                {{ sql }}
            ) as model_limit_subq order by (select null)
            offset 0 rows fetch first {{ limit }} rows only
        {%- endif -%}
    {%- endif -%}
{% endmacro %}
