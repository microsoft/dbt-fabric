{#- T-SQL does not allow CTEs inside subqueries, and the integration test
    models wrap the macro output in FROM (...). Uses inline subqueries and
    CROSS APPLY VALUES to unpivot without CTEs. -#}
{% macro fabric__compare_which_query_columns_differ(a_query, b_query, primary_key_columns, columns, event_time) %}
    {% set columns = audit_helper._ensure_all_pks_are_in_column_set(primary_key_columns, columns) %}
    {% if event_time %}
        {% set event_time_props = audit_helper._get_comparison_bounds(a_query, b_query, event_time) %}
    {% endif %}

    {% set quoted_columns = [] %}
    {% set quoted_keys = [] %}
    {% for column in columns %}{% do quoted_columns.append(adapter.quote(column)) %}{% endfor %}
    {% for column in primary_key_columns %}{% do quoted_keys.append(adapter.quote(column)) %}{% endfor %}
    {% set joined_cols = quoted_columns | join(", ") %}

    select v.column_name, v.has_difference
    from (
        select
            {% for column in columns %}
                {% set quoted_column = adapter.quote(column) %}
                MAX(
                    CASE
                        WHEN (
                            (a.{{ quoted_column }} != b.{{ quoted_column }})
                            or (a.{{ quoted_column }} is null and b.{{ quoted_column }} is not null)
                            or (a.{{ quoted_column }} is not null and b.{{ quoted_column }} is null)
                        ) THEN 1
                        ELSE 0
                    END
                ) as column_{{ loop.index }}_has_difference
                {%- if not loop.last %}, {% endif %}
            {% endfor %}
        from (
            select
                {{ joined_cols }},
                {{ audit_helper._generate_null_safe_surrogate_key(quoted_keys) }} as dbt_audit_surrogate_key
            from ({{ a_query }}) as a_subq
            {{ audit_helper.event_time_filter(event_time_props) }}
        ) a
        inner join (
            select
                {{ joined_cols }},
                {{ audit_helper._generate_null_safe_surrogate_key(quoted_keys) }} as dbt_audit_surrogate_key
            from ({{ b_query }}) as b_subq
            {{ audit_helper.event_time_filter(event_time_props) }}
        ) b on a.dbt_audit_surrogate_key = b.dbt_audit_surrogate_key
    ) calculated
    cross apply (values
        {% for column in columns %}
            ('{{ column | replace("'", "''") }}', column_{{ loop.index }}_has_difference)
            {%- if not loop.last %}, {% endif %}
        {% endfor %}
    ) v(column_name, has_difference)

{% endmacro %}
