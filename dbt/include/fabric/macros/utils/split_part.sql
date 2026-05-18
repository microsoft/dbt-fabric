{% macro fabric__split_part(string_text, delimiter_text, part_number) %}
    (
        select value
        from (
            select
                value,
                {% if part_number > 0 %}
                row_number() over (order by ordinal asc) as idx
                {% else %}
                row_number() over (order by ordinal desc) as idx
                {% endif %}
            from string_split({{ string_text }}, {{ delimiter_text }}, 1)
        ) as _split_data
        where idx = {{ (part_number * -1) if part_number < 0 else part_number }}
    )
{% endmacro %}
