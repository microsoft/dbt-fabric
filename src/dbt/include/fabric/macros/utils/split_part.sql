{% macro fabric__split_part(string_text, delimiter_text, part_number) %}
    (
        select value
        from
            (select 
                value,
                row_number() over (order by ordinal asc) as forward_index,
                row_number() over (order by ordinal desc) as backward_index
            from string_split({{ string_text }}, {{ delimiter_text }}, 1)) as SplitData
        where {% if part_number > 0 %}forward{% else %}backward{% endif %}_index = {{ part_number|abs }}
    )
{% endmacro %}
