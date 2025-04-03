{%- macro fabric__iso_week_start(date) -%}
    cast(dateadd(week, datediff(week, 0, dateadd(day, -1, {{date}})), 0) as date)
{%- endmacro %}
