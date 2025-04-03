{%- macro fabric__month_name(date, short) -%}
{%- set f = 'MMM' if short else 'MMMM' -%}
    cast(format({{ date }}, '{{ f }}') as varchar(4000))
{%- endmacro %}
