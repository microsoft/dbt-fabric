{%- macro fabric__day_name(date, short) -%}
{%- set f = 'ddd' if short else 'dddd' -%}
    cast(format({{ date }}, '{{ f }}') as varchar(4000))
{%- endmacro %}
