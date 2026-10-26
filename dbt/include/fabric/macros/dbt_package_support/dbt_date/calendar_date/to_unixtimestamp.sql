{%- macro fabric__to_unixtimestamp(timestamp) -%}
    datediff_big(second, cast('1970-01-01' as datetime2(6)), {{ timestamp }})
{%- endmacro %}
