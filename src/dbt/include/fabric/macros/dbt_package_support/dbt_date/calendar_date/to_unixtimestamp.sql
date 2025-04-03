{%- macro fabric__to_unixtimestamp(timestamp) -%}
    DATEDIFF(s, '1970-01-01 00:00:00', {{ timestamp }})
{%- endmacro %}