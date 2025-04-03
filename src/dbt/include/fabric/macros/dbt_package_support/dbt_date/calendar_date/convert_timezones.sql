{% macro fabric__convert_timezone(column, target_tz, source_tz) -%}
    CAST({{ column }} as {{ dbt.type_timestamp() }}) AT TIME ZONE '{{ source_tz }}' AT TIME ZONE '{{ target_tz }}'
{%- endmacro -%}
