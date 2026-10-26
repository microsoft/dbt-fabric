{#- Split epoch units into int-sized DATEADD increments; Warehouse timestamps have microsecond precision. -#}
{%- macro fabric__from_unixtimestamp(epochs, format="seconds") -%}

    {%- if format == "seconds" -%}
    {%- set units_per_second = 1 -%}
    {%- elif format == "milliseconds" -%}
    {%- set units_per_second = 1000 -%}
    {%- elif format == "microseconds" -%}
    {%- set units_per_second = 1000000 -%}
    {%- elif format == "nanoseconds" -%}
    {%- set units_per_second = 1000000000 -%}
    {%- else -%}
    {{ exceptions.raise_compiler_error(
        "value " ~ format ~ " for `format` for from_unixtimestamp is not supported."
        )
    }}
    {% endif -%}

    dateadd(microsecond,
        cast((cast({{ epochs }} as bigint) % {{ units_per_second }})
            {% if format == "nanoseconds" %}/ 1000{% else %}* {{ 1000000 // units_per_second }}{% endif %}
            as int),
        dateadd(second,
            cast((cast({{ epochs }} as bigint) / {{ units_per_second }}) % 86400 as int),
            dateadd(day,
                cast((cast({{ epochs }} as bigint) / {{ units_per_second }}) / 86400 as int),
                cast('1970-01-01' as datetime2(6))
            )
        )
    )

{%- endmacro %}
