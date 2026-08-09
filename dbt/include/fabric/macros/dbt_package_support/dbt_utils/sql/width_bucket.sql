{% macro fabric__width_bucket(expr, min_value, max_value, num_buckets) -%}

    case
        when {{ expr }} < {{ min_value }} then 0
        when {{ expr }} >= {{ max_value }} then {{ num_buckets }} + 1
        else floor(
            (
                cast({{ expr }} as decimal(38, 10))
                - cast({{ min_value }} as decimal(38, 10))
            )
            * {{ num_buckets }}
            / nullif(
                cast({{ max_value }} as decimal(38, 10))
                - cast({{ min_value }} as decimal(38, 10)),
                0
            )
        ) + 1
    end

{%- endmacro %}
