{% macro fabric__generate_series(upper_bound) %}
    {% if upper_bound <= 0 %}
        {{ exceptions.raise_compiler_error("upper bound must be positive") }}
    {% endif %}

    {% set ns = namespace(powers_of_two=none) %}
    {% for _ in range(1, 100) %}
        {% if ns.powers_of_two is none and upper_bound <= 2 ** loop.index %}
            {% set ns.powers_of_two = loop.index %}
        {% endif %}
    {% endfor %}

    with fabric_powers_of_two as (
        select 0 as generated_number
        union all
        select 1
    ),
    fabric_generated_series as (
        select
            {% for i in range(ns.powers_of_two) %}
            p{{ i }}.generated_number * power(2, {{ i }})
            {% if not loop.last %} + {% endif %}
            {% endfor %}
            + 1 as generated_number
        from
            {% for i in range(ns.powers_of_two) %}
            fabric_powers_of_two as p{{ i }}
            {% if not loop.last %} cross join {% endif %}
            {% endfor %}
    )

    select generated_number
    from fabric_generated_series
    where generated_number <= {{ upper_bound }}
{% endmacro %}
