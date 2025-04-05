{% macro fabric__date_spine_sql(
        datepart,
        start_date,
        end_date
    ) %}
    WITH l0 AS (
        SELECT
            C
        FROM
            (
                SELECT
                    1
                UNION ALL
                SELECT
                    1
            ) AS d(C)
    ),
    l1 AS (
        SELECT
            1 AS C
        FROM
            l0 AS A
            CROSS JOIN l0 AS b
    ),
    l2 AS (
        SELECT
            1 AS C
        FROM
            l1 AS A
            CROSS JOIN l1 AS b
    ),
    l3 AS (
        SELECT
            1 AS C
        FROM
            l2 AS A
            CROSS JOIN l2 AS b
    ),
    l4 AS (
        SELECT
            1 AS C
        FROM
            l3 AS A
            CROSS JOIN l3 AS b
    ),
    l5 AS (
        SELECT
            1 AS C
        FROM
            l4 AS A
            CROSS JOIN l4 AS b
    ),
    nums AS (
        SELECT
            ROW_NUMBER() over (
                ORDER BY
                    (
                        SELECT
                            NULL
                    )
            ) AS ROWNUM
        FROM
            l5
    ),
    rawdata AS (
        SELECT
            top ({{ dbt.datediff(start_date, end_date, datepart) }}) ROWNUM -1 AS n
        FROM
            nums
        ORDER BY
            ROWNUM
    ),
    all_periods AS (
        SELECT
            (
                {{ dbt.dateadd(
                    datepart,
                    'n',
                    start_date
                ) }}
            ) AS date_ {{ datepart }}
        FROM
            rawdata
    ),
    filtered AS (
        SELECT
            *
        FROM
            all_periods
        WHERE
            date_ {{ datepart }} <= {{ end_date }}
    )
SELECT
    *
FROM
    filtered
{% endmacro %}

{% macro fabric__date_spine(
        datepart,
        start_date,
        end_date
    ) -%}
    {% set date_spine_query %}
    {{ fabric__date_spine_sql(
        datepart,
        start_date,
        end_date
    ) }}
ORDER BY
    1 {% endset %}
    {% set results = run_query(date_spine_query) %}
    {% if execute %}
        {% set results_list = results.columns [0].values() %}
    {% else %}
        {% set results_list = [] %}
    {% endif %}

    {%- for date_field in results_list %}
    SELECT
        '{{ date_field }}' AS date_ {{ datepart }}
        {{ 'union all ' if not loop.last
        ELSE '' }}
    {% endfor -%}
{% endmacro %}
