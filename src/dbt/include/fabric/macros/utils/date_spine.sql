{% macro fabric__date_spine(datepart, start_date, end_date) %}
    
    select *
    from (
        select (
            {{
                dbt.dateadd(
                    datepart,
                    "row_number() over (order by generated_number) - 1",
                    start_date
                )
            }}
        ) as date_{{datepart}}
        from ({{  
                dbt.generate_series(
                    dbt.get_intervals_between(start_date, end_date, datepart)
                )
            }}) raw_data
    ) all_periods
    where date_{{datepart}} <= {{ end_date }}

{% endmacro %}