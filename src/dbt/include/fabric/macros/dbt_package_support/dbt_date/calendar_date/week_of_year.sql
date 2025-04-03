{%- macro fabric__week_of_year(date) -%}
cast({{ dbt_date.date_part('week', date)}} as {{ dbt.type_int() }})
{%- endmacro %}
