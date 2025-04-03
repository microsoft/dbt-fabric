{%- macro fabric__iso_week_of_year(date) -%}
cast({{ dbt_date.date_part('iso_week', date) }} as {{ dbt.type_int() }}) 
{%- endmacro %}
