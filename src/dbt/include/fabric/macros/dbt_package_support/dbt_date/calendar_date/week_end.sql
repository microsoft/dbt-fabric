{%- macro fabric__week_end(date) -%}
{%- set dt = dbt_date.week_start(date) -%}
{{ dbt_date.n_days_away(6, dt) }}
{%- endmacro %}
