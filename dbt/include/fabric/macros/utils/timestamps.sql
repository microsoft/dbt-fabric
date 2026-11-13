{% macro fabric__current_timestamp() -%}
  CAST(SYSDATETIME() AS DATETIME2(6))
{%- endmacro %}

{#-
    A snapshot statement references the snapshot time in several CTEs. Fabric Warehouse evaluates
    each SYSDATETIME() reference separately, so the dbt_valid_to of a closed version and the
    dbt_valid_from of its successor can differ. A literal keeps them equal within the statement.
-#}
{% macro fabric__snapshot_get_time() -%}
    {%- if run_started_at is none -%}
        {{ current_timestamp() }}
    {%- else -%}
        {{ snapshot_string_as_time(run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")) }}
    {%- endif -%}
{%- endmacro %}

{% macro fabric__snapshot_string_as_time(timestamp) -%}
    {%- set result = "CONVERT(DATETIME2(6), '" ~ timestamp ~ "')" -%}
    {{ return(result) }}
{%- endmacro %}
