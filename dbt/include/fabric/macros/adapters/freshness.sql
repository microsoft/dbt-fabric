{% macro fabric__collect_freshness(source, loaded_at_field, filter) %}
  {#
    Override of dbt's default collect_freshness for Fabric Lakehouse.

    Fabric Lakehouse stores some datetime columns as VARCHAR (e.g. CDC timestamps
    from Debezium). TRY_CAST to datetime2(3) handles both cases safely:
    - datetime2 columns → no-op cast, passes through
    - ISO-8601 / datetime str → cast succeeds
    - NULL or unparseable values→ TRY_CAST returns NULL → MAX returns NULL
    → dbt treats source as "infinitely stale"
  #}
  {% call statement('collect_freshness', fetch_result=True, auto_begin=False) -%}
    select
      max(TRY_CAST({{ loaded_at_field }} AS datetime2(3))) as max_loaded_at,
      {{ dbt.current_timestamp() }} as snapshotted_at
    from {{ source }}
    {% if filter %}
      where {{ filter }}
    {% endif %}
  {%- endcall %}
  {{ return(load_result('collect_freshness')) }}
{% endmacro %}
