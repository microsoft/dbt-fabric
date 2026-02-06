{% macro create_or_update_fabric_warehouse_snapshot(snapshot_name) %}
    {% if execute %}
        {{ return(adapter.create_or_update_warehouse_snapshot(snapshot_name)) }}
    {% endif %}
        {{ return("") }}
{% endmacro %}