{% macro fabric__drop_schema_by_name(schema_name) %}
    {% set relation = api.Relation.create(database=target.database, schema=schema_name) %}
    {% do drop_schema(relation) %}
{% endmacro %}