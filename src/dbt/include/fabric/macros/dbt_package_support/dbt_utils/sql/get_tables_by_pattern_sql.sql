{% macro fabric__get_tables_by_pattern_sql(schema_pattern, table_pattern, exclude='', database=target.database) %}

        SELECT DISTINCT
            table_schema AS "table_schema",
            table_name AS "table_name",
            {{ dbt_utils.get_table_types_sql() }}
        FROM [{{database}}].information_schema.tables -- Escape DB name
        WHERE table_schema LIKE '{{ schema_pattern }}'
        AND table_name LIKE '{{ table_pattern }}'
        AND table_name NOT LIKE '{{ exclude }}'

{% endmacro %}
