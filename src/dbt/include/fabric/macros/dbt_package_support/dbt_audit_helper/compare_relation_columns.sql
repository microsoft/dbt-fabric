{% macro fabric__compare_relation_columns(
        a_relation,
        b_relation
    ) %}
    WITH a_cols AS (
        {{ fabric__get_columns_in_relation_sql(a_relation) }}
    ),
    b_cols AS (
        {{ fabric__get_columns_in_relation_sql(b_relation) }}
    )
SELECT
    column_name,
    a_cols.ordinal_position AS a_ordinal_position,
    b_cols.ordinal_position AS b_ordinal_position,
    a_cols.data_type AS a_data_type,
    b_cols.data_type AS b_data_type,
    COALESCE(
        a_cols.ordinal_position = b_cols.ordinal_position,
        FALSE
    ) AS has_ordinal_position_match,
    COALESCE(
        a_cols.data_type = b_cols.data_type,
        FALSE
    ) AS has_data_type_match
FROM
    a_cols full
    OUTER JOIN b_cols USING (column_name)
{% endmacro %}

{% macro fabric__get_columns_in_relation_sql(relation) %}
SELECT
    column_name,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale
FROM
    (
        SELECT
            ordinal_position,
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM
            information_schema.columns
        WHERE
            table_name = '{{ relation.identifier }}'
            AND table_schema = '{{ relation.schema }}'
        UNION ALL
        SELECT
            ordinal_position,
            column_name COLLATE database_default,
            data_type COLLATE database_default,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM
            tempdb.information_schema.columns
        WHERE
            table_name LIKE '{{ relation.identifier }}%'
    ) cols
{% endmacro %}
