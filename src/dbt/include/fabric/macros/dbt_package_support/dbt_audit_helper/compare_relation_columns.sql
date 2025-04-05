{% macro fabric__compare_relation_columns(a_relation, b_relation) %}

with a_cols as (
    {{ fabric__get_columns_in_relation_sql(a_relation) }}
),

b_cols as (
    {{ fabric__get_columns_in_relation_sql(b_relation) }}
)

select
    column_name,
    a_cols.ordinal_position as a_ordinal_position,
    b_cols.ordinal_position as b_ordinal_position,
    a_cols.data_type as a_data_type,
    b_cols.data_type as b_data_type,
    coalesce(a_cols.ordinal_position = b_cols.ordinal_position, false) as has_ordinal_position_match,
    coalesce(a_cols.data_type = b_cols.data_type, false) as has_data_type_match
from a_cols
full outer join b_cols using (column_name)

{% endmacro %}


{% macro fabric__get_columns_in_relation_sql(relation) %}
  SELECT
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM
            (select
                ordinal_position,
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            from INFORMATION_SCHEMA.COLUMNS
            where table_name = '{{ relation.identifier }}'
              and table_schema = '{{ relation.schema }}'
            UNION ALL
            select
                ordinal_position,
                column_name collate database_default,
                data_type collate database_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            from tempdb.INFORMATION_SCHEMA.COLUMNS
            where table_name like '{{ relation.identifier }}%') cols
{% endmacro %}
