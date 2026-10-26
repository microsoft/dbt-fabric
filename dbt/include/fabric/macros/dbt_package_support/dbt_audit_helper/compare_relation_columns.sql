{#- Resolve catalog metadata before rendering the data-plane query. -#}
{% macro fabric__compare_relation_columns(a_relation, b_relation) %}
select
    coalesce(a_cols.column_name, b_cols.column_name) as column_name,
    a_cols.ordinal_position as a_ordinal_position,
    b_cols.ordinal_position as b_ordinal_position,
    a_cols.data_type as a_data_type,
    b_cols.data_type as b_data_type,
    case when a_cols.ordinal_position = b_cols.ordinal_position then 1 else 0 end as has_ordinal_position_match,
    case when a_cols.data_type = b_cols.data_type then 1 else 0 end as has_data_type_match,
    case when a_cols.data_type is not null and b_cols.data_type is null then 1 else 0 end as in_a_only,
    case when b_cols.data_type is not null and a_cols.data_type is null then 1 else 0 end as in_b_only,
    case when a_cols.data_type is not null and b_cols.data_type is not null then 1 else 0 end as in_both
from ({{ fabric__get_columns_in_relation_sql(a_relation) }}) a_cols
full outer join ({{ fabric__get_columns_in_relation_sql(b_relation) }}) b_cols
    on a_cols.column_name = b_cols.column_name
{% endmacro %}


{% macro fabric__get_columns_in_relation_sql(relation) %}
    {% set columns = adapter.get_columns_in_relation(relation) if execute else [] %}
    {% for column in columns %}
        select
            {{ loop.index }} as ordinal_position,
            cast('{{ column.name | replace("'", "''") }}' as varchar(128)) as column_name,
            cast('{{ column.data_type | replace("'", "''") }}' as varchar(128)) as data_type,
            {{ column.char_size if column.char_size is not none else "null" }} as character_maximum_length,
            {{ column.numeric_precision if column.numeric_precision is not none else "null" }} as numeric_precision,
            {{ column.numeric_scale if column.numeric_scale is not none else "null" }} as numeric_scale
        {% if not loop.last %}union all{% endif %}
    {% else %}
        select cast(null as int) as ordinal_position,
            cast(null as varchar(128)) as column_name,
            cast(null as varchar(128)) as data_type,
            cast(null as int) as character_maximum_length,
            cast(null as int) as numeric_precision,
            cast(null as int) as numeric_scale
        where 1 = 0
    {% endfor %}
{% endmacro %}
