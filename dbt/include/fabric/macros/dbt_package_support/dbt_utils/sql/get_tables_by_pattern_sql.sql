{% macro fabric__get_tables_by_pattern_sql(schema_pattern, table_pattern, exclude='', database=target.database) %}

    select
        s.name as table_schema,
        o.name as table_name,
        case o.type
            when 'U' then 'table'
            when 'V' then 'view'
        end as table_type
    from sys.objects as o
    inner join sys.schemas as s
        on s.schema_id = o.schema_id
    where o.type in ('U', 'V')
      and s.name like '{{ schema_pattern }}'
      and o.name like '{{ table_pattern }}'
      {% if exclude %}
      and o.name not like '{{ exclude }}'
      {% endif %}

{% endmacro %}
