{% macro fabric__get_empty_subquery_sql(select_sql, select_sql_header=none) %}
    with __dbt_sbq_tmp as (
        {{ select_sql }}
    )
    select * from __dbt_sbq_tmp
    where 0 = 1
{% endmacro %}

{% macro fabric__get_columns_in_relation(relation) -%}
    {% call statement(
        'get_columns_in_relation',
        fetch_result=True,
        auto_begin=False
    ) %}
        {{ get_use_database_sql(relation.database) }}
        with mapping as (
            select
                row_number() over (partition by object_name(c.object_id) order by c.column_id) as ordinal_position,
                c.name collate database_default as column_name,
                t.name as data_type,
                case
					when (t.name in ('nchar', 'nvarchar', 'sysname') and c.max_length <> -1) then c.max_length / 2
					else c.max_length
				end as character_maximum_length,
                c.precision as numeric_precision,
				c.scale as numeric_scale,
				c.is_nullable,
				c.collation_name,
				c.is_identity
            from sys.columns c {{ information_schema_hints() }}
            inner join sys.types t {{ information_schema_hints() }}
            on c.user_type_id = t.user_type_id
            where c.object_id = object_id('{{ 'tempdb..' ~ relation.include(database=false, schema=false) if '#' in relation.identifier else relation }}')
        )

        select
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            collation_name,
            is_identity
        from mapping
        order by ordinal_position

    {% endcall %}
    {% set table = load_result('get_columns_in_relation').table %}
    {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro fabric__get_columns_in_query(select_sql) %}
    {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
        with __dbt_sbq as
        (
            {{ select_sql }}
        )
        select top 0 *
        from __dbt_sbq
        where 0 = 1

    {% endcall %}

    {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}

{% macro fabric__alter_column_type(relation, column_name, new_column_type) %}
    {#-
        Fabric Warehouse applies the column changes it supports -- widening a type, such as
        varchar(8000) to varchar(max) -- as a metadata-only ALTER COLUMN. Rebuilding the
        relation instead would copy every row of the target for every changed column.

        Whatever the warehouse cannot apply that way -- a narrowing, an incompatible type, a
        clustering column, or a column carrying manually created statistics -- it rejects, and
        dbt does not rebuild the model behind the user's back. Rebuild it with --full-refresh
        to apply such a change.

        https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-table-transact-sql?view=fabric#alter-column
    -#}
    {%- set existing_column = adapter.get_columns_in_relation(relation)
            | selectattr('name', 'equalto', column_name) | list | first -%}

    {%- if not existing_column -%}
        {% do exceptions.raise_compiler_error(
            "Cannot alter column " ~ adapter.quote(column_name) ~ ": it does not exist in " ~ relation
        ) %}
    {%- endif -%}

    {%- if existing_column.is_identity -%}
        {% do exceptions.raise_compiler_error(
            "Cannot alter identity column " ~ adapter.quote(column_name) ~ " in " ~ relation
            ~ ": Fabric Warehouse does not support altering identity columns."
            ~ " Rebuild the model with --full-refresh to change its type."
        ) %}
    {%- endif -%}

    {#- ALTER COLUMN drops the nullability and collation that are not restated, so both have to
        be known before the column can be altered without losing them. -#}
    {%- if existing_column.is_nullable is none -%}
        {% do exceptions.raise_compiler_error(
            "Cannot alter column " ~ adapter.quote(column_name) ~ " in " ~ relation
            ~ ": its nullability is unknown, and ALTER COLUMN would drop a NOT NULL constraint."
            ~ " Rebuild the model with --full-refresh to change its type."
        ) %}
    {%- endif -%}

    {%- set nullability = 'NULL' if existing_column.is_nullable else 'NOT NULL' -%}
    {%- set collation = ' COLLATE ' ~ existing_column.collation_name
            if existing_column.collation_name and 'char' in new_column_type | lower else '' -%}

    {% do log("Altering " ~ relation ~ " column " ~ adapter.quote(column_name) ~ " to " ~ new_column_type
        ~ "; rebuild the model with --full-refresh if Fabric Warehouse rejects the change.") %}

    {% call statement('alter_column_type') %}
        ALTER TABLE {{ relation }}
        ALTER COLUMN {{ adapter.quote(column_name) }} {{ new_column_type }}{{ collation }} {{ nullability }}
    {% endcall %}
{% endmacro %}

{% macro fabric__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
  {% call statement('add_drop_columns') -%}
    {% if add_columns %}
        alter {{ relation.type }} {{ relation }}
        add {% for column in add_columns %}[{{ column.name | replace(']', ']]') }}] {{ column.data_type }}{{ ', ' if not loop.last }}{% endfor %};
    {% endif %}

    {% if remove_columns %}
        alter {{ relation.type }} {{ relation }}
        drop column {% for column in remove_columns %}[{{ column.name | replace(']', ']]') }}]{{ ',' if not loop.last }}{% endfor %};
    {% endif %}
  {%- endcall -%}
{% endmacro %}
