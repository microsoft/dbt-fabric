{% macro fabric_constraint_name(relation, columns, suffix, name=none) %}
    {% set name = dbt_constraints.sanitize_constraint_name(
        name or relation.schema ~ "_" ~ relation.identifier ~ "_" ~ (columns | join("_")) ~ "_" ~ suffix
    ) %}
    {% if name | length > 128 %}
        {% set name = name[:95] ~ "_" ~ local_md5(name) %}
    {% endif %}
    {{ return(adapter.quote(name)) }}
{% endmacro %}

{% macro fabric_constraint_columns(columns) %}
    {% set quoted = [] %}
    {% for column in columns %}{% do quoted.append(adapter.quote(column)) %}{% endfor %}
    {{ return(quoted | join(", ")) }}
{% endmacro %}

{% macro fabric__unique_constraint_exists(table_relation, column_names, lookup_cache) %}
    {% set sql %}
        select kc.name as constraint_name, c.name as column_name
        from {{ adapter.quote(table_relation.database) }}.sys.key_constraints kc
        join {{ adapter.quote(table_relation.database) }}.sys.index_columns ic
            on kc.parent_object_id = ic.object_id and kc.unique_index_id = ic.index_id
        join {{ adapter.quote(table_relation.database) }}.sys.columns c
            on ic.object_id = c.object_id and ic.column_id = c.column_id
        where kc.parent_object_id = object_id('{{ table_relation | replace("'", "''") }}')
            and ic.key_ordinal > 0
        order by kc.name, ic.key_ordinal
    {% endset %}
    {% set keys = dbt_constraints.get_results_group_by(run_query(sql), "constraint_name") %}
    {% for rows in keys.values() %}
        {% if dbt_constraints.column_list_matches(rows | map(attribute="column_name") | list, column_names) %}
            {{ return(true) }}
        {% endif %}
    {% endfor %}
    {{ return(false) }}
{% endmacro %}

{% macro fabric_constraint_foreign_key_exists(table_relation, column_names, pk_relation=none, pk_columns=none) %}
    {% set sql %}
        select fk.name as constraint_name, c.name as column_name, pc.name as referenced_column
        from {{ adapter.quote(table_relation.database) }}.sys.foreign_keys fk
        join {{ adapter.quote(table_relation.database) }}.sys.foreign_key_columns fc
            on fk.object_id = fc.constraint_object_id
        join {{ adapter.quote(table_relation.database) }}.sys.columns c
            on fc.parent_object_id = c.object_id and fc.parent_column_id = c.column_id
        join {{ adapter.quote(table_relation.database) }}.sys.columns pc
            on fc.referenced_object_id = pc.object_id and fc.referenced_column_id = pc.column_id
        where fk.parent_object_id = object_id('{{ table_relation | replace("'", "''") }}')
        {% if pk_relation %}
            and fk.referenced_object_id = object_id('{{ pk_relation | replace("'", "''") }}')
        {% endif %}
        order by fk.name, fc.constraint_column_id
    {% endset %}
    {% set keys = dbt_constraints.get_results_group_by(run_query(sql), "constraint_name") %}
    {% for rows in keys.values() %}
        {% if dbt_constraints.column_list_matches(rows | map(attribute="column_name") | list, column_names) %}
            {% if not pk_relation %}{{ return(true) }}{% endif %}
            {% set expected = {} %}
            {% set actual = {} %}
            {% for column in column_names %}
                {% do expected.update({column | upper: pk_columns[loop.index0] | upper}) %}
            {% endfor %}
            {% for row in rows %}
                {% do actual.update({row.column_name | upper: row.referenced_column | upper}) %}
            {% endfor %}
            {% if actual == expected %}{{ return(true) }}{% endif %}
        {% endif %}
    {% endfor %}
    {{ return(false) }}
{% endmacro %}

{% macro fabric__foreign_key_exists(table_relation, column_names, lookup_cache) %}
    {{ return(fabric_constraint_foreign_key_exists(table_relation, column_names)) }}
{% endmacro %}

{% macro fabric_constraint_has_permission(table_relation, permission, verify_permissions) %}
    {% if not verify_permissions %}{{ return(true) }}{% endif %}
    {% set sql %}
        {{ get_use_database_sql(table_relation.database) }}
        select has_perms_by_name(
            '{{ table_relation.include(database=false) | replace("'", "''") }}',
            'OBJECT', '{{ permission }}'
        ) as allowed
    {% endset %}
    {% set result = run_query(sql) %}
    {{ return(result.rows[0][0] == 1) }}
{% endmacro %}

{% macro fabric__have_ownership_priv(table_relation, verify_permissions, lookup_cache) %}
    {{ return(fabric_constraint_has_permission(table_relation, "ALTER", verify_permissions)) }}
{% endmacro %}

{% macro fabric__have_references_priv(table_relation, verify_permissions, lookup_cache) %}
    {{ return(fabric_constraint_has_permission(table_relation, "REFERENCES", verify_permissions)) }}
{% endmacro %}

{% macro fabric_constraint_create_key(table_relation, column_names, verify_permissions, constraint_name, lookup_cache, kind) %}
    {% if dbt_constraints.unique_constraint_exists(table_relation, column_names, lookup_cache) %}
        {% do return(none) %}
    {% endif %}
    {% if not dbt_constraints.have_ownership_priv(table_relation, verify_permissions, lookup_cache) %}
        {{ exceptions.warn("Skipping " ~ kind ~ " on " ~ table_relation ~ ": ALTER permission is required.") }}
        {% do return(none) %}
    {% endif %}
    {% set sql %}
        alter table {{ table_relation }}
        add constraint {{ fabric_constraint_name(table_relation, column_names, "PK" if kind == "PRIMARY KEY" else "UK", constraint_name) }}
        {{ kind }} nonclustered ({{ fabric_constraint_columns(column_names) }}) not enforced
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}

{% macro fabric__create_primary_key(table_relation, column_names, verify_permissions, quote_columns, constraint_name, lookup_cache, rely_clause) %}
    {% do fabric_constraint_create_key(table_relation, column_names, verify_permissions, constraint_name, lookup_cache, "PRIMARY KEY") %}
{% endmacro %}

{% macro fabric__create_unique_key(table_relation, column_names, verify_permissions, quote_columns, constraint_name, lookup_cache, rely_clause) %}
    {% do fabric_constraint_create_key(table_relation, column_names, verify_permissions, constraint_name, lookup_cache, "UNIQUE") %}
{% endmacro %}

{% macro fabric__create_foreign_key(pk_table_relation, pk_column_names, fk_table_relation, fk_column_names, verify_permissions, quote_columns, constraint_name, lookup_cache, rely_clause) %}
    {% if pk_table_relation.database != fk_table_relation.database %}
        {{ exceptions.raise_compiler_error("Fabric foreign keys must reference a table in the same database.") }}
    {% endif %}
    {% if not dbt_constraints.unique_constraint_exists(pk_table_relation, pk_column_names, lookup_cache) %}
        {{ exceptions.warn("Skipping foreign key on " ~ fk_table_relation ~ ": the referenced columns need a primary or unique key.") }}
        {% do return(none) %}
    {% endif %}
    {% if fabric_constraint_foreign_key_exists(fk_table_relation, fk_column_names, pk_table_relation, pk_column_names) %}
        {% do return(none) %}
    {% endif %}
    {% if not dbt_constraints.have_ownership_priv(fk_table_relation, verify_permissions, lookup_cache)
        or not dbt_constraints.have_references_priv(pk_table_relation, verify_permissions, lookup_cache) %}
        {{ exceptions.warn("Skipping foreign key on " ~ fk_table_relation ~ ": ALTER and REFERENCES permissions are required.") }}
        {% do return(none) %}
    {% endif %}
    {% set sql %}
        alter table {{ fk_table_relation }}
        add constraint {{ fabric_constraint_name(
            fk_table_relation, fk_column_names + [pk_table_relation.schema, pk_table_relation.identifier] + pk_column_names,
            "FK", constraint_name
        ) }}
        foreign key ({{ fabric_constraint_columns(fk_column_names) }})
        references {{ pk_table_relation.include(database=false) }} ({{ fabric_constraint_columns(pk_column_names) }})
        not enforced
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}

{% macro fabric__create_not_null(table_relation, column_names, verify_permissions, quote_columns, lookup_cache, rely_clause) %}
    {% set columns = adapter.get_columns_in_relation(table_relation) %}
    {% for column in columns if column.name | upper in (column_names | map("upper") | list) %}
        {% if column.is_nullable %}
            {{ exceptions.warn("Fabric cannot add a NOT NULL constraint to existing column "
                ~ table_relation ~ "." ~ adapter.quote(column.name)
                ~ ". Declare it in an enforced model contract and rebuild with --full-refresh.") }}
        {% endif %}
    {% endfor %}
{% endmacro %}

{% macro fabric__release_constraints_for_rebuild(rebuild_relations) %}
    {% set drop_plan = {} %}
    {% set selected_tables = [] %}
    {% for relation in rebuild_relations %}
        {% do selected_tables.append([relation.database, relation.schema, relation.identifier]) %}
    {% endfor %}
    {% for relation in rebuild_relations %}
        {% set sql %}
            select fk.name as constraint_name, s.name as schema_name, t.name as table_name
            from {{ adapter.quote(relation.database) }}.sys.foreign_keys fk
            join {{ adapter.quote(relation.database) }}.sys.tables t on fk.parent_object_id = t.object_id
            join {{ adapter.quote(relation.database) }}.sys.schemas s on t.schema_id = s.schema_id
            where fk.parent_object_id = object_id('{{ relation | replace("'", "''") }}')
                or fk.referenced_object_id = object_id('{{ relation | replace("'", "''") }}')
        {% endset %}
        {% for row in run_query(sql).rows %}
            {% if [relation.database, row.schema_name, row.table_name] not in selected_tables %}
                {{ exceptions.raise_compiler_error(
                    "Cannot rebuild " ~ relation ~ ": foreign key " ~ row.constraint_name
                    ~ " belongs to unselected table " ~ row.schema_name ~ "." ~ row.table_name
                    ~ ". Include that table in the build so its foreign key can be safely recreated."
                ) }}
            {% endif %}
            {% set child = api.Relation.create(
                database=relation.database, schema=row.schema_name, identifier=row.table_name
            ) %}
            {% set ddl %}alter table {{ child }} drop constraint {{ adapter.quote(row.constraint_name) }}{% endset %}
            {% do drop_plan.update({ddl: ddl}) %}
        {% endfor %}
    {% endfor %}
    {% for ddl in drop_plan.values() %}{% do run_query(ddl) %}{% endfor %}
{% endmacro %}
