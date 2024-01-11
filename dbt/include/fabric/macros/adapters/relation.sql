{% macro fabric__make_temp_relation(base_relation, suffix) %}
    {%- set temp_identifier = base_relation.identifier ~ suffix -%}
    {%- set temp_relation = base_relation.incorporate(
                                path={"identifier": temp_identifier}) -%}

    {{ return(temp_relation) }}
{% endmacro %}

{% macro fabric__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    {{ fabric__drop_relation_script(relation) }}
  {%- endcall %}
{% endmacro %}

{% macro fabric__drop_relation_script(relation) -%}

    {% if relation.type == 'view' -%}
      {% call statement('find_references', fetch_result=true) %}
      USE [{{ relation.database }}];
      select
          sch.name as schema_name,
          obj.name as view_name
      from sys.sql_expression_dependencies refs
      inner join sys.objects obj
      on refs.referencing_id = obj.object_id
      inner join sys.schemas sch
      on obj.schema_id = sch.schema_id
      where refs.referenced_database_name = '{{ relation.database }}'
      and refs.referenced_schema_name = '{{ relation.schema }}'
      and refs.referenced_entity_name = '{{ relation.identifier }}'
      and refs.referencing_class = 1
      and obj.type = 'V'
      {% endcall %}
      {% set references = load_result('find_references')['data'] %}
      {% for reference in references -%}
      -- dropping referenced view {{ reference[0] }}.{{ reference[1] }}
      {{ fabric__drop_relation_script(relation.incorporate(
          type="view",
          path={"schema": reference[0], "identifier": reference[1]})) }}
      {% endfor %}
    {% elif relation.type == 'table'%}
      {# {% call statement('find_references', fetch_result=true) %}
        USE [{{ relation.database }}];
        SELECT  obj.name AS FK_NAME,
        sch.name AS [schema_name],
        tab1.name AS [table],
        col1.name AS [column],
        tab2.name AS [referenced_table],
        col2.name AS [referenced_column]
        FROM sys.foreign_key_columns fkc
        INNER JOIN sys.objects obj
            ON obj.object_id = fkc.constraint_object_id
        INNER JOIN sys.tables tab1
            ON tab1.object_id = fkc.parent_object_id
        INNER JOIN sys.schemas sch
            ON tab1.schema_id = sch.schema_id
        INNER JOIN sys.columns col1
            ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
        INNER JOIN sys.tables tab2
            ON tab2.object_id = fkc.referenced_object_id
        INNER JOIN sys.columns col2
            ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id
        WHERE sch.name = '{{ relation.schema }}' and tab2.name = '{{ relation.identifier }}'
      {% endcall %}
      {% set references = load_result('find_references')['data'] %}
      {% for reference in references -%}
      -- dropping referenced table {{ reference[0] }}.{{ reference[1] }}
        {{ fabric__drop_relation_script(relation.incorporate(
            type="table",
            path={"schema": reference[1], "identifier": reference[2]})) }}
        {% endfor %} #}
      {% set object_id_type = 'U' %}

    {%- else -%}
        {{ exceptions.raise_not_implemented('Invalid relation being dropped: ' ~ relation) }}
    {% endif %}
    USE [{{ relation.database }}];
    EXEC('DROP {{ relation.type }} IF EXISTS {{ relation.include(database=False) }};');
{% endmacro %}

{% macro fabric__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
     USE [{{ from_relation.database }}];
      EXEC sp_rename '{{ from_relation.schema }}.{{ from_relation.identifier }}', '{{ to_relation.identifier }}'
  {%- endcall %}
{% endmacro %}

-- DROP fabric__truncate_relation when TRUNCATE TABLE is supported
{% macro fabric__truncate_relation(relation) -%}

  {% set tempTableName %}
    {{ relation.include(database=False).identifier.replace("#", "") }}_{{ range(21000, 109000) | random }}
  {% endset %}

  {% call statement('truncate_relation') -%}
    CREATE TABLE {{ tempTableName }} AS SELECT * FROM {{ relation }} WHERE 1=2
    EXEC('DROP TABLE IF EXISTS {{ relation.include(database=False) }};');
    EXEC('CREATE TABLE {{ relation.include(database=False) }} AS SELECT * FROM {{ tempTableName }};');
    EXEC('DROP TABLE IF EXISTS {{ tempTableName }};');
  {%- endcall %}

{% endmacro %}
