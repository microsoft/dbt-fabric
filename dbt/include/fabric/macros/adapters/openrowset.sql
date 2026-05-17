{#
    OPENROWSET support for Fabric Data Warehouse.
    Enables file-based sources (Parquet, CSV, JSONL) from OneLake, ADLS, or Azure Blob Storage.
    See: https://learn.microsoft.com/en-us/sql/t-sql/functions/openrowset-bulk-transact-sql?view=fabric

    Usage in models:
      SELECT * FROM {{ openrowset_source('my_source', 'my_table') }}

    Source definition in sources.yml:
      sources:
        - name: my_source
          meta:
            openrowset:
              path: "https://onelake.dfs.fabric.microsoft.com/<workspaceId>/<lakehouseId>/Files"
          tables:
            - name: my_parquet_table
              meta:
                openrowset:
                  file: "data/sales.parquet"
              columns:
                - name: id
                  data_type: int
                - name: amount
                  data_type: "decimal(10,2)"

    Supported formats (Fabric Warehouse): PARQUET, CSV, JSONL
    Note: DELTA format is NOT supported in Fabric Warehouse OPENROWSET.
    Format is auto-detected from file extension, or can be set explicitly via meta.openrowset.format.

    Format-specific defaults (user can override via meta.openrowset.options):
      CSV:     HEADER_ROW = TRUE
      PARQUET: no extra options needed
      JSONL:   no extra options needed

    Supported options per format:
      CSV:     HEADER_ROW, FIELDTERMINATOR, ROWTERMINATOR, FIELDQUOTE, ESCAPECHAR,
               PARSER_VERSION, FIRSTROW, LASTROW, CODEPAGE, DATAFILETYPE
      PARQUET: ROWS_PER_BATCH, MAXERRORS, FIRSTROW, CODEPAGE, DATAFILETYPE
      JSONL:   FIRSTROW, CODEPAGE, DATAFILETYPE, ROWS_PER_BATCH, MAXERRORS

    Universal options (all formats): ROWS_PER_BATCH, MAXERRORS

    WITH clause column mapping (column-level meta.openrowset):
      path:    JSON path for JSONL/Parquet nested fields, e.g. "$.updated"
      ordinal: Column position number for positional mapping
#}

{% macro openrowset_source(source_name, table_name) %}
    {#-- Register the source dependency during parsing (graph not available yet) --#}
    {%- set _src_relation = source(source_name, table_name) -%}
    {%- if execute -%}
        {{- adapter.dispatch('openrowset_source', 'dbt')(source_name, table_name) -}}
    {%- endif -%}
{% endmacro %}

{% macro fabric__openrowset_source(source_name, table_name) %}
    {#-- Resolve source node from the graph --#}
    {%- set source_node = graph.sources.values()
        | selectattr('source_name', 'equalto', source_name)
        | selectattr('name', 'equalto', table_name)
        | list -%}

    {%- if source_node | length == 0 -%}
        {%- do exceptions.raise_compiler_error(
            "Source '" ~ source_name ~ "." ~ table_name ~ "' not found. "
            ~ "Make sure it is defined in your sources.yml."
        ) -%}
    {%- endif -%}
    {%- set source_node = source_node[0] -%}

    {#-- Get openrowset config from table meta, then source meta --#}
    {%- set table_meta = source_node.meta or {} -%}
    {%- set table_openrowset = table_meta.get('openrowset', {}) -%}

    {#-- Get parent source meta for shared config (e.g. base path) --#}
    {%- set source_meta = source_node.source_meta or {} -%}
    {%- set source_openrowset = source_meta.get('openrowset', {}) -%}

    {#-- Build the full file path --#}
    {%- set base_path = source_openrowset.get('path', '') -%}
    {%- set file_path = table_openrowset.get('file', '') -%}
    {%- set folder = table_openrowset.get('folder', '') -%}
    {%- set full_path = table_openrowset.get('path', '') -%}

    {%- if full_path -%}
        {#-- Table-level path overrides everything --#}
        {%- set bulk_path = full_path -%}
    {%- elif base_path and (file_path or folder) -%}
        {#-- Combine source base path + folder + file --#}
        {%- set path_parts = [base_path.rstrip('/')] -%}
        {%- if folder -%}
            {%- do path_parts.append(folder.strip('/')) -%}
        {%- endif -%}
        {%- if file_path -%}
            {%- do path_parts.append(file_path.strip('/')) -%}
        {%- endif -%}
        {%- set bulk_path = path_parts | join('/') -%}
    {%- else -%}
        {%- do exceptions.raise_compiler_error(
            "OPENROWSET source '" ~ source_name ~ "." ~ table_name ~ "' requires a file path. "
            ~ "Set 'path' in source-level meta.openrowset or 'file'/'path' in table-level meta.openrowset."
        ) -%}
    {%- endif -%}

    {#-- Detect format from explicit config or file extension --#}
    {%- set explicit_format = table_openrowset.get('format', source_openrowset.get('format', '')) -%}
    {%- if explicit_format -%}
        {%- set file_format = explicit_format | upper -%}
    {%- else -%}
        {%- set file_format = fabric__detect_format(bulk_path) -%}
    {%- endif -%}

    {#-- Get format-specific defaults --#}
    {%- set format_defaults = fabric__openrowset_format_defaults(file_format) -%}

    {#-- Merge: format defaults < source-level overrides < table-level overrides --#}
    {%- set merged_options = {} -%}
    {%- do merged_options.update(format_defaults) -%}
    {%- set source_options = source_openrowset.get('options', {}) -%}
    {%- set table_options = table_openrowset.get('options', {}) -%}
    {%- do merged_options.update(source_options) -%}
    {%- do merged_options.update(table_options) -%}

    {#-- Build the OPENROWSET SQL --#}
    {{ fabric__build_openrowset_sql(bulk_path, file_format, merged_options, source_node.columns, table_name) }}
{%- endmacro %}


{% macro fabric__detect_format(path) %}
    {%- set path_lower = path | lower -%}
    {%- if path_lower.endswith('.parquet') or path_lower.endswith('.parq') -%}
        {{ return('PARQUET') }}
    {%- elif path_lower.endswith('.csv') or path_lower.endswith('.tsv') -%}
        {{ return('CSV') }}
    {%- elif path_lower.endswith('.jsonl') or path_lower.endswith('.ldjson') or path_lower.endswith('.ndjson') -%}
        {{ return('JSONL') }}
    {%- else -%}
        {%- do exceptions.raise_compiler_error(
            "Cannot detect file format from path: '" ~ path ~ "'. "
            ~ "Specify 'format' in meta.openrowset (one of: PARQUET, CSV, JSONL)."
        ) -%}
    {%- endif -%}
{% endmacro %}


{% macro fabric__openrowset_format_defaults(file_format) %}
    {%- if file_format == 'CSV' -%}
        {{ return({'HEADER_ROW': true}) }}
    {%- elif file_format == 'PARQUET' -%}
        {{ return({}) }}
    {%- elif file_format == 'JSONL' -%}
        {{ return({}) }}
    {%- else -%}
        {{ return({}) }}
    {%- endif -%}
{% endmacro %}


{% macro fabric__build_openrowset_sql(bulk_path, file_format, options, columns, alias) %}
    OPENROWSET(
        BULK '{{ bulk_path }}',
        FORMAT = '{{ file_format }}'
        {#-- Render additional options --#}
        {%- for key, value in options.items() %}
            {%- if value is sameas true or value == 'True' or value == 'TRUE' %},
        {{ key }} = TRUE
            {%- elif value is sameas false or value == 'False' or value == 'FALSE' %},
        {{ key }} = FALSE
            {%- elif value is number %},
        {{ key }} = {{ value }}
            {%- else %},
        {{ key }} = '{{ value }}'
            {%- endif -%}
        {%- endfor -%}
    )
    {#-- WITH clause for explicit column schema --#}
    {%- if columns | length > 0 %}
    WITH (
        {%- for col_name, col_config in columns.items() %}
        {%- set col_openrowset = (col_config.get('meta', {}) or {}).get('openrowset', {}) or {} %}
        {{ col_name }} {{ col_config.data_type }}
        {%- if col_openrowset.get('path') %} '{{ col_openrowset.path }}'{%- endif %}
        {%- if col_openrowset.get('ordinal') %} {{ col_openrowset.ordinal }}{%- endif %}
        {{- ', ' if not loop.last }}
        {%- endfor %}
    )
    {%- endif %}
    AS [{{ alias }}]
{%- endmacro %}
