{#
  Shared helpers for Fabric IDENTITY column support.

  A column is declared as IDENTITY via `meta.identity: auto|insert` (see
  `dbt.adapters.fabric.table_refresh.resolve_identity_column` for validation and
  `FabricAdapter.render_raw_columns_constraints` for DDL rendering). Only
  `materialized='table'`, contract-enforced models are supported.

  - 'auto': Fabric assigns the value. The column is excluded from the INSERT
    column/select list so the engine generates it.
  - 'insert': the model's own query supplies explicit values. The column stays in
    the INSERT/select list, wrapped with SET IDENTITY_INSERT ON/OFF, followed by a
    DBCC CHECKIDENT reseed so later 'auto' inserts don't collide with the values
    just written.
#}

{% macro fabric_assert_identity_supported(identity_column) %}
  {%- if identity_column is not none and model.config.materialized != 'table' -%}
    {% do exceptions.raise_compiler_error(
        "Fabric `meta.identity` columns are only supported for materialized='table' "
        "models (got materialized='" ~ model.config.materialized ~ "' for model '"
        ~ model.name ~ "'). Remove `meta.identity` or change the materialization."
    ) %}
  {%- endif -%}
{% endmacro %}

{% macro fabric_identity_insert_columns(column_names, identity_column) %}
  {#- Quoted, comma-separated column list for an INSERT/SELECT, excluding the
      identity column when it is in 'auto' mode. -#}
  {%- set quoted_columns = [] -%}
  {%- for column_name in column_names -%}
    {%- set is_identity_column = (
        identity_column is not none
        and column_name | lower == identity_column.name | lower
    ) -%}
    {%- if not (is_identity_column and identity_column.mode == 'auto') -%}
      {%- do quoted_columns.append(adapter.quote(column_name)) -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return(quoted_columns | join(', ')) }}
{% endmacro %}
