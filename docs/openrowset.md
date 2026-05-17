# OPENROWSET Source — File-Based Ingestion for Fabric Data Warehouse

The `dbt-fabric` adapter provides native [`OPENROWSET(BULK ...)`](https://learn.microsoft.com/en-us/sql/t-sql/functions/openrowset-bulk-transact-sql?view=fabric) support, enabling you to query files directly from OneLake, Azure Data Lake Storage (ADLS Gen2), or Azure Blob Storage without loading them into a table first.

This is useful for:

- **ELT ingestion** — read raw files from a Lakehouse into staging models
- **Data exploration** — query file contents ad-hoc without creating external tables
- **Cross-workspace loads** — read files from other Fabric workspaces via OneLake

## Table of Contents

- [Quick Start](#quick-start)
- [Supported Formats](#supported-formats)
- [Path Configuration](#path-configuration)
  - [Source-Level Base Path](#source-level-base-path)
  - [Table-Level File Reference](#table-level-file-reference)
  - [Path Resolution Rules](#path-resolution-rules)
  - [Supported Storage Locations](#supported-storage-locations)
- [Wildcards and Folder Reads](#wildcards-and-folder-reads)
- [Format Detection](#format-detection)
- [Format Options](#format-options)
  - [CSV Options](#csv-options)
  - [Parquet Options](#parquet-options)
  - [JSONL Options](#jsonl-options)
  - [Option Override Hierarchy](#option-override-hierarchy)
- [Column Schema (WITH Clause)](#column-schema-with-clause)
  - [Basic Column Definitions](#basic-column-definitions)
  - [Column Path Mapping (JSON / Nested Fields)](#column-path-mapping-json--nested-fields)
  - [Column Ordinal Mapping](#column-ordinal-mapping)
  - [Schema Inference (No WITH Clause)](#schema-inference-no-with-clause)
- [Complete Examples](#complete-examples)
  - [Parquet from OneLake](#parquet-from-onelake)
  - [CSV with Default Options](#csv-with-default-options)
  - [CSV with Custom Delimiter](#csv-with-custom-delimiter)
  - [TSV (Tab-Separated)](#tsv-tab-separated)
  - [JSONL with Nested Fields](#jsonl-with-nested-fields)
  - [Parquet without Schema (Auto-Detect)](#parquet-without-schema-auto-detect)
  - [External Storage (Azure Blob)](#external-storage-azure-blob)
  - [Multiple Tables from One Source](#multiple-tables-from-one-source)
- [Permissions](#permissions)
- [Limitations](#limitations)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

### 1. Define a source in your `sources.yml`

```yaml
sources:
  - name: my_lakehouse
    meta:
      openrowset:
        path: "https://onelake.dfs.fabric.microsoft.com/<workspaceId>/<lakehouseId>/Files"
    tables:
      - name: sales
        meta:
          openrowset:
            file: "data/sales.parquet"
        columns:
          - name: id
            data_type: int
          - name: amount
            data_type: "decimal(10,2)"
          - name: sale_date
            data_type: date
```

### 2. Use it in a model

```sql
-- models/stg_sales.sql
SELECT *
FROM {{ openrowset_source('my_lakehouse', 'sales') }}
```

### 3. Compiled SQL

dbt compiles a model to the following SQL:

```sql
SELECT *
FROM OPENROWSET(
    BULK 'https://onelake.dfs.fabric.microsoft.com/<workspaceId>/<lakehouseId>/Files/data/sales.parquet',
    FORMAT = 'PARQUET'
)
WITH (
    id int,
    amount decimal(10,2),
    sale_date date
)
AS [sales]
```

---

## Supported Formats

| Format    | Auto-Detected Extensions                 | Notes                                          |
|-----------|------------------------------------------|-------------------------------------------------|
| `PARQUET` | `.parquet`, `.parq`                      | Schema inference available; WITH clause optional |
| `CSV`     | `.csv`, `.tsv`                           | Default: `HEADER_ROW = TRUE`                    |
| `JSONL`   | `.jsonl`, `.ldjson`, `.ndjson`           | Newline-delimited JSON only                     |

> **Important:** DELTA format is **not** supported in Fabric Warehouse `OPENROWSET`. Use Lakehouse tables or shortcuts instead.

---

## Path Configuration

File paths are configured via `meta.openrowset` at both the **source** level and **table** level in your `sources.yml`.

### Source-Level Base Path

Set a shared base path for all tables in the source. This is typically the root of your Lakehouse Files folder or a storage container.

```yaml
sources:
  - name: my_lakehouse
    meta:
      openrowset:
        path: "https://onelake.dfs.fabric.microsoft.com/<workspaceId>/<lakehouseId>/Files"
```

### Table-Level File Reference

Each table specifies its file location relative to the source base path. You have three options:

| Key      | Description                                              | Example                          |
|----------|----------------------------------------------------------|----------------------------------|
| `file`   | Relative file path, appended to source `path`            | `"data/sales.parquet"`           |
| `folder` | Relative folder path, appended to source `path`          | `"data/sales"`                   |
| `path`   | Full absolute URL — overrides source `path` entirely     | `"https://.../.../file.parquet"` |

```yaml
tables:
  # Using 'file' — appended to source path
  - name: sales
    meta:
      openrowset:
        file: "data/sales.parquet"

  # Using 'folder' — reads all files in a folder
  - name: all_orders
    meta:
      openrowset:
        folder: "data/orders"
        format: PARQUET

  # Using 'path' — full URL, ignores source base path entirely
  - name: external_data
    meta:
      openrowset:
        path: "https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/latest/data.parquet"
```

### Path Resolution Rules

The macro resolves the final `BULK` path as follows:

1. If table-level `path` is set → use it as-is (full override)
2. If source-level `path` + table-level `file` or `folder` → concatenate them with `/`
3. If neither is provided → compilation error

When both `folder` and `file` are set, they are concatenated: `<source path>/<folder>/<file>`.

### Supported Storage Locations

| Storage                     | URL Format                                                                      |
|-----------------------------|---------------------------------------------------------------------------------|
| **OneLake (Fabric)**        | `https://onelake.dfs.fabric.microsoft.com/<workspaceId>/<lakehouseId>/Files/...` |
| **Azure Blob Storage**      | `https://<account>.blob.core.windows.net/<container>/...`                        |
| **ADLS Gen2 (https)**       | `https://<account>.dfs.core.windows.net/<container>/...`                         |
| **ADLS Gen2 (abfss)**       | `abfss://<container>@<account>.dfs.core.windows.net/...`                         |

> For OneLake paths, use `dfs.fabric.microsoft.com` (not `blob`). 

---

## Wildcards and Folder Reads

`OPENROWSET` supports reading multiple files at once using wildcards or folder paths. This works for all formats.

```yaml
tables:
  # Wildcard pattern — format auto-detected from *.parquet extension
  - name: partitioned_sales
    meta:
      openrowset:
        file: "data/year=2024/*.parquet"
    columns:
      - name: id
        data_type: int
      - name: amount
        data_type: "decimal(10,2)"

  # Recursive traversal — explicit format required (no file extension to detect from)
  - name: all_logs
    meta:
      openrowset:
        folder: "logs/**"
        format: CSV
    columns:
      - name: timestamp
        data_type: datetime2
      - name: message
        data_type: "varchar(8000)"

  # Bare folder — explicit format required
  - name: events
    meta:
      openrowset:
        folder: "data/events"
        format: JSONL
```

> **Rule:** When the path ends with a recognized file extension (`.parquet`, `.csv`, `.jsonl`, etc.), the format is auto-detected. When the path is a bare folder or uses `/**`, you must set `format` explicitly.

You can use the `filepath()` and `filename()` functions in your model SQL to filter or partition data:

```sql
-- models/stg_sales_2024.sql
SELECT
    sales.filepath(1) AS year_partition,
    sales.*
FROM {{ openrowset_source('my_lakehouse', 'partitioned_sales') }} AS sales
WHERE sales.filepath(1) = '2024'
```

---

## Format Detection

The macro auto-detects the file format from the path's file extension:

| Extension(s)                        | Detected Format |
|-------------------------------------|-----------------|
| `.parquet`, `.parq`                 | `PARQUET`       |
| `.csv`, `.tsv`                      | `CSV`           |
| `.jsonl`, `.ldjson`, `.ndjson`      | `JSONL`         |

To override auto-detection (e.g., for files with non-standard extensions), set `format` explicitly:

```yaml
tables:
  - name: custom_file
    meta:
      openrowset:
        file: "data/export.txt"
        format: CSV    # Force CSV format for a .txt file
```

---

## Format Options

Options are set in `meta.openrowset.options` at the source or table level. All options are passed directly to the `OPENROWSET` function.

### CSV Options

CSV sources automatically default to `HEADER_ROW = TRUE`. All other options use Fabric engine defaults unless you override them.

| Option             | Default (dbt-fabric)     | Engine Default               | Description                                       |
|--------------------|--------------------------|------------------------------|---------------------------------------------------|
| `HEADER_ROW`       | `TRUE`                   | `FALSE`                      | Whether the first row is a header row              |
| `FIELDTERMINATOR`  | *(not set)*              | `,` (comma)                  | Field delimiter character                          |
| `ROWTERMINATOR`    | *(not set)*              | `\r\n` (CRLF)               | Row delimiter character                            |
| `FIELDQUOTE`       | *(not set)*              | `"` (double quote)           | Quote character for enclosing fields               |
| `ESCAPECHAR`       | *(not set)*              | *(none)*                     | Escape character for special characters            |
| `PARSER_VERSION`   | *(not set)*              | `2.0`                        | CSV parser version: `'1.0'` or `'2.0'`            |
| `FIRSTROW`         | *(not set)*              | `1`                          | First row number to read (1-based)                 |
| `LASTROW`          | *(not set)*              | *(end of file)*              | Last row number to read                            |
| `CODEPAGE`         | *(not set)*              | `OEM`                        | `'ACP'`, `'OEM'`, `'RAW'`, or a code page number  |
| `DATAFILETYPE`     | *(not set)*              | `char`                       | `'char'` (single-byte) or `'widechar'` (UTF-16)   |

**CSV parser version notes:**

- **Parser 2.0** (default): Optimized for performance. Max column length 8000 chars. Max row size 8 MB. Does not support `DATA_COMPRESSION`.
- **Parser 1.0**: Supports all options and encodings. Does **not** support `HEADER_ROW`. Auto-fallback occurs when using 1.0-only options.

Example with custom CSV options:

```yaml
tables:
  - name: products
    meta:
      openrowset:
        file: "products_semicolon.csv"
        options:
          FIELDTERMINATOR: ";"
          FIELDQUOTE: '"'
          PARSER_VERSION: "2.0"
    columns:
      - name: product_id
        data_type: int
      - name: name
        data_type: "varchar(200)"
      - name: price
        data_type: "decimal(10,2)"
```

### Parquet Options

Parquet files typically need no additional options. The engine reads the schema from the file metadata.

| Option             | Description                                          |
|--------------------|------------------------------------------------------|
| `FIRSTROW`         | First row number to read                             |
| `ROWS_PER_BATCH`   | Approximate rows per batch (performance hint)        |
| `MAXERRORS`         | Max formatting errors before failure (default: 10)   |
| `CODEPAGE`         | Code page for character data                         |
| `DATAFILETYPE`     | `'char'` or `'widechar'`                             |

### JSONL Options

JSONL (newline-delimited JSON) files require each line to be a valid JSON document. The newline character is the record separator and cannot appear within a JSON document.

| Option             | Description                                          |
|--------------------|------------------------------------------------------|
| `FIRSTROW`         | First row number to read                             |
| `ROWS_PER_BATCH`   | Approximate rows per batch (performance hint)        |
| `MAXERRORS`         | Max formatting errors before failure (default: 10)   |
| `CODEPAGE`         | Code page for character data                         |
| `DATAFILETYPE`     | `'char'` or `'widechar'`                             |

### Option Override Hierarchy

Options merge in this order (last wins):

1. **Format defaults** — CSV gets `HEADER_ROW = TRUE`; Parquet and JSONL have no defaults
2. **Source-level** `meta.openrowset.options` — shared across all tables in the source
3. **Table-level** `meta.openrowset.options` — specific to one table

```yaml
sources:
  - name: shared_csv_source
    meta:
      openrowset:
        path: "https://onelake.dfs.fabric.microsoft.com/<workspaceId>/<lakehouseId>/Files"
        options:
          PARSER_VERSION: "2.0"          # applies to all tables in this source
    tables:
      - name: standard_csv
        meta:
          openrowset:
            file: "data/standard.csv"
            # Inherits: HEADER_ROW=TRUE (format default), PARSER_VERSION='2.0' (source)

      - name: semicolon_csv
        meta:
          openrowset:
            file: "data/semicolon.csv"
            options:
              FIELDTERMINATOR: ";"        # table-level override
              # Inherits: HEADER_ROW=TRUE (format default), PARSER_VERSION='2.0' (source)

      - name: no_header_csv
        meta:
          openrowset:
            file: "data/raw.csv"
            options:
              HEADER_ROW: false           # overrides the format default
              FIRSTROW: 1
```

To disable the `HEADER_ROW = TRUE` default, explicitly set `HEADER_ROW: false` at the table level.

---

## Column Schema (WITH Clause)

### Basic Column Definitions

Define columns in your source table's `columns` list. Each column needs a `name` and `data_type`. These generate the `WITH (...)` clause in the compiled SQL.

```yaml
columns:
  - name: customer_id
    data_type: int
  - name: first_name
    data_type: "varchar(100)"
  - name: amount
    data_type: "decimal(10,2)"
  - name: created_at
    data_type: datetime2
```

Compiled output:

```sql
WITH (
    customer_id int,
    first_name varchar(100),
    amount decimal(10,2),
    created_at datetime2
)
```

Common Fabric Warehouse data types for the WITH clause: `int`, `bigint`, `smallint`, `tinyint`, `bit`, `float`, `real`, `decimal(p,s)`, `numeric(p,s)`, `money`, `date`, `time`, `datetime2`, `datetimeoffset`, `varchar(n)`, `nvarchar(n)`, `char(n)`, `varbinary(n)`, `uniqueidentifier`.

### Column Path Mapping (JSON / Nested Fields)

For JSONL files or Parquet files with nested/complex types, use `meta.openrowset.path` on individual columns to specify a JSON path expression.

```yaml
columns:
  - name: event_id
    data_type: "varchar(50)"

  - name: event_date
    data_type: DATE
    meta:
      openrowset:
        path: "$.updated"          # Maps to a different JSON property name

  - name: user_id
    data_type: INT
    meta:
      openrowset:
        path: "$.user.id"          # Nested object access

  - name: fatal_cases
    data_type: INT
    meta:
      openrowset:
        path: "$.statistics.deaths"  # Deeply nested property
```

Compiled output:

```sql
WITH (
    event_id varchar(50),
    event_date DATE '$.updated',
    user_id INT '$.user.id',
    fatal_cases INT '$.statistics.deaths'
)
```

### Column Ordinal Mapping

Use `meta.openrowset.ordinal` to bind a column by its position (1-based) in the file rather than by name.

```yaml
columns:
  - name: cases
    data_type: INT
    meta:
      openrowset:
        ordinal: 3                  # Maps to the 3rd column in the file
```

Compiled output:

```sql
WITH (
    cases INT 3
)
```

### Schema Inference (No WITH Clause)

For Parquet files, you can omit the `columns` list entirely. The engine will infer the schema from the Parquet file metadata. This does **not** work for CSV or JSONL.

```yaml
tables:
  - name: customers_auto
    meta:
      openrowset:
        file: "customers.parquet"
    # No columns defined — engine auto-detects schema
```

Compiled output:

```sql
OPENROWSET(
    BULK '...',
    FORMAT = 'PARQUET'
)
AS [customers_auto]
```

> **Note:** Schema inference returns all columns from the file. For production models, define explicit column schemas to control data types and filter columns.

---

## Complete Examples

### Parquet from OneLake

```yaml
# sources.yml
sources:
  - name: my_lakehouse
    meta:
      openrowset:
        path: "https://onelake.dfs.fabric.microsoft.com/<workspaceId>/<lakehouseId>/Files"
    tables:
      - name: customers
        meta:
          openrowset:
            file: "customers.parquet"
        columns:
          - name: customer_id
            data_type: int
          - name: first_name
            data_type: "varchar(100)"
          - name: last_name
            data_type: "varchar(100)"
          - name: email
            data_type: "varchar(200)"
          - name: created_at
            data_type: datetime2
```

```sql
-- models/stg_customers.sql
SELECT * FROM {{ openrowset_source('my_lakehouse', 'customers') }}
```

**Compiled SQL:**

```sql
SELECT * FROM OPENROWSET(
    BULK 'https://onelake.dfs.fabric.microsoft.com/<workspaceId>/<lakehouseId>/Files/customers.parquet',
    FORMAT = 'PARQUET'
)
WITH (
    customer_id int,
    first_name varchar(100),
    last_name varchar(100),
    email varchar(200),
    created_at datetime2
)
AS [customers]
```

### CSV with Default Options

```yaml
tables:
  - name: orders
    meta:
      openrowset:
        file: "orders.csv"
    columns:
      - name: order_id
        data_type: int
      - name: customer_id
        data_type: int
      - name: order_date
        data_type: date
      - name: amount
        data_type: "decimal(10,2)"
      - name: status
        data_type: "varchar(50)"
```

**Compiled SQL:**

```sql
SELECT * FROM OPENROWSET(
    BULK '...Files/orders.csv',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE
)
WITH (
    order_id int,
    customer_id int,
    order_date date,
    amount decimal(10,2),
    status varchar(50)
)
AS [orders]
```

### CSV with Custom Delimiter

```yaml
tables:
  - name: products
    meta:
      openrowset:
        file: "products_semicolon.csv"
        options:
          FIELDTERMINATOR: ";"
    columns:
      - name: product_id
        data_type: int
      - name: name
        data_type: "varchar(200)"
      - name: category
        data_type: "varchar(100)"
      - name: price
        data_type: "decimal(10,2)"
      - name: in_stock
        data_type: bit
```

**Compiled SQL:**

```sql
SELECT * FROM OPENROWSET(
    BULK '...Files/products_semicolon.csv',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR = ';'
)
WITH (
    product_id int,
    name varchar(200),
    category varchar(100),
    price decimal(10,2),
    in_stock bit
)
AS [products]
```

### TSV (Tab-Separated)

`.tsv` files are auto-detected as CSV format. Set the tab delimiter explicitly:

```yaml
tables:
  - name: orders_tsv
    meta:
      openrowset:
        file: "orders.tsv"
        options:
          FIELDTERMINATOR: "\t"
    columns:
      - name: order_id
        data_type: int
      - name: customer_id
        data_type: int
      - name: order_date
        data_type: date
      - name: amount
        data_type: "decimal(10,2)"
      - name: status
        data_type: "varchar(50)"
```

### JSONL with Nested Fields

```yaml
tables:
  - name: events
    meta:
      openrowset:
        file: "events.jsonl"
    columns:
      - name: event_id
        data_type: "varchar(50)"
      - name: event_type
        data_type: "varchar(50)"
      - name: timestamp
        data_type: "varchar(50)"
      - name: user_id
        data_type: INT
        meta:
          openrowset:
            path: "$.user.id"
      - name: user_name
        data_type: "varchar(100)"
        meta:
          openrowset:
            path: "$.user.name"
      - name: page
        data_type: "varchar(200)"
        meta:
          openrowset:
            path: "$.properties.page"
```

**Compiled SQL:**

```sql
SELECT * FROM OPENROWSET(
    BULK '...Files/events.jsonl',
    FORMAT = 'JSONL'
)
WITH (
    event_id varchar(50),
    event_type varchar(50),
    timestamp varchar(50),
    user_id INT '$.user.id',
    user_name varchar(100) '$.user.name',
    page varchar(200) '$.properties.page'
)
AS [events]
```

### Parquet without Schema (Auto-Detect)

```yaml
tables:
  - name: customers_auto
    meta:
      openrowset:
        file: "customers.parquet"
```

```sql
-- models/explore_customers.sql
SELECT TOP 100 * FROM {{ openrowset_source('my_lakehouse', 'customers_auto') }}
```

**Compiled SQL:**

```sql
SELECT TOP 100 * FROM OPENROWSET(
    BULK '...Files/customers.parquet',
    FORMAT = 'PARQUET'
)
AS [customers_auto]
```

### External Storage (Azure Blob)

Use a full `path` at the table level to read from storage outside your Lakehouse:

```yaml
sources:
  - name: external_blob
    tables:
      - name: covid_data
        meta:
          openrowset:
            path: "https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/bing_covid-19_data/latest/bing_covid-19_data.parquet"
        columns:
          - name: id
            data_type: int
          - name: confirmed
            data_type: int
          - name: deaths
            data_type: int
          - name: country_region
            data_type: "varchar(200)"
```

### Multiple Tables from One Source

Define a shared base path once and reference multiple files:

```yaml
sources:
  - name: my_lakehouse
    meta:
      openrowset:
        path: "https://onelake.dfs.fabric.microsoft.com/<workspaceId>/<lakehouseId>/Files"
    tables:
      - name: customers
        meta:
          openrowset:
            file: "customers.parquet"
        columns:
          - name: customer_id
            data_type: int
          - name: first_name
            data_type: "varchar(100)"
          - name: last_name
            data_type: "varchar(100)"

      - name: orders
        meta:
          openrowset:
            file: "orders.csv"
        columns:
          - name: order_id
            data_type: int
          - name: customer_id
            data_type: int
          - name: amount
            data_type: "decimal(10,2)"

      - name: events
        meta:
          openrowset:
            file: "events.jsonl"
        columns:
          - name: event_id
            data_type: "varchar(50)"
          - name: event_type
            data_type: "varchar(50)"
```

```sql
-- models/stg_customers.sql
SELECT * FROM {{ openrowset_source('my_lakehouse', 'customers') }}

-- models/stg_orders.sql
SELECT * FROM {{ openrowset_source('my_lakehouse', 'orders') }}

-- models/stg_events.sql
SELECT * FROM {{ openrowset_source('my_lakehouse', 'events') }}
```

---

## Permissions

The executing principal requires:

| Permission                               | Scope                                           |
|------------------------------------------|-------------------------------------------------|
| `ADMINISTER DATABASE BULK OPERATIONS`    | On the Fabric Warehouse                         |
| **Storage Blob Data Reader** (or higher) | On the target storage account/container          |

For **OneLake** paths within the same Fabric tenant, permissions are enforced via Microsoft Entra ID passthrough — no additional storage role is needed if the user has Fabric workspace access.

Grant the bulk operations permission:

```sql
GRANT ADMINISTER DATABASE BULK OPERATIONS TO [user@domain.com];
```

---

## Limitations

- **DELTA format** is not supported in Fabric Warehouse `OPENROWSET`. Use Lakehouse tables or shortcuts.
- **Schema inference** (omitting the WITH clause) only works for **Parquet** files. CSV and JSONL require explicit column definitions.
- **CSV Parser 2.0** has a max column length of 8,000 characters and max row size of 8 MB.
- **CSV Parser 1.0** does not support `HEADER_ROW`.
- **JSONL** files must use newlines as record separators. Newlines within a JSON document are not allowed.
- `SINGLE_BLOB`, `SINGLE_CLOB`, and `SINGLE_NCLOB` options are not supported in Fabric Warehouse.
- The `DATA_SOURCE` option (named external data sources) is not supported in Fabric Warehouse — use full absolute URIs.
- File paths must be absolute URLs. Relative paths are only supported with `DATA_SOURCE`, which is not available in Fabric Warehouse.

---

## Troubleshooting

### "Cannot detect file format from path"

The file extension is not recognized. Set `format` explicitly:

```yaml
meta:
  openrowset:
    file: "data/export.txt"
    format: CSV
```

### "OPENROWSET source requires a file path"

Neither a table-level `file`/`folder`/`path` nor a source-level `path` was configured. Ensure your source has a base `path` and your table has a `file` or `folder`.

### "Source not found"

The source name and table name in `openrowset_source('source_name', 'table_name')` must match exactly what's defined in `sources.yml`.

### Permission errors (403 / Access Denied)

- Verify the executing user has `ADMINISTER DATABASE BULK OPERATIONS`
- For non-OneLake storage, ensure the user has **Storage Blob Data Reader** on the storage account
- For OneLake, verify workspace access in Fabric

### "TCP Provider: Error code 0x2746"

This is a connection error, not related to OPENROWSET. Check your Fabric Warehouse connection settings in `profiles.yml`.

### Column type mismatch errors

Ensure the `data_type` in your column definitions matches the actual data in the file. Common issues:
- Using `date` for a column that contains datetime values → use `datetime2`
- Using `int` for values that exceed int range → use `bigint`
- Not specifying length for `varchar` → use `varchar(n)` with an appropriate length
