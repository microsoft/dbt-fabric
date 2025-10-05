# Comparison between dbt-fabric and dbt-fabric-samdebruyn

This adapter has all the features of [Microsoft's dbt-fabric adapter](https://github.com/microsoft/dbt-fabric), plus some additional features.
The following features are exclusive to dbt-fabric-samdebruyn:

## Support for Python models

This adapter supports [Python models](https://docs.getdbt.com/docs/build/python-models). To use this, just add information about your [Fabric Workspace](configuration.md#workspace_name) and [Lakehouse](configuration.md#lakehouse_name) to the `profiles.yml` file.

## Automatically find the host name of your Fabric Workspace

It can be tedious to find the correct host name for your Fabric Workspace, especially if you have separate Workspaces for development and production environments.

This adapter will automatically retrieve the host name for your Fabric Workspace, based on the [`workspace_name`](configuration.md#workspace_name) or [`workspace_id`](configuration.md#workspace_id) provided in the configuration.

This allows you to write a configuration like this:

```yaml
default:
  target: dev
    outputs:
    dev:
      type: fabric
      driver: ODBC Driver 18 for SQL Server
      workspace: "gold_{{ env_var('FABRIC_ENV', 'dev') }}"
      database: dwh
      schema: dbt
```

Then, to run dbt against your production environment/Workspace, you can simply set the `FABRIC_ENV` environment variable to `prod` (if your Workspaces are named accordingly).

## Extended support for [authentication methods](configuration.md#authentication)

While most authentication methods have been contributed back to dbt-fabric, some newer options are only available in this adapter.

## MERGE in incremental and microbatch models

Incremental models in dbt-fabric support the `append`, `insert_overwrite`, and `delete+insert` strategies.

This adapter adds support for [MERGE](https://learn.microsoft.com/sql/t-sql/statements/merge-transact-sql?view=sql-server-ver17&WT.mc_id=MVP_310840).

```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge'
) }}

select * from source('my_source', 'my_table')
{% if is_incremental() %}
where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

When using the `merge` strategy, dbt will generate a `MERGE` statement that matches on the `unique_key` and updates existing records or inserts new records as necessary. The `unique_key` can be a single column or a list of columns.

The adapter will use the `merge` strategy by default if a `unique_key` is provided and no `incremental_strategy` is specified.

This also works for microbatch models:

```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='microbatch',
    batch_size='day',
    begin='2025-01-01',
    event_time='created_at'
) }}

select * from source('my_source', 'my_table')
{% endif %}
```

## Better support for popular packages

[dbt-utils](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/) is already fully supported and more packages are being tested and added.

## Plenty of bugfixes

The quality of this adapter is guaranteed by an extensive test suite of integration tests, which run on every change. Through this process, quite a few bugs have been found and fixed.

## More on the roadmap

Ideas for future improvements include:

- Synchronization of documentation between Microsoft Purview and dbt docs
- Merging support for Spark SQL models into this adapter and allowing users to choose between Fabric SQL and Spark SQL models in the same project
- Integration with external Iceberg/Delta Lake tables
- Integration with external Iceberg/Delta Lake catalogs
- [Create an issue with your idea](https://github.com/sdebruyn/dbt-fabric/issues)

## Paid support

For companies that want to use this adapter in production, [I offer paid support and consulting services](https://debruyn.dev/services/).