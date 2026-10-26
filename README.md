# dbt-fabric

[dbt](https://www.getdbt.com) adapter for Microsoft Fabric Synapse Data Warehouse.

The 1.12.x adapter series requires dbt-core 1.12 or newer.
Adapter minor versions follow the corresponding dbt-core minor version.

## Documentation

We've bundled all documentation on the dbt docs site
* [Profile setup & authentication](https://docs.getdbt.com/docs/core/connect-data-platform/fabric-setup)
* [Adapter documentation, usage and important notes](https://docs.getdbt.com/reference/resource-configs/fabric-configs)

## Installation

This adapter uses [`mssql-python`](https://github.com/microsoft/mssql-python) to connect to Fabric, which bundles its own driver.

Latest version: ![PyPI](https://img.shields.io/pypi/v/dbt-fabric?label=latest&logo=pypi)

```shell
pip install -U dbt-fabric
```

## dbt package compatibility

The following releases were reviewed for the 1.12 adapter update. This is a
macro compatibility review, not certification of every package feature.

| Package | Reviewed release | Fabric integration |
| --- | --- | --- |
| `dbt-labs/dbt_utils` | [1.4.1](https://github.com/dbt-labs/dbt-utils/releases/tag/1.4.1) | Existing Fabric overrides; upstream supplies case-insensitive `star(rename=...)` and `recency` handling for `--empty`. |
| `metaplane/dbt_expectations` | [0.10.10](https://github.com/metaplane/dbt-expectations/releases/tag/0.10.10) | Updated within-record uniqueness override to use the upstream quoting fix, missing-value helper, and type-independent hashes. Also corrected percentage tolerance, standard-deviation, and grouped-recency behavior. |
| `godatadriven/dbt_date` | [0.21.0](https://github.com/godatadriven/dbt-date/releases/tag/0.21.0) | Existing Fabric overrides; epoch conversions now handle large and negative integer timestamps with microsecond precision. Nanoseconds are truncated to microseconds. New compile-time date comparisons and localization helpers come from the package. |
| `elementary-data/elementary` | [0.26.1](https://github.com/elementary-data/dbt-data-reliability/releases/tag/0.26.1) | Package supplies its own Fabric macros, including T-SQL sample formatting. Upgrade the dbt package for those fixes; the Elementary CLI is a separate dependency. |
| `dbt-labs/dbt_project_evaluator` | [1.4.1](https://github.com/dbt-labs/dbt-project-evaluator/releases/tag/v1.4.1) | Package supplies its own Fabric macros, including directory-path fixes. Resource-ID handling and Jinja-comment filtering also come from the package. |
| `brooklyn-data/dbt_artifacts` | [2.11.1](https://github.com/brooklyn-data/dbt_artifacts/releases/tag/2.11.1) | Fabric uploads for all 11 datasets, including execution timing and affected-row counts. Core models require the separate upstream patch described below. Snowflake-only analytics remain Snowflake-only. |
| `Snowflake-Labs/dbt_constraints` | [1.0.10](https://github.com/Snowflake-Labs/dbt_constraints/releases/tag/1.0.10) | Fabric PK, unique, and FK creation, catalog lookups, permission checks, and pre-build FK release. Keys are `NOT ENFORCED`; see the limitations below. |
| `dbt-labs/audit_helper` | [0.14.0](https://github.com/dbt-labs/dbt-audit-helper/releases/tag/0.14.0) | Metadata comparison includes presence flags and full type parameters. Corrected event-time bounds and quoting in column-difference comparisons. |
| `data-mie/dbt_profiler` | [1.0.0](https://github.com/data-mie/dbt-profiler/releases/tag/1.0.0) | View-safe profiling, fractional numeric/bit averages, per-group medians, centered standard deviations, and untruncated numeric extrema. |
| `dbt-labs/dbt_external_tables` | [0.12.4](https://github.com/dbt-labs/dbt-external-tables/releases/tag/0.12.4) | OPENROWSET-backed views for Parquet, CSV, and JSONL; escaped schema creation and source identifiers, including restaging and full refresh. These are not persistent external tables. |

Use the maintained `metaplane/dbt_expectations` and `godatadriven/dbt_date`
packages rather than their older Calogica distributions. Configure these
dispatch entries in `dbt_project.yml` to select the adapter's overrides:

```yaml
dispatch:
  - macro_namespace: dbt_utils
    search_order: ["dbt", "dbt_utils"]
  - macro_namespace: dbt_expectations
    search_order: ["dbt", "dbt_expectations"]
  - macro_namespace: dbt_date
    search_order: ["dbt", "dbt_date"]
  - macro_namespace: dbt_artifacts
    search_order: ["dbt", "dbt_artifacts"]
  - macro_namespace: dbt_constraints
    search_order: ["dbt", "dbt_constraints"]
  - macro_namespace: audit_helper
    search_order: ["dbt", "audit_helper"]
  - macro_namespace: dbt_profiler
    search_order: ["dbt", "dbt_profiler"]
  - macro_namespace: dbt_external_tables
    search_order: ["dbt", "dbt_external_tables"]

vars:
  "dbt_date:time_zone": "UTC"
```

Keep Elementary and Project Evaluator's native dispatch configuration.
Package upgrades are made in the consuming project's `packages.yml`, followed
by `dbt deps`; upgrading the adapter does not upgrade installed dbt packages.

Support remains partial: regex tests are not covered by this update, Elementary
0.26.1 explicitly rejects regex tests on Fabric, and dbt-expectations median /
quantile tests still need T-SQL-specific windowing adaptations. Warehouse regex
scalar functions are currently a
[preview feature](https://learn.microsoft.com/fabric/data-warehouse/regular-expressions);
their availability alone does not make all upstream regex macros compatible.

### dbt_artifacts upstream dependency

**Adapter changes alone do not make the unmodified 2.11.1 release compatible.**
The package's model SQL and non-dispatched column lists contain SQL Server-only
branches, and its project configuration enables unsupported documentation
persistence on Fabric. The unpublished
[upstream compatibility patch](tests/functional/adapter/fixtures/dbt_artifacts_fabric.patch)
adds Fabric to those branches without enabling Snowflake-only models.
It has not been submitted or released upstream.

Until a compatible upstream release is available, use a patched package revision,
or download the patch and apply it to the installed 2.11.1 package after `dbt deps`:

```shell
git -C dbt_packages/dbt_artifacts apply /absolute/path/to/dbt_artifacts_fabric.patch
```

Configure the dispatch entry above and the package's upload hook:

```yaml
on-run-end:
  - "{{ dbt_artifacts.upload_results(results) }}"
```

Initialize the package tables with `dbt run --select package:dbt_artifacts`
before running other project commands. JSON metadata is stored as text.
The integration test explicitly applies the patch to its temporary package
installation, then exercises all 11 uploads and all 34 enabled core models;
it does not demonstrate compatibility with the unpatched release.

### dbt_constraints limitations

Fabric primary and unique keys use `NONCLUSTERED NOT ENFORCED`; foreign keys use
`NOT ENFORCED` and must reference the same database. Passing dbt tests provides
the data validation; Warehouse does not enforce those keys during subsequent DML.
Declare `NOT NULL` in an enforced model contract and rebuild the table to establish
physical nullability. The package warns rather than claiming to add `NOT NULL`
to an existing nullable column.

The package's pre-build hook releases foreign keys on tables selected for rebuild.
If a referenced table has an incoming FK from an unselected table, the build fails
before dropping any keys: include the dependent table in `dbt build` so its FK can
be recreated. Keep the package's default start/end hooks enabled, and include the
constraint tests in the build. Disabling release-before-build can leave keys that
block table replacement or truncation.

Profiling results have no implicit row order; order the consuming query explicitly.
Quote special identifiers in `group_by`, for example `[order date]`.

## Performance guidance for large projects

Fabric Warehouse DDL operations (e.g. `CREATE TABLE`, `sp_rename`) hold catalog locks that can block `sys.tables`/`sys.views` reads from concurrent dbt sessions. With many models and high thread counts this causes `list_<schema>` steps to stall for minutes.

**Strongly recommended for projects with 500+ models or concurrent dbt runs:**

```yaml
# profiles.yml
my_fabric_project:
  target: dev
  outputs:
    dev:
      type: fabric
      # ... connection settings ...
      threads: 4          # keep low (4–8) to reduce catalog lock pressure
      query_timeout: 30   # fail fast on blocked catalog reads (seconds)
```

In your `dbt_project.yml`:

```yaml
flags:
  cache_selected_only: true  # only list schemas for models in the current run
```

Or pass `--no-populate-cache` on the CLI for a single run. This prevents dbt from listing every schema in the warehouse upfront, significantly reducing catalog read pressure during concurrent runs.

## Changelog

See [the changelog](CHANGELOG.md)

## Contributing

[![Unit tests](https://github.com/microsoft/dbt-fabric/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/microsoft/dbt-fabric/actions/workflows/unit-tests.yml)
[![Integration tests on Azure](https://github.com/microsoft/dbt-fabric/actions/workflows/integration-tests-azure.yml/badge.svg)](https://github.com/microsoft/dbt-fabric/actions/workflows/integration-tests-azure.yml)
[![Publish Docker images for CI/CD](https://github.com/microsoft/dbt-fabric/actions/workflows/publish-docker.yml/badge.svg)](https://github.com/microsoft/dbt-fabric/actions/workflows/publish-docker.yml)

This adapter is Microsoft-maintained.
You are welcome to contribute by creating issues, opening or reviewing pull requests.
If you're unsure how to get started, check out our [contributing guide](CONTRIBUTING.md).

### Core 1.12 development environment

The development group follows published Core 1.12 patch releases available through
the configured CFS feed, and `uv.lock` pins the resolved versions. See the
[contributing guide](CONTRIBUTING.md#package-feed) for feed access and setup.

```shell
uv sync --locked
uv run --locked pytest tests/unit
```

**Known Core 1.12.5 limitation:** `run-operation --sql` starts a transaction but
does not commit it before releasing the connection. Inline writes can therefore
be rolled back despite a success result. This requires a dbt Core fix; Fabric's
rollback-on-close behavior remains unchanged.

When using `--empty` with an explicit SQL alias, disable the generated relation
alias so T-SQL receives only one alias. This retains the zero-row filter:

```sql
select * from
{{ source('my_source', 'my_table').incorporate(require_alias=False) }} as t
```

The same pattern applies to `ref()`. Omitting the model's explicit alias also
avoids the duplicate-alias problem.

## License

[![PyPI - License](https://img.shields.io/pypi/l/dbt-fabric)](https://github.com/microsoft/dbt-fabric/blob/main/LICENSE)

## Code of Conduct

This project and everyone involved is expected to follow the [Microsoft Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
