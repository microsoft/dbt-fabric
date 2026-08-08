# dbt-fabric

[dbt](https://www.getdbt.com) adapter for Microsoft Fabric Synapse Data Warehouse.

The adapter supports dbt-core 1.4 or newer and follows the same versioning scheme.
E.g. version 1.1.x of the adapter will be compatible with dbt-core 1.1.x.

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

## License

[![PyPI - License](https://img.shields.io/pypi/l/dbt-fabric)](https://github.com/microsoft/dbt-fabric/blob/main/LICENSE)

## Code of Conduct

This project and everyone involved is expected to follow the [Microsoft Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
