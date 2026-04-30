# dbt-fabric

[dbt](https://www.getdbt.com) adapter for Microsoft Fabric Synapse Data Warehouse.

The adapter supports dbt-core 1.4 or newer and follows the same versioning scheme.
E.g. version 1.1.x of the adapter will be compatible with dbt-core 1.1.x.

## Documentation

We've bundled all documentation on the dbt docs site
* [Profile setup & authentication](https://docs.getdbt.com/docs/core/connect-data-platform/fabric-setup)
* [Adapter documentation, usage and important notes](https://docs.getdbt.com/reference/resource-configs/fabric-configs)

## Installation

Latest version: ![PyPI](https://img.shields.io/pypi/v/dbt-fabric?label=latest&logo=pypi)

```shell
pip install -U dbt-fabric
```

### Driver Options

dbt-fabric supports two database drivers:

| Driver | Python Version | ODBC Required | Recommended |
|--------|----------------|---------------|-------------|
| **mssql-python** (default) | 3.10+ | No | ✅ Yes |
| **pyodbc** (fallback) | 3.9+ | Yes | Legacy |

By default, dbt-fabric uses Microsoft's native `mssql-python` driver if available (Python 3.10+), 
and automatically falls back to `pyodbc` otherwise.

#### Using mssql-python (Default - Recommended)

No additional installation required! Just install dbt-fabric:

```shell
pip install dbt-fabric
```

<details><summary>System Dependencies for mssql-python</summary>
<p>

The mssql-python driver bundles the native SQL Server driver libraries, but requires some system libraries for SSL/Kerberos support:

**macOS:**
```shell
# OpenSSL is required
brew install openssl
```

**Debian/Ubuntu:**
```shell
sudo apt-get install -y libltdl7 libkrb5-3 libgssapi-krb5-2
```

**RHEL/CentOS/Fedora:**
```shell
sudo dnf install -y libtool-ltdl krb5-libs
```

**SUSE/openSUSE:**
```shell
sudo zypper install -y libltdl7 libkrb5-3 libgssapi-krb5-2
```

**Alpine Linux:**
```shell
apk add libtool krb5-libs krb5-dev
```

</p>
</details>

#### Using pyodbc (Legacy/Fallback)

If you need to use pyodbc (for specific SQL types like `geography`, `geometry`, `xml`), 
install the optional dependency and the Microsoft ODBC driver:

```shell
pip install "dbt-fabric[pyodbc]"
```

ODBC Driver installation:
[Windows](https://docs.microsoft.com/nl-be/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16#download-for-windows) |
[macOS](https://docs.microsoft.com/nl-be/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver16) |
[Linux](https://docs.microsoft.com/nl-be/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver16)

<details><summary>Debian/Ubuntu ODBC Setup</summary>
<p>

Make sure to install the ODBC headers as well as the driver linked above:

```shell
sudo apt-get install -y unixodbc-dev
```

</p>
</details>

## Driver Configuration

### Automatic Driver Selection (Default)

By default, dbt-fabric uses `mssql-python` if available, falling back to `pyodbc`:

```yaml
# profiles.yml - no driver_backend needed
fabric:
  target: dev
  outputs:
    dev:
      type: fabric
      server: myserver.database.fabric.microsoft.com
      database: mydb
      schema: dbo
      authentication: ServicePrincipal
      tenant_id: "{{ env_var('AZURE_TENANT_ID') }}"
      client_id: "{{ env_var('AZURE_CLIENT_ID') }}"
      client_secret: "{{ env_var('AZURE_CLIENT_SECRET') }}"
```

### Explicit Driver Selection

Force a specific driver with `driver_backend`:

```yaml
# Force mssql-python (fail if unavailable)
fabric:
  outputs:
    dev:
      type: fabric
      driver_backend: mssql-python
      server: myserver.database.fabric.microsoft.com
      # ...

# Force pyodbc (requires ODBC driver installed)
fabric:
  outputs:
    dev:
      type: fabric
      driver_backend: pyodbc
      driver: "ODBC Driver 18 for SQL Server"
      server: myserver.database.fabric.microsoft.com
      # ...
```

### Environment Variable Override

Override the driver backend without changing profiles.yml:

```shell
DBT_FABRIC_DRIVER_BACKEND=pyodbc dbt run
```

### When to Use pyodbc

Use `driver_backend: pyodbc` if you:
- Use unsupported SQL types (`geography`, `geometry`, `xml`, etc.)
- Need compatibility with existing pyodbc-based tooling

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
