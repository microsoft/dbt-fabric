# Configuration

You'll need to create a profile in the [profiles.yml](https://docs.getdbt.com/docs/core/connect-data-platform/profiles.yml) file to connect to Microsoft Fabric. The adapter offers several ways to configure your connection so that it can be flexible to your needs.

## Example profiles.yml

Here's an example `profiles.yml` file that connects to a Microsoft Fabric instance using the ODBC Driver 18 for SQL Server and automatic authentication:

```yaml
default:
  target: dev
    outputs:
    dev:
      type: fabric
      driver: ODBC Driver 18 for SQL Server
      workspace: your workspace name
      database: name_of_your_data_warehouse
      schema: schema_to_build_models_in
```

??? tip "Use environment variables anywhere"

    You can use environment variables in any configuration value in your `profiles.yml` file.

    ```yaml
    default:
      target: dev
      outputs:
        dev:
          type: fabric
          driver: ODBC Driver 18 for SQL Server
          ...
          client_id: "{{ env_var('AZURE_CLIENT_ID', 'an optional default value') }}"
          client_secret: "{{ env_var('AZURE_CLIENT_SECRET') }}"
    ```
    
    Make sure to surround the Jinja block with quotes.

## All configuration options

### `type`

**Required configuration option.**

Only possible value: `fabric`

The type of the adapter. This must be set to `fabric` to use this adapter.

### `driver`

**Required configuration option.**

Possible values:

- `ODBC Driver 18 for SQL Server`
- `ODBC Driver 17 for SQL Server`

The ODBC driver to use to connect to Microsoft Fabric. You must have [this driver installed](installation.md) on your machine.

### `host`

Alias: `server`<br>
Example value: `abc-123.datawarehouse.fabric.microsoft.com`

The server part of your connection string. This is unique per Workspace in Fabric.

You can leave this empty and let the adapter find it automatically by providing information about your Workspace. See [`workspace_name`](#workspace_name) and [`workspace_id`](#workspace_id).

### `database`

**Required configuration option.**

Example value: `gold_dwh`

The name of your data warehouse in Fabric. It's recommended to avoid using spaces in the name of your data warehouse, although it's supported.

### `schema`

**Required configuration option.**

Example value: `dbt`

The schema in your data warehouse where dbt will build models. You must have write access to this schema. It's recommended to avoid using spaces in the schema name, although it's supported.

??? tip "Override per model"

    The schema can be overridden per model/seed/test/folder/... using the [`schema`](https://docs.getdbt.com/reference/resource-configs/schema) config.

??? tip "Further customization"

    You can even completely customize how dbt generates the schema name using the [`generate_schema_name`](https://docs.getdbt.com/docs/build/custom-schemas) macro.

### `workspace_name` :fontawesome-brands-python:

Alias: `workspace`<br>
Example value: `My Workspace`

The name of your Fabric Workspace. This is used to automatically find the [`host`](#host) value for you.

Not used if [`workspace_id`](#workspace_id) is also provided.
Not used for SQL models (so only for Python models) if `host` is provided.

??? info "Python models"

    If you are using Python models in your project, either [`workspace_name`](#workspace_name) or [`workspace_id`](#workspace_id) must be provided.

??? info "auth: ActiveDirectoryServicePrincipal"

    When using this option together with [`authentication`](#authentication) set to `ActiveDirectoryServicePrincipal`, you also need to provide the [`tenant_id`](#tenant_id) option.

Behind the scenes, the adapter will do an API call to first find the Workspace ID, and then use that to find the server name.

### `workspace_id` :fontawesome-brands-python:

Example value: `7275c94d-9280-438b-bd67-ffeb8c305c9b`

The ID of your Fabric Workspace. This is used to automatically find the `host` value for you.

Not used for SQL models (so only for Python models) if `host` is provided.

??? info "Python models"

    If you are using Python models in your project, either [`workspace_name`](#workspace_name) or [`workspace_id`](#workspace_id) must be provided.

??? info "auth: ActiveDirectoryServicePrincipal"

    When using this option together with [`authentication`](#authentication) set to `ActiveDirectoryServicePrincipal`, you also need to provide the [`tenant_id`](#tenant_id) option.

Behind the scenes, the adapter will do an API call to first find the server name.

### `authentication` :fontawesome-solid-lock:

Alias: `auth`<br>
Possible values (case insensitive):

- [`ActiveDirectoryIntegrated`](#activedirectoryintegrated)
- [`ActiveDirectoryPassword`](#activedirectorypassword)
- [`ActiveDirectoryServicePrincipal`](#activedirectoryserviceprincipal)
- [`ActiveDirectoryInteractive`](#activedirectoryinteractive)
- [`ActiveDirectoryMsi`](#activedirectorymsi)
- [`auto`](#auto) (default)
- [`CLI`](#cli)
- [`environment`](#environment)
- [`FabricSpark` / `SynapseSpark`](#fabricspark)

The adapter supports an authentication method for every use case. The default is `auto`, which will try to use the best available method depending on your environment.

If you can't find a suitable method for your use case, please [open an issue](https://github.com/sdebruyn/dbt-fabric/issues).

#### `ActiveDirectoryIntegrated`

Authenticate with a Windows credential federated through Microsoft Entra ID with integrated authentication. This works on domain-joined machines. On macOS and Linux, it's recommended to use the latest version of the ODBC Driver 18.

??? info "Workspace info and Python models"

    This is not compatible with the [`workspace_name`](#workspace_name) or [`workspace_id`](#workspace_id) options or with Python models. In this case, it's recommended to look at the [`auto`](#auto) or [`CLI`](#cli) options as alternatives.

#### `ActiveDirectoryPassword`

Authenticate with a Microsoft Entra ID username and password. You must provide the [`username`](#username) and [`password`](#password) options.

??? info "Workspace info and Python models"

    This is not compatible with the [`workspace_name`](#workspace_name) or [`workspace_id`](#workspace_id) options or with Python models. In this case, it's recommended to look at the [`auto`](#auto) or [`CLI`](#cli) options as alternatives.

#### `ActiveDirectoryServicePrincipal`

Authenticate with a Microsoft Entra ID service principal using a client ID and client secret. You must provide the [`client_id`](#client_id) and [`client_secret`](#client_secret) options.

??? info "Tenant ID required for Workspace info or Python models"

    If you are using [`workspace_name`](#workspace_name) or [`workspace_id`](#workspace_id), you also need to provide the [`tenant_id`](#tenant_id) option.

#### `ActiveDirectoryInteractive`

Authenticate with a Microsoft Entra ID username and password using an interactive prompt. You must provide the [`username`](#username) option.

??? info "Workspace info and Python models"

    This is not compatible with the [`workspace_name`](#workspace_name) or [`workspace_id`](#workspace_id) options or with Python models. In this case, it's recommended to look at the [`auto`](#auto) or [`CLI`](#cli) options as alternatives.

#### `ActiveDirectoryMsi`

Authenticate with a managed identity configured in your environment. This is typically used when running in Azure.

??? info "Workspace info and Python models"

    This is not compatible with the [`workspace_name`](#workspace_name) or [`workspace_id`](#workspace_id) options or with Python models. In this case, it's recommended to look at the [`auto`](#auto) or [`CLI`](#cli) options as alternatives.

#### `auto`

**Default authentication method.**

This will try to authenticate using the best available method depending on your environment. It can automatically pick up configurations for managed identities, service principals, Azure CLI/PowerShell users, and more. The full list and order of methods is described on [Microsoft Learn](https://learn.microsoft.com/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python&WT.mc_id=MVP_310840).

#### `CLI`

Authenticate using the credentials from the Azure CLI. You must be logged in using `az login`. There have been reports of issues when using an outdated version of the Azure CLI, so make sure to use the latest version. Your account does not need to have access to any Azure subscriptions or resources and the selected Azure subscription does not matter.

Since the Azure CLI supports [a variety of authentication methods](https://learn.microsoft.com/cli/azure/authenticate-azure-cli?view=azure-cli-latest&WT.mc_id=MVP_310840), this is a flexible option that works in many scenarios.

#### `environment`

Authenticate using environment variables. This works similarly to the `auto` method, but only uses environment variables. See [Microsoft Learn](https://learn.microsoft.com/python/api/azure-identity/azure.identity.environmentcredential?view=azure-python&WT.mc_id=MVP_310840) for the list of supported environment variables.

#### `FabricSpark`

Alias: `SynapseSpark`

This authentication methods works inside a Fabric or Synapse notebook. It uses [NotebookUtils](https://learn.microsoft.com/fabric/data-engineering/notebook-utilities?WT.mc_id=MVP_310840) to get an access token for the current user.

### `username` :fontawesome-solid-lock:

Aliases: `UID`, `user`<br>
Example value: `satya.nadella@microsoft.com`

The username to use for authentication. This is required if you are using the `ActiveDirectoryPassword` or `ActiveDirectoryInteractive` authentication methods.

### `password` :fontawesome-solid-lock:

Aliases: `PWD`, `pass`<br>
Example value: `IL0veC0p!lot!`

The password to use for authentication. This is required if you are using the `ActiveDirectoryPassword` authentication method.

It's not recommended to hardcode this in your `profiles.yml` file. Instead, [use an environment variable](#example-profilesyml).

### `client_id` :fontawesome-solid-lock:

Alias: `app_id`<br>
Example value: `123e4567-e89b-12d3-a456-426614174000`

The client ID of the Microsoft Entra ID application (service principal) to use for authentication. This is required if you are using the `ActiveDirectoryServicePrincipal` authentication method.

### `client_secret` :fontawesome-solid-lock:

Alias: `app_secret`<br>
Example value: `0123456789abcdef`

The client secret of the Microsoft Entra ID application (service principal) to use for authentication. This is required if you are using the `ActiveDirectoryServicePrincipal` authentication method.

It's not recommended to hardcode this in your `profiles.yml` file. Instead, [use an environment variable](#example-profilesyml).

### `tenant_id` :fontawesome-solid-lock:

Example value: `72f988bf-86f1-41af-91ab-2d7cd011db47`

When `authentication` is set to `ActiveDirectoryServicePrincipal`, the adapter needs to know your Microsoft Entra ID tenant ID to be able to authenticate. This is required if you are using [`workspace_name`](#workspace_name) or [`workspace_id`](#workspace_id) or if you are using Python models.

### `access_token` :fontawesome-solid-lock:

This option overrides all other authentication methods and directly uses the provided access token to authenticate. This can be useful if you want to fully manage the authentication yourself.

??? warning "Token lifetime"

    This is not a recommended way to authenticate, as it requires you to manage the access token yourself. This is only meant for advanced use cases. In normal scenarios, the adapter manages the lifetime of the token for you and will automatically refresh it when needed. In this case, you will need to handle that yourself.

??? warning "Token scope"

    Microsoft accepts multiple token scopes for Fabric. However, if you are using the [`workspace_name`](#workspace_name) or [`workspace_id`](#workspace_id) options or if you are using Python models, the token must have the `https://analysis.windows.net/powerbi/api/.default` scope.

### `token_scope` :fontawesome-solid-lock:

Example values:

- `https://analysis.windows.net/powerbi/api/.default`
- `https://database.windows.net/.default`
- `pbi`
- `DW`

Depending on the [`authentication`](#authentication) method you are using, the adapter will request an access token for a specific scope. This scope will be automatically determined based on your configuration. However, if you need to override the scope for some reason, you can use this option to set a custom scope.

### `schema_auth`

Alias: `schema_authorization`<br>
Example value: `some_group_or_user`

If your dbt project is using a schema which does not exist yet, dbt will create it for you. Use this configuration option to set the owner of the schema after creation. This can be a user or a group.

### `lakehouse_name` :fontawesome-brands-python:

Alias: `lakehouse`<br>
Example value: `My Lakehouse`

The name of the Lakehouse in Fabric you wish to use for running Python models.

This is not used for SQL models.
This is not used if [`lakehouse_id`](#lakehouse_id) is also provided.

If you are using Python models in your project, either [`lakehouse_name`](#lakehouse_name) or [`lakehouse_id`](#lakehouse_id) must be provided.

When using this option together with [`authentication`](#authentication) set to `ActiveDirectoryServicePrincipal`, you also need to provide the [`tenant_id`](#tenant_id) option.

### `lakehouse_id` :fontawesome-brands-python:

Example value: `123e4567-e89b-12d3-a456-426614174000`

The ID of the Lakehouse in Fabric you wish to use for running Python models. This is not used for SQL models.

If you are using Python models in your project, either [`lakehouse_name`](#lakehouse_name) or [`lakehouse_id`](#lakehouse_id) must be provided.

When using this option together with [`authentication`](#authentication) set to `ActiveDirectoryServicePrincipal`, you also need to provide the [`tenant_id`](#tenant_id) option.

### `encrypt`

Possible values: `true`, `false`<br>
Default: `true`

Whether to use encryption for the connection. It's recommended to leave this enabled. This could be disabled for advanced networking scenarios.

### `trust_cert`

Alias: `TrustServerCertificate`<br>
Possible values: `true`, `false`<br>
Default: `false`

Whether to trust the server certificate without validation. It's recommended to leave this disabled. This could be enabled for advanced networking scenarios.

### `retries`

Possible values: any integer<br>
Default: `3`

The number of times to retry a failed connection before failing. This will not rerun a failed query, but will only be used for intermittent connection issues.

### `login_timeout`

Possible values: any integer (seconds) :timer:

The timeout for establishing a connection to the server. This can be useful if you are receiving the `Login timeout expired` error. A value of 30 seconds could improve the connection resiliency. The adapter has no default value and will use the ODBC driver's default if not set.

### `query_timeout`

Possible values: any integer (seconds) :timer:

The timeout for executing a query. This can be useful if you are receiving the `Query timeout expired` error. The default is no timeout.

### `trace_flag`

Alias: `SQL_ATTR_TRACE`<br>
Possible values: `true`, `false`

Set this to true to enable ODBC tracing. This is useful for debugging connection issues.
