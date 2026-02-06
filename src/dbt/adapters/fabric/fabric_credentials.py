from dataclasses import dataclass

from dbt.adapters.contracts.connection import Credentials


@dataclass
class FabricCredentials(Credentials):
    driver: str
    host: str | None = None
    database: str
    schema: str
    UID: str | None = None
    PWD: str | None = None
    windows_login: bool | None = False
    trace_flag: bool | None = False
    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    access_token: str | None = None
    token_scope: str | None = None
    authentication: str | None = "auto"
    encrypt: bool | None = True  # default value in MS ODBC Driver 18 as well
    trust_cert: bool | None = False  # default value in MS ODBC Driver 18 as well
    retries: int = 3
    schema_authorization: str | None = None
    login_timeout: int | None = 0
    query_timeout: int | None = 0
    workspace_id: str | None = None
    workspace_name: str | None = None
    lakehouse_id: str | None = None
    lakehouse_name: str | None = None
    tenant_id: str | None = None
    fabric_base_api_uri: str = "https://api.fabric.microsoft.com/v1"
    powerbi_base_api_uri: str = "https://api.powerbi.com/v1.0"

    _ALIASES = {
        "user": "UID",
        "username": "UID",
        "pass": "PWD",
        "password": "PWD",
        "server": "host",
        "trusted_connection": "windows_login",
        "auth": "authentication",
        "app_id": "client_id",
        "app_secret": "client_secret",
        "TrustServerCertificate": "trust_cert",
        "schema_auth": "schema_authorization",
        "SQL_ATTR_TRACE": "trace_flag",
        "workspace": "workspace_name",
        "lakehouse": "lakehouse_name",
    }

    @property
    def type(self):
        return "fabric"

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'
        # raise NotImplementedError
        if self.windows_login is True:
            self.authentication = "Windows Login"

        if self.authentication.lower().strip() == "serviceprincipal":
            self.authentication = "ActiveDirectoryServicePrincipal"

        return (
            "host",
            "database",
            "schema",
            "UID",
            "client_id",
            "tenant_id",
            "authentication",
            "token_scope",
            "workspace_id",
            "workspace_name",
            "lakehouse_id",
            "lakehouse_name",
            "encrypt",
            "trust_cert",
            "retries",
            "login_timeout",
            "query_timeout",
            "trace_flag",
            "fabric_base_api_uri",
            "powerbi_base_api_uri",
        )

    @property
    def unique_field(self):
        return self.host or self.workspace_id or self.workspace_name
