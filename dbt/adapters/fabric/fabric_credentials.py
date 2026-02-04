from dataclasses import dataclass
from typing import Optional

from dbt.adapters.contracts.connection import Credentials


@dataclass
class FabricCredentials(Credentials):
    """
    Credentials for connecting to Microsoft Fabric Synapse Data Warehouse.

    Attributes
    ----------
    driver_backend : str
        Driver to use for database connections.
        - "auto" (default): Use mssql-python if available, fallback to pyodbc
        - "mssql-python": Force mssql-python (fails if unavailable)
        - "pyodbc": Force pyodbc (requires ODBC driver installed)
    driver : Optional[str]
        ODBC driver name. Only used when driver_backend is "pyodbc".
        Deprecated when using mssql-python backend.
    """

    host: str
    database: str
    schema: str
    driver_backend: str = "auto"
    driver: Optional[str] = None  # Only used for pyodbc backend
    UID: Optional[str] = None
    PWD: Optional[str] = None
    windows_login: Optional[bool] = False
    trace_flag: Optional[bool] = False
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    access_token: Optional[str] = None
    # Added for access token expiration for oAuth and integration tests scenarios.
    access_token_expires_on: Optional[int] = 0
    authentication: str = "ActiveDirectoryServicePrincipal"
    # default value in MS ODBC Driver 18 as well
    encrypt: Optional[bool] = True
    # default value in MS ODBC Driver 18 as well
    trust_cert: Optional[bool] = False
    retries: int = 3
    schema_authorization: Optional[str] = None
    login_timeout: Optional[int] = 0
    query_timeout: Optional[int] = 0
    workspace_id: Optional[str] = None
    warehouse_snapshot_name: Optional[str] = None
    warehouse_snapshot_id: Optional[str] = None
    snapshot_timestamp: Optional[str] = None
    api_url: Optional[str] = "https://api.fabric.microsoft.com/v1"

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
        "workspace_id": "workspace_id",
        "warehouse_snapshot_name": "warehouse_snapshot_name",
        "api_url": "api_url",
    }

    @property
    def type(self):
        return "fabric"

    def validate_snapshot_properties(self):
        workspace_provided = self.workspace_id is not None
        snapshot_name_provided = self.warehouse_snapshot_name is not None

        if workspace_provided != snapshot_name_provided:
            raise ValueError(
                "Both workspace_id and warehouse_snapshot_name must be provided together, "
                "or both must be None. Cannot have one without the other."
            )

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'
        # raise NotImplementedError
        if self.windows_login is True:
            self.authentication = "Windows Login"

        if self.authentication.lower().strip() == "serviceprincipal":
            self.authentication = "ActiveDirectoryServicePrincipal"

        self.validate_snapshot_properties()

        return (
            "server",
            "database",
            "schema",
            "warehouse_snapshot_name",
            "snapshot_timestamp",
            "UID",
            "workspace_id",
            "authentication",
            "retries",
            "login_timeout",
            "query_timeout",
            "trace_flag",
            "encrypt",
            "trust_cert",
            "api_url",
        )

    @property
    def unique_field(self):
        return self.host
