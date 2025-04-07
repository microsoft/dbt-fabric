from dataclasses import dataclass
from typing import Optional

from dbt.adapters.contracts.connection import Credentials


@dataclass
class FabricCredentials(Credentials):
    driver: str
    host: str
    database: str
    schema: str
    UID: Optional[str] = None
    PWD: Optional[str] = None
    windows_login: Optional[bool] = False
    trace_flag: Optional[bool] = False
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    access_token: Optional[str] = None
    token_scope: Optional[str] = None
    authentication: Optional[str] = "auto"
    encrypt: Optional[bool] = True  # default value in MS ODBC Driver 18 as well
    trust_cert: Optional[bool] = False  # default value in MS ODBC Driver 18 as well
    retries: int = 3
    schema_authorization: Optional[str] = None
    login_timeout: Optional[int] = 0
    query_timeout: Optional[int] = 0
    livy_session_connection_string: Optional[str] = None

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
            "server",
            "database",
            "schema",
            "UID",
            "client_id",
            "authentication",
            "token_scope",
            "encrypt",
            "trust_cert",
            "retries",
            "login_timeout",
            "query_timeout",
            "trace_flag",
            "livy_session_connection_string",
        )

    @property
    def unique_field(self):
        return self.host
