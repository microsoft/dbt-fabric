import abc
from dataclasses import dataclass
from typing import Any

from dbt.adapters.contracts.connection import Credentials


@dataclass
class BaseFabricCredentials(Credentials, metaclass=abc.ABCMeta):
    database: str
    schema: str
    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    access_token: str | None = None
    token_scope: str | None = None
    authentication: str = "auto"
    retries: int = 3
    query_timeout: int = 24 * 60 * 60  # 24 hours in seconds
    spark_session_timeout: int = 15 * 60  # 15 minutes in seconds
    workspace_id: str | None = None
    workspace_name: str | None = None
    fabric_base_api_uri: str = "https://api.fabric.microsoft.com/v1"
    powerbi_base_api_uri: str = "https://api.powerbi.com/v1.0"
    livy_session_name: str = "dbt-fabric-samdebruyn"

    _ALIASES = {
        "trusted_connection": "windows_login",
        "auth": "authentication",
        "app_id": "client_id",
        "app_secret": "client_secret",
        "workspace": "workspace_name",
    }

    def __post_serialize__(self, dct: dict, context: dict | None = None) -> dict[Any, Any]:
        des = super().__post_serialize__(dct, context)

        if des.get("authentication", "").lower().strip() == "serviceprincipal":
            des["authentication"] = "ActiveDirectoryServicePrincipal"

        return des

    @property
    @abc.abstractmethod
    def lakehouse_name(self) -> str | None:
        """The name of the Lakehouse to use for Python models."""
        ...

    def _connection_keys(self) -> tuple[str, ...]:
        return (
            "database",
            "schema",
            "tenant_id",
            "client_id",
            "token_scope",
            "authentication",
            "retries",
            "workspace_id",
            "workspace_name",
            "query_timeout",
            "fabric_base_api_uri",
            "powerbi_base_api_uri",
            "livy_session_name",
        )

    @property
    def unique_field(self) -> str:
        ws_unique = self.workspace_id or self.workspace_name
        assert ws_unique is not None, "Either workspace_id or workspace_name must be provided"
        return ws_unique
