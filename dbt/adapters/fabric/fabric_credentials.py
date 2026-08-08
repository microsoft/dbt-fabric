from dataclasses import dataclass
from typing import Any

from dbt.adapters.fabric.base_credentials import BaseFabricCredentials


@dataclass
class FabricCredentials(BaseFabricCredentials):
    host: str | None = None
    UID: str | None = None
    PWD: str | None = None
    windows_login: bool | None = False
    trace_flag: bool | None = False
    encrypt: bool | None = True
    trust_cert: bool | None = False
    schema_authorization: str | None = None
    login_timeout: int = 0
    lock_timeout: int = 30000

    _ALIASES = BaseFabricCredentials._ALIASES | {
        "user": "UID",
        "username": "UID",
        "pass": "PWD",
        "password": "PWD",
        "server": "host",
        "trusted_connection": "windows_login",
        "TrustServerCertificate": "trust_cert",
        "schema_auth": "schema_authorization",
        "SQL_ATTR_TRACE": "trace_flag",
    }

    @property
    def type(self):
        return "fabric"

    def __post_serialize__(self, dct: dict, context: dict | None = None) -> dict[Any, Any]:
        des = super().__post_serialize__(dct, context)

        if des.get("authentication", "").lower() == "auto":
            des["authentication"] = "ActiveDirectoryDefault"

        if des.get("windows_login", False) is True:
            des["authentication"] = "Windows Login"

        return des

    def _connection_keys(self) -> tuple[str, ...]:
        return super()._connection_keys() + (
            "UID",
            "windows_login",
            "trace_flag",
            "encrypt",
            "trust_cert",
            "schema_authorization",
            "login_timeout",
            "lock_timeout",
        )

    @property
    def unique_field(self):
        return self.host or super().unique_field
