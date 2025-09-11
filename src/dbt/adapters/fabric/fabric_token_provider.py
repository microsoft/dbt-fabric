import re
import struct
import time
from itertools import chain, repeat
from typing import Dict, Optional

from azure.core.credentials import AccessToken
from azure.identity import (
    AzureCliCredential,
    ClientSecretCredential,
    DefaultAzureCredential,
    EnvironmentCredential,
)

from dbt.adapters.fabric.fabric_credentials import FabricCredentials


def get_mssparkutils_access_token(scope: str) -> AccessToken:
    from notebookutils import credentials

    aad_token = credentials.getToken(scope)
    expires_on = int(time.time() + 4500.0)
    token = AccessToken(
        token=aad_token,
        expires_on=expires_on,
    )
    return token


def get_cli_access_token(scope: str) -> AccessToken:
    token = AzureCliCredential().get_token(scope)
    return token


def get_auto_access_token(scope: str) -> AccessToken:
    token = DefaultAzureCredential().get_token(scope)
    return token


def get_environment_access_token(scope: str) -> AccessToken:
    token = EnvironmentCredential().get_token(scope)
    return token


azure_auth_functions = {
    "cli": get_cli_access_token,
    "auto": get_auto_access_token,
    "environment": get_environment_access_token,
    "synapsespark": get_mssparkutils_access_token,
    "fabricspark": get_mssparkutils_access_token,
}


class FabricTokenProvider:
    AZURE_CREDENTIAL_SCOPE = "https://database.windows.net/.default"
    FABRIC_CREDENTIAL_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
    SYNAPSE_SPARK_CREDENTIAL_SCOPE = "DW"
    FABRIC_SPARK_CREDENTIAL_SCOPE = "pbi"
    _tokens: dict[str, AccessToken] = {}
    SQL_COPT_SS_ACCESS_TOKEN = 1256

    def __init__(self, credentials: FabricCredentials):
        self.credentials = credentials

    def get_token_scope(self) -> str:
        if self.credentials.token_scope:
            return self.credentials.token_scope

        if not self.credentials.host and (
            self.credentials.workspace_id or self.credentials.workspace_name
        ):
            return self.FABRIC_CREDENTIAL_SCOPE
        if "synapse" in self.credentials.authentication.lower():
            return self.SYNAPSE_SPARK_CREDENTIAL_SCOPE
        if "fabric" in self.credentials.authentication.lower():
            return self.FABRIC_SPARK_CREDENTIAL_SCOPE
        if "azuresynapse.net" in self.credentials.host.lower():
            return self.SYNAPSE_SPARK_CREDENTIAL_SCOPE
        if "fabric.microsoft.com" in self.credentials.host.lower():
            return self.FABRIC_CREDENTIAL_SCOPE
        if "database.windows.net" in self.credentials.host.lower():
            return self.AZURE_CREDENTIAL_SCOPE
        return self.FABRIC_CREDENTIAL_SCOPE

    def get_api_token(self) -> str | None:
        return self._get_token(usage_is_sql=False)

    def get_sql_token(self, scope: Optional[str] = None) -> str | None:
        return self._get_token(scope=scope, usage_is_sql=True)

    def _get_token(self, scope: Optional[str] = None, usage_is_sql: bool = False) -> str | None:
        MAX_REMAINING_TIME = 300

        if self.credentials.access_token:
            return self.credentials.access_token

        _scope = self.FABRIC_CREDENTIAL_SCOPE if not usage_is_sql else None
        scope = scope or self.credentials.token_scope or _scope or self.get_token_scope()
        authentication = str(self.credentials.authentication).lower()

        current_token = self._tokens.get(scope)
        time_remaining = (
            (current_token.expires_on - time.time()) if current_token else MAX_REMAINING_TIME
        )

        if usage_is_sql and authentication not in azure_auth_functions:
            return None
        if current_token and time_remaining >= MAX_REMAINING_TIME:
            return current_token.token
        if authentication in azure_auth_functions:
            azure_auth_function = azure_auth_functions[authentication]
            token = azure_auth_function(scope)
            self._tokens[scope] = token
            return token.token
        if authentication == "activedirectoryserviceprincipal" and not usage_is_sql:
            client_id = self.credentials.client_id
            client_secret = self.credentials.client_secret
            tenant_id = self.credentials.tenant_id
            cred = ClientSecretCredential(
                client_id=client_id,
                client_secret=client_secret,
                tenant_id=tenant_id,
            )
            token = cred.get_token(scope)
            self._tokens[scope] = token
            return token.token

        return None

    @staticmethod
    def convert_bytes_to_mswindows_byte_string(value: bytes) -> bytes:
        encoded_bytes = bytes(chain.from_iterable(zip(value, repeat(0))))
        return struct.pack("<i", len(encoded_bytes)) + encoded_bytes

    @staticmethod
    def convert_access_token_to_mswindows_byte_string(token: str) -> bytes:
        value = bytes(token, "UTF-8")
        return FabricTokenProvider.convert_bytes_to_mswindows_byte_string(value)

    def get_pyodbc_attributes(self) -> dict[int, bytes]:
        attrs_before: Dict
        token = self.get_sql_token()
        if token:
            token_bytes = self.convert_access_token_to_mswindows_byte_string(token)
            attrs_before = {self.SQL_COPT_SS_ACCESS_TOKEN: token_bytes}
        else:
            attrs_before = {}
        return attrs_before
