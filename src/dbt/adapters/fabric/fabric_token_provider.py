import struct
import time
from itertools import chain, repeat
from typing import Any

from azure.core.credentials import AccessToken
from azure.identity import (
    AzureCliCredential,
    ClientSecretCredential,
    DefaultAzureCredential,
    DeviceCodeCredential,
    EnvironmentCredential,
    InteractiveBrowserCredential,
    ManagedIdentityCredential,
)

from dbt.adapters.fabric.base_credentials import BaseFabricCredentials


def get_notebookutils_access_token(scope: str) -> AccessToken:
    from notebookutils import credentials

    aad_token = credentials.getToken(scope)
    expires_on = int(time.time() + 4500.0)
    token = AccessToken(
        token=aad_token,
        expires_on=expires_on,
    )
    return token


class FabricTokenProvider:
    SQL_CREDENTIAL_SCOPE = "https://database.windows.net/.default"
    FABRIC_CREDENTIAL_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
    FABRIC_SPARK_CREDENTIAL_SCOPE = "pbi"
    _tokens: dict[str, AccessToken] = {}
    SQL_COPT_SS_ACCESS_TOKEN = 1256

    def __init__(self, credentials: BaseFabricCredentials):
        self.credentials = credentials

    def get_access_token(self, scope: str | None = None) -> str:
        MAX_REMAINING_TIME = 300

        if self.credentials.access_token:
            return self.credentials.access_token

        scope = scope or self.credentials.token_scope or self.FABRIC_CREDENTIAL_SCOPE

        current_token = self._tokens.get(scope)
        time_remaining = (
            (current_token.expires_on - time.time()) if current_token else MAX_REMAINING_TIME
        )

        if current_token and time_remaining >= MAX_REMAINING_TIME:
            return current_token.token

        credential: Any | None = None
        token: AccessToken

        if self.credentials.authentication.lower() == "activedirectoryserviceprincipal":
            if not all(
                [
                    self.credentials.client_id,
                    self.credentials.client_secret,
                    self.credentials.tenant_id,
                ]
            ):
                raise ValueError(
                    "client_id, client_secret, and tenant_id must be provided for ActiveDirectoryServicePrincipal authentication."
                )
            credential = ClientSecretCredential(
                client_id=self.credentials.client_id,  # type: ignore
                client_secret=self.credentials.client_secret,  # type: ignore
                tenant_id=self.credentials.tenant_id,  # type: ignore
            )
        elif self.credentials.authentication.lower() == "activedirectorydefault":
            credential = DefaultAzureCredential()
        elif self.credentials.authentication.lower() == "activedirectoryinteractive":
            credential = InteractiveBrowserCredential()
        elif self.credentials.authentication.lower() == "activedirectorydevicecodeflow":
            credential = DeviceCodeCredential()
        elif self.credentials.authentication.lower() == "activedirectorymsi":
            credential = ManagedIdentityCredential()
        elif self.credentials.authentication.lower() == "cli":
            credential = AzureCliCredential()
        elif self.credentials.authentication.lower() == "environment":
            credential = EnvironmentCredential()
        elif self.credentials.authentication.lower() == "notebookutils":
            token = get_notebookutils_access_token(scope)
        else:
            raise ValueError(
                f"Unsupported authentication method: {self.credentials.authentication}"
            )

        if credential is not None:
            token = credential.get_token(scope)

        self._tokens[scope] = token
        return token.token

    def get_sql_attrs_before(self) -> dict[int, bytes] | None:
        if "ActiveDirectory" in self.credentials.authentication:
            return None

        token = self.get_access_token(scope=self.SQL_CREDENTIAL_SCOPE)
        token_byte_value = bytes(token, "UTF-8")
        encoded_bytes = bytes(chain.from_iterable(zip(token_byte_value, repeat(0))))
        token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes
        return {self.SQL_COPT_SS_ACCESS_TOKEN: token_bytes}
