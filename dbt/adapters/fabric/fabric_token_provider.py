import importlib
import re
import struct
import threading
import time
from collections.abc import Callable
from itertools import chain, repeat
from typing import Any

import requests
from azure.core.credentials import AccessToken, TokenCredential
from azure.identity import (
    AzureCliCredential,
    ClientAssertionCredential,
    ClientSecretCredential,
    DefaultAzureCredential,
    DeviceCodeCredential,
    EnvironmentCredential,
    InteractiveBrowserCredential,
    ManagedIdentityCredential,
)

from dbt.adapters.fabric.base_credentials import BaseFabricCredentials

DOTTED_PATH_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+$")

# A cached token is refreshed once less than this much of its validity is left.
REFRESH_THRESHOLD_SECONDS = 300


def get_notebookutils_access_token(scope: str) -> AccessToken:
    """Acquire an access token via Fabric notebookutils (for use inside Fabric notebooks).

    Args:
        scope: The OAuth scope to request a token for.
    """
    from notebookutils import credentials

    aad_token = credentials.getToken(scope)
    expires_on = int(time.time() + 4500.0)
    token = AccessToken(
        token=aad_token,
        expires_on=expires_on,
    )
    return token


def _build_federated_token_callable(
    credentials: BaseFabricCredentials,
) -> Callable[[], str]:
    if credentials.federated_token_url:
        url = credentials.federated_token_url
        headers = {}
        if credentials.federated_token_header:
            headers["Authorization"] = credentials.federated_token_header

        def fetch_from_url() -> str:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()["value"]

        return fetch_from_url

    if not credentials.federated_token_file:
        raise ValueError(
            "Either federated_token_url or federated_token_file must be configured "
            "for workload_identity authentication"
        )
    path = credentials.federated_token_file

    def read_from_file() -> str:
        with open(path) as f:
            return f.read().strip()

    return read_from_file


def load_token_credential(
    credential_class: str, credential_kwargs: dict[str, Any] | None
) -> TokenCredential:
    credential_kwargs = credential_kwargs or {}

    if not DOTTED_PATH_RE.match(credential_class):
        raise ValueError(
            f"credential_class must be a dotted import path "
            f"(e.g. 'my_pkg.auth.MyCredential'), got: {credential_class!r}"
        )

    module_path, class_name = credential_class.rsplit(".", 1)

    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        raise ValueError(
            f"Could not import module {module_path!r} from credential_class {credential_class!r}"
        ) from exc

    try:
        cls = getattr(module, class_name)
    except AttributeError as exc:
        raise ValueError(
            f"Module {module_path!r} has no attribute {class_name!r} "
            f"(from credential_class {credential_class!r})"
        ) from exc

    instance = cls(**credential_kwargs)

    if not isinstance(instance, TokenCredential):
        raise TypeError(
            f"{credential_class!r} is not a TokenCredential implementation. "
            f"The class must implement the azure.core.credentials.TokenCredential protocol "
            f"(i.e. have a get_token method)."
        )

    return instance


class FabricTokenProvider:
    SQL_CREDENTIAL_SCOPE = "https://database.windows.net/.default"
    FABRIC_CREDENTIAL_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
    SQL_COPT_SS_ACCESS_TOKEN = 1256

    def __init__(self, credentials: BaseFabricCredentials):
        self.credentials = credentials
        self._custom_credential: TokenCredential | None = None
        # Cached tokens and the locks guarding them belong to this provider, so
        # a provider can only ever hand out tokens for its own credentials.
        self._tokens: dict[str, AccessToken] = {}
        self._scope_locks: dict[str, threading.Lock] = {}
        self._scope_locks_guard = threading.Lock()
        self._custom_credential_lock = threading.Lock()

    def get_access_token(self, scope: str | None = None) -> str:
        """Return a valid access token for the given scope, refreshing if near expiry.

        Tokens are cached per scope on this provider and reused until they have
        less than 5 minutes of validity remaining. Concurrent callers of the same
        scope share a single acquisition; other scopes and other credential
        contexts keep running while that acquisition is in flight.

        Args:
            scope: The OAuth scope. Defaults to the Fabric API scope if not provided.

        Raises:
            ValueError: If the configured authentication method is not supported,
                or if required credentials (client_id, etc.) are missing.
        """
        if self.credentials.access_token:
            return self.credentials.access_token

        scope = scope or self.credentials.token_scope or self.FABRIC_CREDENTIAL_SCOPE

        cached = self._cached_token(scope)
        if cached is not None:
            return cached

        with self._scope_lock(scope):
            # Another thread may have refreshed this scope while we waited.
            cached = self._cached_token(scope)
            if cached is not None:
                return cached

            # A failure propagates instead of being cached as a token: the next
            # call retries rather than serving a stale or foreign token.
            token = self._acquire_token(scope)
            self._tokens[scope] = token
            return token.token

    def _cached_token(self, scope: str) -> str | None:
        """Return the cached token for a scope while it is still fresh enough."""
        token = self._tokens.get(scope)
        if token is not None and token.expires_on - time.time() >= REFRESH_THRESHOLD_SECONDS:
            return token.token
        return None

    def _scope_lock(self, scope: str) -> threading.Lock:
        """Return this provider's lock for a scope, creating it on first use."""
        with self._scope_locks_guard:
            return self._scope_locks.setdefault(scope, threading.Lock())

    def _acquire_token(self, scope: str) -> AccessToken:
        """Acquire a fresh token for a scope from the configured credential."""
        authentication = self.credentials.authentication.lower()

        if authentication == "notebookutils":
            return get_notebookutils_access_token(scope)

        return self._token_credential(authentication).get_token(scope)

    def _token_credential(self, authentication: str) -> TokenCredential:
        """Return the azure-identity credential for the configured auth method.

        Args:
            authentication: The lower-cased ``authentication`` credential value.

        Raises:
            ValueError: If the authentication method is not supported, or if
                required credentials (client_id, etc.) are missing.
        """
        if authentication == "activedirectoryserviceprincipal":
            if not all(
                [
                    self.credentials.client_id,
                    self.credentials.client_secret,
                    self.credentials.tenant_id,
                ]
            ):
                raise ValueError(
                    "client_id, client_secret, and tenant_id must be provided "
                    "for ActiveDirectoryServicePrincipal authentication."
                )
            return ClientSecretCredential(
                client_id=self.credentials.client_id,  # type: ignore
                client_secret=self.credentials.client_secret,  # type: ignore
                tenant_id=self.credentials.tenant_id,  # type: ignore
            )
        if authentication == "activedirectorydefault":
            return DefaultAzureCredential()
        if authentication == "activedirectoryinteractive":
            return InteractiveBrowserCredential()
        if authentication == "activedirectorydevicecodeflow":
            return DeviceCodeCredential()
        if authentication == "activedirectorymsi":
            return ManagedIdentityCredential()
        if authentication == "cli":
            return AzureCliCredential()
        if authentication == "environment":
            return EnvironmentCredential()
        if authentication == "workload_identity":
            return self._custom_token_credential(
                lambda: ClientAssertionCredential(
                    tenant_id=self.credentials.tenant_id,
                    client_id=self.credentials.client_id,
                    func=_build_federated_token_callable(self.credentials),
                )
            )
        if authentication == "token_credential":
            assert self.credentials.credential_class is not None
            credential_class = self.credentials.credential_class
            return self._custom_token_credential(
                lambda: load_token_credential(
                    credential_class,
                    self.credentials.credential_kwargs,
                )
            )

        raise ValueError(f"Unsupported authentication method: {self.credentials.authentication}")

    def _custom_token_credential(self, factory: Callable[[], TokenCredential]) -> TokenCredential:
        """Build this provider's custom credential once and reuse it afterwards.

        Args:
            factory: Builds the credential configured for these credentials.
        """
        with self._custom_credential_lock:
            if self._custom_credential is None:
                self._custom_credential = factory()
            return self._custom_credential

    def get_sql_attrs_before(self) -> dict[int, bytes] | None:
        """Build the SQL connection attrs_before dict with an encoded access token.

        Returns None when a driver-native ActiveDirectory authentication mode is
        used, since the mssql-python driver handles token acquisition internally
        in that case. "ActiveDirectoryAccessToken" is excluded from this check:
        it is not a real driver-native mode, but this adapter's own convention
        for supplying a pre-fetched token, which must always be attached via
        attrs_before.
        """
        if (
            self.credentials.authentication.lower() != "activedirectoryaccesstoken"
            and "activedirectory" in self.credentials.authentication.lower()
        ):
            return None

        token = self.get_access_token(scope=self.SQL_CREDENTIAL_SCOPE)
        token_byte_value = bytes(token, "UTF-8")
        encoded_bytes = bytes(chain.from_iterable(zip(token_byte_value, repeat(0))))
        token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes
        return {self.SQL_COPT_SS_ACCESS_TOKEN: token_bytes}
