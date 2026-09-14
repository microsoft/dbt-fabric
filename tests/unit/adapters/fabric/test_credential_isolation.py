"""Regression tests for credential-context isolation and token-cache concurrency.

Two credential contexts living in the same process must never share tokens,
token providers, Fabric API state or Purview clients. Every credential in this
module is synthetic and every token acquisition is mocked: these tests never
talk to Azure, Fabric or Purview.
"""

import copy
import gc
import json
import pickle
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from types import SimpleNamespace
from unittest import mock
from urllib.parse import unquote_plus

import dbt_common.exceptions
import pytest
from azure.core.credentials import AccessToken, TokenCredential

from dbt.adapters.fabric import fabric_token_provider as token_provider_module
from dbt.adapters.fabric.base_connection_manager import BaseFabricConnectionManager
from dbt.adapters.fabric.fabric_api_client import FabricApiClient, FabricApiError
from dbt.adapters.fabric.fabric_connection_manager import FabricConnectionManager
from dbt.adapters.fabric.fabric_credentials import FabricCredentials
from dbt.adapters.fabric.fabric_token_provider import FabricTokenProvider
from dbt.adapters.fabric.purview_client import _PURVIEW_SCOPE

# Upper bound for every blocking wait in this module: exceeded only when the
# code under test deadlocks, never during a healthy run.
WAIT_TIMEOUT = 10.0

# How long a racing implementation is given to pile into a token acquisition
# that is already in flight. Correctness is asserted on the recorded call
# count, not on this window; it only bounds the wait for the fixed
# implementation, where the other threads never reach the acquisition at all.
RACE_WINDOW = 0.5

FABRIC_SCOPE = FabricTokenProvider.FABRIC_CREDENTIAL_SCOPE
SQL_SCOPE = FabricTokenProvider.SQL_CREDENTIAL_SCOPE


def make_credentials(name: str, **overrides) -> FabricCredentials:
    """Build synthetic service principal credentials for context ``name``."""
    kwargs: dict = {
        "database": f"warehouse-{name}",
        "schema": "dbo",
        "authentication": "ActiveDirectoryServicePrincipal",
        "tenant_id": f"tenant-{name}",
        "client_id": f"client-{name}",
        "client_secret": f"synthetic-secret-{name}",
        "workspace_id": f"workspace-{name}",
        "purview_endpoint": f"https://purview-{name}.example.invalid",
    }
    kwargs.update(overrides)
    return FabricCredentials(**kwargs)


class StubAzureCredential:
    """Deterministic stand-in for an azure-identity credential.

    Returns a token that encodes both the client it was created for and the
    requested scope, so tests can assert which identity a caller ended up with.
    """

    def __init__(
        self,
        client_id: str,
        *,
        expires_in: int = 3600,
        failures: int = 0,
        gate: threading.Event | None = None,
        started: threading.Event | None = None,
        clock=time.time,
    ) -> None:
        self.client_id = client_id
        self.expires_in = expires_in
        self.failures = failures
        self.gate = gate
        self.started = started
        self.clock = clock
        self.scopes: list[str] = []
        self.all_entered = threading.Event()
        self.expected_entries = 0
        self._lock = threading.Lock()

    @property
    def call_count(self) -> int:
        return len(self.scopes)

    def get_token(self, *scopes, **kwargs) -> AccessToken:
        with self._lock:
            self.scopes.append(scopes[0])
            attempt = len(self.scopes)
            if self.expected_entries and attempt >= self.expected_entries:
                self.all_entered.set()
        if self.started is not None:
            self.started.set()
        if self.gate is not None and not self.gate.wait(WAIT_TIMEOUT):
            raise AssertionError(f"gate for {self.client_id} was never released")
        if attempt <= self.failures:
            raise RuntimeError(f"token acquisition failed for {self.client_id}")
        return AccessToken(
            token=f"token|{self.client_id}|{scopes[0]}",
            expires_on=int(self.clock()) + self.expires_in,
        )


class StubCredentialFactory:
    """Records every azure-identity credential construction and returns stubs.

    One stub per client id: the adapter builds a new azure-identity credential
    for every acquisition, but they all stand for the same identity.
    """

    def __init__(self, **per_client_kwargs) -> None:
        self.per_client_kwargs = per_client_kwargs
        self.constructed: list[dict] = []
        self.stubs: dict[str, StubAzureCredential] = {}

    def __call__(self, **kwargs) -> StubAzureCredential:
        self.constructed.append(kwargs)
        client_id = kwargs["client_id"]
        if client_id not in self.stubs:
            self.stubs[client_id] = StubAzureCredential(
                client_id, **self.per_client_kwargs.get(client_id, {})
            )
        return self.stubs[client_id]


class StubTokenCredential(TokenCredential):
    """Minimal TokenCredential loaded through ``credential_class``.

    The token carries both the configured marker and the instance identity, so
    tests can tell contexts apart and see whether an instance was reused.
    """

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    def get_token(self, *scopes, **kwargs) -> AccessToken:
        marker = self.kwargs.get("marker", "unset")
        return AccessToken(
            token=f"custom|{marker}|{id(self)}|{scopes[0]}", expires_on=int(time.time()) + 3600
        )


CREDENTIAL_CLASS_PATH = "tests.unit.adapters.fabric.test_credential_isolation.StubTokenCredential"


@contextmanager
def patched_client_secret_credential(**per_client_kwargs):
    factory = StubCredentialFactory(**per_client_kwargs)
    with mock.patch.object(token_provider_module, "ClientSecretCredential", side_effect=factory):
        yield factory


@contextmanager
def frozen_clock(now: float):
    """Freeze the clock the token provider uses, without touching other modules."""
    with mock.patch.object(token_provider_module, "time", SimpleNamespace(time=lambda: now)):
        yield


def _response(status_code=200, json_data=None, headers=None):
    response = mock.MagicMock()
    response.status_code = status_code
    response.json.return_value = json_data if json_data is not None else {}
    response.headers = headers or {}
    response.text = ""
    return response


def _workspace_from_url(url: str) -> str:
    return url.split("/workspaces/")[1].split("/")[0]


def _fabric_api_side_effect(method, url, json=None, headers=None):
    """Answer warehouse lookups with data derived from the requested workspace."""
    workspace = _workspace_from_url(url)
    return _response(
        200,
        {
            "value": [
                {
                    "id": f"warehouse-id-{workspace}",
                    "displayName": f"warehouse-{workspace.removeprefix('workspace-')}",
                    "properties": {
                        "connectionString": f"{workspace}.datawarehouse.example.invalid"
                    },
                }
            ]
        },
    )


def _lookup_providers_concurrently(credentials, threads: int = 4) -> list[FabricTokenProvider]:
    """Look the token provider of one credentials object up from several threads."""
    start = threading.Barrier(threads, timeout=WAIT_TIMEOUT)

    def lookup() -> FabricTokenProvider:
        start.wait()
        return BaseFabricConnectionManager.get_fabric_token_provider(credentials)

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(lookup) for _ in range(threads)]
        return [future.result(timeout=WAIT_TIMEOUT) for future in futures]


def _authorization(call) -> str:
    return call.kwargs["headers"]["Authorization"]


def _requested_url(call) -> str:
    return call.args[1]


class TestTokenProviderIsolation:
    def test_second_context_gets_its_own_token(self):
        provider_a = FabricTokenProvider(make_credentials("a"))
        provider_b = FabricTokenProvider(make_credentials("b"))

        with patched_client_secret_credential() as factory:
            token_a = provider_a.get_access_token()
            token_b = provider_b.get_access_token()

        assert token_a == f"token|client-a|{FABRIC_SCOPE}"
        assert token_b == f"token|client-b|{FABRIC_SCOPE}"
        assert [c["tenant_id"] for c in factory.constructed] == ["tenant-a", "tenant-b"]
        assert [c["client_id"] for c in factory.constructed] == ["client-a", "client-b"]

    def test_token_is_reused_within_a_context(self):
        provider = FabricTokenProvider(make_credentials("a"))

        with patched_client_secret_credential() as factory:
            first = provider.get_access_token()
            second = provider.get_access_token()

        assert first == second
        assert len(factory.constructed) == 1
        assert factory.stubs["client-a"].call_count == 1

    def test_scopes_are_cached_independently(self):
        provider = FabricTokenProvider(make_credentials("a"))

        with patched_client_secret_credential() as factory:
            fabric_token = provider.get_access_token()
            sql_token = provider.get_access_token(SQL_SCOPE)
            provider.get_access_token()
            provider.get_access_token(SQL_SCOPE)

        assert fabric_token == f"token|client-a|{FABRIC_SCOPE}"
        assert sql_token == f"token|client-a|{SQL_SCOPE}"
        assert factory.stubs["client-a"].scopes == [FABRIC_SCOPE, SQL_SCOPE]

    def test_configured_token_scope_is_used_per_context(self):
        provider_a = FabricTokenProvider(
            make_credentials("a", token_scope="https://scope-a.example.invalid/.default")
        )
        provider_b = FabricTokenProvider(
            make_credentials("b", token_scope="https://scope-b.example.invalid/.default")
        )

        with patched_client_secret_credential() as factory:
            token_a = provider_a.get_access_token()
            token_b = provider_b.get_access_token()

        assert token_a == "token|client-a|https://scope-a.example.invalid/.default"
        assert token_b == "token|client-b|https://scope-b.example.invalid/.default"
        assert factory.stubs["client-a"].scopes == ["https://scope-a.example.invalid/.default"]
        assert factory.stubs["client-b"].scopes == ["https://scope-b.example.invalid/.default"]

    def test_static_access_tokens_are_not_shared(self):
        creds_a = make_credentials(
            "a", authentication="ActiveDirectoryAccessToken", access_token="static-token-a"
        )
        creds_b = make_credentials(
            "b", authentication="ActiveDirectoryAccessToken", access_token="static-token-b"
        )

        provider_a = FabricTokenProvider(creds_a)
        provider_b = FabricTokenProvider(creds_b)

        assert provider_a.get_access_token() == "static-token-a"
        assert provider_b.get_access_token() == "static-token-b"

        attrs_b = provider_b.get_sql_attrs_before()
        assert attrs_b is not None
        assert b"static-token-b" in attrs_b[FabricTokenProvider.SQL_COPT_SS_ACCESS_TOKEN].replace(
            b"\x00", b""
        )

    def test_static_access_token_context_does_not_poison_other_context(self):
        static_provider = FabricTokenProvider(
            make_credentials(
                "a", authentication="ActiveDirectoryAccessToken", access_token="static-token-a"
            )
        )
        provider_b = FabricTokenProvider(make_credentials("b"))

        static_provider.get_access_token()
        with patched_client_secret_credential():
            assert provider_b.get_access_token() == f"token|client-b|{FABRIC_SCOPE}"

    def test_token_at_refresh_threshold_is_reused(self):
        now = 1_000_000.0
        provider = FabricTokenProvider(make_credentials("a"))

        with (
            frozen_clock(now),
            patched_client_secret_credential(
                **{"client-a": {"expires_in": 300, "clock": lambda: now}}
            ) as factory,
        ):
            provider.get_access_token()
            provider.get_access_token()

        assert factory.stubs["client-a"].call_count == 1

    def test_token_below_refresh_threshold_is_refreshed(self):
        now = 1_000_000.0
        provider = FabricTokenProvider(make_credentials("a"))

        with (
            frozen_clock(now),
            patched_client_secret_credential(
                **{"client-a": {"expires_in": 299, "clock": lambda: now}}
            ) as factory,
        ):
            provider.get_access_token()
            provider.get_access_token()

        assert factory.stubs["client-a"].call_count == 2

    def test_failed_acquisition_propagates_and_a_retry_recovers(self):
        provider = FabricTokenProvider(make_credentials("a"))

        with patched_client_secret_credential(**{"client-a": {"failures": 1}}) as factory:
            with pytest.raises(RuntimeError, match="token acquisition failed for client-a"):
                provider.get_access_token()

            assert provider.get_access_token() == f"token|client-a|{FABRIC_SCOPE}"

        assert factory.stubs["client-a"].call_count == 2

    def test_failed_acquisition_never_returns_another_contexts_token(self):
        provider_a = FabricTokenProvider(make_credentials("a"))
        provider_b = FabricTokenProvider(make_credentials("b"))

        with patched_client_secret_credential(**{"client-b": {"failures": 1}}):
            assert provider_a.get_access_token() == f"token|client-a|{FABRIC_SCOPE}"

            with pytest.raises(RuntimeError, match="token acquisition failed for client-b"):
                provider_b.get_access_token()

            assert provider_b.get_access_token() == f"token|client-b|{FABRIC_SCOPE}"

    def test_failed_refresh_does_not_cache_a_failure(self):
        now = 1_000_000.0
        provider = FabricTokenProvider(make_credentials("a"))
        stub = StubAzureCredential("client-a", expires_in=299, clock=lambda: now)

        with (
            frozen_clock(now),
            mock.patch.object(token_provider_module, "ClientSecretCredential", return_value=stub),
        ):
            first = provider.get_access_token()

            stub.failures = stub.call_count + 1
            with pytest.raises(RuntimeError):
                provider.get_access_token()

            stub.failures = 0
            stub.expires_in = 3600
            recovered = provider.get_access_token()

        assert first == f"token|client-a|{FABRIC_SCOPE}"
        assert recovered == first
        assert stub.call_count == 3

    def test_workload_identity_credentials_are_per_context(self):
        creds_a = make_credentials(
            "a",
            authentication="workload_identity",
            client_secret=None,
            federated_token_url="https://oidc-a.example.invalid/token",
        )
        creds_b = make_credentials(
            "b",
            authentication="workload_identity",
            client_secret=None,
            federated_token_url="https://oidc-b.example.invalid/token",
        )
        factory = StubCredentialFactory()

        with mock.patch.object(
            token_provider_module, "ClientAssertionCredential", side_effect=factory
        ):
            token_a = FabricTokenProvider(creds_a).get_access_token()
            provider_b = FabricTokenProvider(creds_b)
            token_b = provider_b.get_access_token()
            provider_b.get_access_token(SQL_SCOPE)

        assert token_a == f"token|client-a|{FABRIC_SCOPE}"
        assert token_b == f"token|client-b|{FABRIC_SCOPE}"
        assert [c["tenant_id"] for c in factory.constructed] == ["tenant-a", "tenant-b"]

    def test_custom_token_credentials_are_per_context(self):
        provider_a = FabricTokenProvider(self._custom_credentials("a"))
        provider_b = FabricTokenProvider(self._custom_credentials("b"))

        token_a = provider_a.get_access_token()
        token_b = provider_b.get_access_token()

        marker_a, instance_a = token_a.split("|")[1:3]
        marker_b, instance_b = token_b.split("|")[1:3]
        assert (marker_a, marker_b) == ("a", "b")
        assert instance_a != instance_b

    def test_custom_credential_instance_is_reused_within_a_context(self):
        provider = FabricTokenProvider(self._custom_credentials("a"))

        fabric_token = provider.get_access_token()
        sql_token = provider.get_access_token(SQL_SCOPE)

        assert fabric_token.endswith(FABRIC_SCOPE)
        assert sql_token.endswith(SQL_SCOPE)
        assert fabric_token.split("|")[2] == sql_token.split("|")[2]

    @staticmethod
    def _custom_credentials(name: str) -> FabricCredentials:
        return make_credentials(
            name,
            authentication="token_credential",
            client_secret=None,
            credential_class=CREDENTIAL_CLASS_PATH,
            credential_kwargs={"marker": name},
        )


class TestTokenCacheConcurrency:
    def _run_concurrently(self, calls):
        with ThreadPoolExecutor(max_workers=len(calls)) as pool:
            start = threading.Barrier(len(calls), timeout=WAIT_TIMEOUT)

            def run(call):
                start.wait()
                return call()

            futures = [pool.submit(run, call) for call in calls]
            return [future.result(timeout=WAIT_TIMEOUT) for future in futures]

    def test_concurrent_first_use_acquires_a_single_token(self):
        threads = 4
        gate = threading.Event()
        started = threading.Event()
        stub = StubAzureCredential("client-a", gate=gate, started=started)
        stub.expected_entries = threads
        provider = FabricTokenProvider(make_credentials("a"))

        with mock.patch.object(token_provider_module, "ClientSecretCredential", return_value=stub):
            releaser = threading.Thread(target=self._release_when_settled, args=(stub, gate))
            releaser.start()
            try:
                tokens = self._run_concurrently([provider.get_access_token] * threads)
            finally:
                gate.set()
                releaser.join(timeout=WAIT_TIMEOUT)

        assert started.is_set()
        assert tokens == [f"token|client-a|{FABRIC_SCOPE}"] * threads
        assert stub.call_count == 1

    def test_concurrent_refresh_acquires_a_single_token(self):
        threads = 4
        now = 1_000_000.0
        gate = threading.Event()
        expiring = StubAzureCredential("client-a", expires_in=299, clock=lambda: now)
        provider = FabricTokenProvider(make_credentials("a"))

        with frozen_clock(now):
            with mock.patch.object(
                token_provider_module, "ClientSecretCredential", return_value=expiring
            ):
                provider.get_access_token()

            refreshing = StubAzureCredential("client-a", gate=gate, clock=lambda: now)
            refreshing.expected_entries = threads
            with mock.patch.object(
                token_provider_module, "ClientSecretCredential", return_value=refreshing
            ):
                releaser = threading.Thread(
                    target=self._release_when_settled, args=(refreshing, gate)
                )
                releaser.start()
                try:
                    tokens = self._run_concurrently([provider.get_access_token] * threads)
                finally:
                    gate.set()
                    releaser.join(timeout=WAIT_TIMEOUT)

        assert tokens == [f"token|client-a|{FABRIC_SCOPE}"] * threads
        assert refreshing.call_count == 1

    def test_acquisition_does_not_block_another_context(self):
        gate = threading.Event()
        started = threading.Event()
        blocked = StubAzureCredential("client-a", gate=gate, started=started)
        free = StubAzureCredential("client-b")
        provider_a = FabricTokenProvider(make_credentials("a"))
        provider_b = FabricTokenProvider(make_credentials("b"))

        def factory(**kwargs):
            return blocked if kwargs["client_id"] == "client-a" else free

        with mock.patch.object(
            token_provider_module, "ClientSecretCredential", side_effect=factory
        ):
            with ThreadPoolExecutor(max_workers=2) as pool:
                slow = pool.submit(provider_a.get_access_token)
                assert started.wait(WAIT_TIMEOUT), "context A never started acquiring"

                fast = pool.submit(provider_b.get_access_token)
                assert fast.result(timeout=WAIT_TIMEOUT) == f"token|client-b|{FABRIC_SCOPE}"

                gate.set()
                assert slow.result(timeout=WAIT_TIMEOUT) == f"token|client-a|{FABRIC_SCOPE}"

    def test_acquisition_does_not_block_another_scope(self):
        gate = threading.Event()
        started = threading.Event()
        blocked = StubAzureCredential("client-a", gate=gate, started=started)
        free = StubAzureCredential("client-a")
        provider = FabricTokenProvider(make_credentials("a"))

        with mock.patch.object(
            token_provider_module, "ClientSecretCredential", side_effect=[blocked, free]
        ):
            with ThreadPoolExecutor(max_workers=2) as pool:
                slow = pool.submit(provider.get_access_token, FABRIC_SCOPE)
                assert started.wait(WAIT_TIMEOUT), "the first scope never started acquiring"

                fast = pool.submit(provider.get_access_token, SQL_SCOPE)
                assert fast.result(timeout=WAIT_TIMEOUT) == f"token|client-a|{SQL_SCOPE}"

                gate.set()
                assert slow.result(timeout=WAIT_TIMEOUT) == f"token|client-a|{FABRIC_SCOPE}"

    @staticmethod
    def _release_when_settled(stub: StubAzureCredential, gate: threading.Event) -> None:
        """Release the in-flight acquisition once every racing thread arrived.

        A synchronized implementation lets only one thread reach the credential,
        so the wait falls through after RACE_WINDOW; an unsynchronized one sets
        the event as soon as all threads piled in.
        """
        stub.all_entered.wait(RACE_WINDOW)
        gate.set()


class TestConnectionManagerIsolation:
    def test_token_provider_is_per_credential_context(self):
        creds_a = make_credentials("a")
        creds_b = make_credentials("b")

        provider_a = BaseFabricConnectionManager.get_fabric_token_provider(creds_a)
        provider_b = BaseFabricConnectionManager.get_fabric_token_provider(creds_b)

        assert provider_a is not provider_b
        assert provider_a.credentials is creds_a
        assert provider_b.credentials is creds_b

    def test_token_provider_is_reused_within_a_context(self):
        creds = make_credentials("a")

        first = BaseFabricConnectionManager.get_fabric_token_provider(creds)
        second = BaseFabricConnectionManager.get_fabric_token_provider(creds)

        assert first is second

    def test_concurrent_first_use_shares_one_token_provider(self):
        creds = make_credentials("a")

        providers = _lookup_providers_concurrently(creds)

        assert len({id(provider) for provider in providers}) == 1

    def test_concurrent_first_use_of_a_copy_shares_one_token_provider(self):
        original = make_credentials("a")
        BaseFabricConnectionManager.get_fabric_token_provider(original)
        clone = copy.copy(original)

        providers = _lookup_providers_concurrently(clone)

        assert len({id(provider) for provider in providers}) == 1
        assert providers[0].credentials is clone

    def test_api_clients_use_their_own_workspace_and_identity(self):
        creds_a = make_credentials("a")
        creds_b = make_credentials("b")

        with patched_client_secret_credential():
            client_a = BaseFabricConnectionManager.get_fabric_api_client(creds_a)
            client_b = BaseFabricConnectionManager.get_fabric_api_client(creds_b)

            assert client_a is not client_b

            with mock.patch(
                "dbt.adapters.fabric.fabric_api_client.requests.request",
                side_effect=_fabric_api_side_effect,
            ) as request:
                client_a.get_warehouses()
                client_b.get_warehouses()

        urls = [_requested_url(call) for call in request.call_args_list]
        tokens = [_authorization(call) for call in request.call_args_list]
        assert "/workspaces/workspace-a/" in urls[0]
        assert "/workspaces/workspace-b/" in urls[1]
        assert tokens == [
            f"Bearer token|client-a|{FABRIC_SCOPE}",
            f"Bearer token|client-b|{FABRIC_SCOPE}",
        ]

    def test_api_client_is_reused_within_a_context(self):
        creds = make_credentials("a")

        first = BaseFabricConnectionManager.get_fabric_api_client(creds)
        second = BaseFabricConnectionManager.get_fabric_api_client(creds)

        assert first is second

    def test_api_client_state_is_not_shared_between_contexts(self):
        creds_a = make_credentials("a", workspace_id=None, workspace_name="workspace-a")
        creds_b = make_credentials("b", workspace_id=None, workspace_name="workspace-b")

        def resolve(method, url, json=None, headers=None):
            name = unquote_plus(url).split("name eq '")[1].split("'")[0]
            return _response(200, {"value": [{"id": f"resolved-{name}"}]})

        with patched_client_secret_credential():
            client_a = BaseFabricConnectionManager.get_fabric_api_client(creds_a)
            client_b = BaseFabricConnectionManager.get_fabric_api_client(creds_b)

            with mock.patch(
                "dbt.adapters.fabric.fabric_api_client.requests.request", side_effect=resolve
            ):
                assert client_a.get_workspace_id() == "resolved-workspace-a"
                assert client_b.get_workspace_id() == "resolved-workspace-b"

    def test_purview_clients_use_their_own_endpoint_and_identity(self):
        creds_a = make_credentials("a")
        creds_b = make_credentials("b")

        with patched_client_secret_credential():
            client_a = BaseFabricConnectionManager.get_purview_client(creds_a)
            client_b = BaseFabricConnectionManager.get_purview_client(creds_b)

            assert client_a is not client_b

            with mock.patch(
                "dbt.adapters.fabric.purview_client.requests.request",
                return_value=_response(200, {"value": []}),
            ) as request:
                client_a.search_entities("model_one")
                client_b.search_entities("model_one")

        urls = [_requested_url(call) for call in request.call_args_list]
        tokens = [_authorization(call) for call in request.call_args_list]
        assert urls[0].startswith("https://purview-a.example.invalid/")
        assert urls[1].startswith("https://purview-b.example.invalid/")
        assert tokens == [
            f"Bearer token|client-a|{_PURVIEW_SCOPE}",
            f"Bearer token|client-b|{_PURVIEW_SCOPE}",
        ]

    def test_purview_client_is_reused_within_a_context(self):
        creds = make_credentials("a")

        first = BaseFabricConnectionManager.get_purview_client(creds)
        second = BaseFabricConnectionManager.get_purview_client(creds)

        assert first is second

    def test_purview_endpoint_is_required_per_context(self):
        BaseFabricConnectionManager.get_purview_client(make_credentials("a"))

        with pytest.raises(dbt_common.exceptions.DbtConfigError, match="purview_endpoint"):
            BaseFabricConnectionManager.get_purview_client(
                make_credentials("b", purview_endpoint=None)
            )

    def test_configured_host_is_per_credential_context(self):
        creds_a = make_credentials("a", host="host-a.example.invalid")
        creds_b = make_credentials("b", host="host-b.example.invalid")

        assert FabricConnectionManager.get_host(creds_a) == "host-a.example.invalid"
        assert FabricConnectionManager.get_host(creds_b) == "host-b.example.invalid"

    def test_resolved_host_is_per_credential_context(self):
        creds_a = make_credentials("a")
        creds_b = make_credentials("b")

        with patched_client_secret_credential():
            with mock.patch(
                "dbt.adapters.fabric.fabric_api_client.requests.request",
                side_effect=_fabric_api_side_effect,
            ):
                host_a = FabricConnectionManager.get_host(creds_a)
                host_b = FabricConnectionManager.get_host(creds_b)

        assert host_a == "workspace-a.datawarehouse.example.invalid"
        assert host_b == "workspace-b.datawarehouse.example.invalid"

    def test_failed_host_resolution_is_not_cached(self):
        creds = make_credentials("a")
        attempts: list[str] = []

        def flaky(method, url, json=None, headers=None):
            attempts.append(url)
            if len(attempts) == 1:
                return _response(500)
            return _fabric_api_side_effect(method, url, json, headers)

        with patched_client_secret_credential():
            with mock.patch(
                "dbt.adapters.fabric.fabric_api_client.requests.request", side_effect=flaky
            ):
                with pytest.raises(FabricApiError):
                    FabricConnectionManager.get_host(creds)

                assert (
                    FabricConnectionManager.get_host(creds)
                    == "workspace-a.datawarehouse.example.invalid"
                )

    def test_resolved_host_is_cached_within_a_context(self):
        creds = make_credentials("a")

        with patched_client_secret_credential():
            with mock.patch(
                "dbt.adapters.fabric.fabric_api_client.requests.request",
                side_effect=_fabric_api_side_effect,
            ) as request:
                first = FabricConnectionManager.get_host(creds)
                second = FabricConnectionManager.get_host(creds)

        assert first == second
        assert request.call_count == 1


class TestApiClientFactory:
    def test_create_is_scoped_to_the_credential_context(self):
        creds_a = make_credentials("a")
        creds_b = make_credentials("b")
        provider_a = BaseFabricConnectionManager.get_fabric_token_provider(creds_a)
        provider_b = BaseFabricConnectionManager.get_fabric_token_provider(creds_b)

        client_a = FabricApiClient.create(creds_a, provider_a)
        client_b = FabricApiClient.create(creds_b, provider_b)

        assert client_a is not client_b
        assert client_a is FabricApiClient.create(creds_a, provider_a)
        assert client_b._credentials is creds_b
        assert client_b._token_provider is provider_b

    def test_create_matches_the_connection_manager_client(self):
        creds = make_credentials("a")
        provider = BaseFabricConnectionManager.get_fabric_token_provider(creds)

        assert FabricApiClient.create(creds, provider) is (
            BaseFabricConnectionManager.get_fabric_api_client(creds)
        )

    def test_create_honours_an_explicit_token_provider(self):
        creds = make_credentials("a")
        other_provider = FabricTokenProvider(make_credentials("b"))

        default_client = BaseFabricConnectionManager.get_fabric_api_client(creds)
        explicit_client = FabricApiClient.create(creds, other_provider)

        assert explicit_client is not default_client
        assert explicit_client._token_provider is other_provider


class TestCopiedCredentials:
    """A copy that is configured before its first use is a context of its own."""

    def test_shallow_copy_gets_its_own_provider_and_token(self):
        original = make_credentials(
            "a", authentication="ActiveDirectoryAccessToken", access_token="synthetic-a"
        )
        provider_a = BaseFabricConnectionManager.get_fabric_token_provider(original)
        assert provider_a.get_access_token() == "synthetic-a"

        clone = copy.copy(original)
        clone.access_token = "synthetic-b"
        provider_b = BaseFabricConnectionManager.get_fabric_token_provider(clone)

        assert clone is not original
        assert provider_b is not provider_a
        assert provider_b.credentials is clone
        assert provider_b.get_access_token() == "synthetic-b"

        # The original keeps the context it already had.
        assert BaseFabricConnectionManager.get_fabric_token_provider(original) is provider_a
        assert provider_a.get_access_token() == "synthetic-a"

    def test_shallow_copy_gets_its_own_api_and_purview_clients(self):
        original = make_credentials("a")
        with patched_client_secret_credential():
            client_a = BaseFabricConnectionManager.get_fabric_api_client(original)
            purview_a = BaseFabricConnectionManager.get_purview_client(original)

            clone = copy.copy(original)
            clone.tenant_id = "tenant-b"
            clone.client_id = "client-b"
            clone.client_secret = "synthetic-secret-b"
            clone.workspace_id = "workspace-b"
            clone.purview_endpoint = "https://purview-b.example.invalid"

            client_b = BaseFabricConnectionManager.get_fabric_api_client(clone)
            purview_b = BaseFabricConnectionManager.get_purview_client(clone)

            assert client_b is not client_a
            assert purview_b is not purview_a

            with mock.patch(
                "dbt.adapters.fabric.fabric_api_client.requests.request",
                side_effect=_fabric_api_side_effect,
            ) as fabric_request:
                client_b.get_warehouses()

            with mock.patch(
                "dbt.adapters.fabric.purview_client.requests.request",
                return_value=_response(200, {"value": []}),
            ) as purview_request:
                purview_b.search_entities("model_one")

        fabric_call = fabric_request.call_args_list[0]
        purview_call = purview_request.call_args_list[0]
        assert "/workspaces/workspace-b/" in _requested_url(fabric_call)
        assert _authorization(fabric_call) == f"Bearer token|client-b|{FABRIC_SCOPE}"
        assert _requested_url(purview_call).startswith("https://purview-b.example.invalid/")
        assert _authorization(purview_call) == f"Bearer token|client-b|{_PURVIEW_SCOPE}"

    def test_shallow_copy_resolves_its_own_host(self):
        original = make_credentials("a")

        with patched_client_secret_credential():
            with mock.patch(
                "dbt.adapters.fabric.fabric_api_client.requests.request",
                side_effect=_fabric_api_side_effect,
            ):
                host_a = FabricConnectionManager.get_host(original)

                clone = copy.copy(original)
                clone.workspace_id = "workspace-b"
                host_b = FabricConnectionManager.get_host(clone)

        assert host_a == "workspace-a.datawarehouse.example.invalid"
        assert host_b == "workspace-b.datawarehouse.example.invalid"

    def test_deepcopy_and_pickle_get_their_own_provider_and_token(self):
        original = make_credentials(
            "a", authentication="ActiveDirectoryAccessToken", access_token="synthetic-a"
        )
        provider_a = BaseFabricConnectionManager.get_fabric_token_provider(original)
        BaseFabricConnectionManager.get_fabric_api_client(original)
        assert provider_a.get_access_token() == "synthetic-a"

        clone = copy.deepcopy(original)
        clone.access_token = "synthetic-deepcopy"
        restored = pickle.loads(pickle.dumps(original))
        restored.access_token = "synthetic-pickle"

        clone_provider = BaseFabricConnectionManager.get_fabric_token_provider(clone)
        restored_provider = BaseFabricConnectionManager.get_fabric_token_provider(restored)

        assert clone_provider.credentials is clone
        assert clone_provider.get_access_token() == "synthetic-deepcopy"
        assert restored_provider.credentials is restored
        assert restored_provider.get_access_token() == "synthetic-pickle"
        assert len({id(provider_a), id(clone_provider), id(restored_provider)}) == 3

        clone_client = BaseFabricConnectionManager.get_fabric_api_client(clone)
        assert clone_client is not BaseFabricConnectionManager.get_fabric_api_client(original)
        assert clone_client._credentials is clone


class TestCredentialStateHygiene:
    def test_runtime_state_is_not_serialized(self):
        creds = make_credentials("a")
        before = creds.to_dict()

        BaseFabricConnectionManager.get_fabric_api_client(creds)
        BaseFabricConnectionManager.get_purview_client(creds)

        after = creds.to_dict()

        assert after == before
        # Runtime objects (clients, providers, locks) are not JSON-serializable.
        json.dumps(after)

    def test_credentials_stay_copyable_and_picklable(self):
        creds = make_credentials("a")
        # The runtime state holds a lock, which neither copies nor pickles.
        BaseFabricConnectionManager.get_fabric_api_client(creds)
        BaseFabricConnectionManager.get_purview_client(creds)

        copies = [copy.copy(creds), copy.deepcopy(creds), pickle.loads(pickle.dumps(creds))]

        assert [c.client_id for c in copies] == ["client-a"] * 3
        assert [c.to_dict() for c in copies] == [creds.to_dict()] * 3

    def test_runtime_state_is_released_with_the_credentials(self):
        creds = make_credentials("a")
        provider = BaseFabricConnectionManager.get_fabric_token_provider(creds)
        client = BaseFabricConnectionManager.get_fabric_api_client(creds)
        observed = [weakref.ref(creds), weakref.ref(provider), weakref.ref(client)]

        del creds, provider, client
        gc.collect()

        assert [ref() for ref in observed] == [None, None, None]
