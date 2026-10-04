"""Regression tests for https://github.com/microsoft/dbt-fabric/issues/434.

Upgrading from 1.9.9 broke profiles that used the legacy ``authentication``
values ``ServicePrincipal`` and ``fabricnotebook``: the token provider's
dispatch logic only recognized the newer canonical names
(``ActiveDirectoryServicePrincipal`` / ``notebookutils``) and raised
``Unsupported authentication method`` for the old aliases, even though
``BaseFabricCredentials.__post_serialize__`` already normalizes
``ServicePrincipal`` for display/serialization purposes.
"""

from dataclasses import dataclass
from typing import Any
from unittest.mock import patch

import pytest
from azure.core.credentials import AccessToken
from azure.identity import ClientSecretCredential

from dbt.adapters.fabric.fabric_token_provider import (
    AUTHENTICATION_ALIASES,
    FabricTokenProvider,
)


@dataclass
class FakeCredentials:
    """Lightweight stand-in for FabricTokenProvider tests (not a real Credentials)."""

    database: str = "test_db"
    schema: str = "dbo"
    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    access_token: str | None = None
    token_scope: str | None = None
    authentication: str = "ServicePrincipal"
    credential_class: str | None = None
    credential_kwargs: dict[str, Any] | None = None

    def __post_init__(self):
        if self.credential_kwargs is None:
            self.credential_kwargs = {}


class TestAuthenticationAliasesMapping:
    """The alias table itself must contain the two reported legacy names."""

    def test_serviceprincipal_maps_to_canonical_name(self):
        assert AUTHENTICATION_ALIASES["serviceprincipal"] == "activedirectoryserviceprincipal"

    def test_fabricnotebook_maps_to_notebookutils(self):
        assert AUTHENTICATION_ALIASES["fabricnotebook"] == "notebookutils"


class TestServicePrincipalAliasDispatch:
    """authentication: ServicePrincipal must keep acquiring tokens like
    ActiveDirectoryServicePrincipal, as it did before 1.11.0.
    """

    def _credentials(self, authentication: str) -> FakeCredentials:
        return FakeCredentials(
            authentication=authentication,
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret",
        )

    @patch.object(ClientSecretCredential, "get_token")
    def test_serviceprincipal_alias_acquires_token(self, mock_get_token):
        mock_get_token.return_value = AccessToken(token="alias-token", expires_on=9999999999)
        provider = FabricTokenProvider(self._credentials("ServicePrincipal"))

        token = provider.get_access_token("https://database.windows.net/.default")

        assert token == "alias-token"
        mock_get_token.assert_called_once()

    @patch.object(ClientSecretCredential, "get_token")
    def test_serviceprincipal_alias_is_case_insensitive(self, mock_get_token):
        mock_get_token.return_value = AccessToken(token="alias-token", expires_on=9999999999)
        provider = FabricTokenProvider(self._credentials("servicePRINCIPAL"))

        token = provider.get_access_token("https://database.windows.net/.default")

        assert token == "alias-token"

    @patch.object(ClientSecretCredential, "get_token")
    def test_canonical_name_still_works(self, mock_get_token):
        """The new canonical name must keep working after the alias fix."""
        mock_get_token.return_value = AccessToken(token="canonical-token", expires_on=9999999999)
        provider = FabricTokenProvider(self._credentials("ActiveDirectoryServicePrincipal"))

        token = provider.get_access_token("https://database.windows.net/.default")

        assert token == "canonical-token"

    def test_serviceprincipal_alias_still_requires_client_credentials(self):
        """The alias must enforce the same required-field validation as the
        canonical name, not silently skip it."""
        creds = FakeCredentials(authentication="ServicePrincipal")
        provider = FabricTokenProvider(creds)

        with pytest.raises(ValueError, match="client_id, client_secret, and tenant_id"):
            provider.get_access_token("https://database.windows.net/.default")


class TestFabricNotebookAliasDispatch:
    """authentication: fabricnotebook must keep working as an alias for
    notebookutils.
    """

    @patch("dbt.adapters.fabric.fabric_token_provider.get_notebookutils_access_token")
    def test_fabricnotebook_alias_calls_notebookutils(self, mock_get_token):
        mock_get_token.return_value = AccessToken(token="notebook-token", expires_on=9999999999)
        creds = FakeCredentials(authentication="fabricnotebook")
        provider = FabricTokenProvider(creds)

        token = provider.get_access_token("https://database.windows.net/.default")

        assert token == "notebook-token"
        mock_get_token.assert_called_once_with("https://database.windows.net/.default")

    @patch("dbt.adapters.fabric.fabric_token_provider.get_notebookutils_access_token")
    def test_fabricnotebook_alias_is_case_insensitive(self, mock_get_token):
        mock_get_token.return_value = AccessToken(token="notebook-token", expires_on=9999999999)
        creds = FakeCredentials(authentication="FabricNotebook")
        provider = FabricTokenProvider(creds)

        token = provider.get_access_token("https://database.windows.net/.default")

        assert token == "notebook-token"

    @patch("dbt.adapters.fabric.fabric_token_provider.get_notebookutils_access_token")
    def test_canonical_notebookutils_still_works(self, mock_get_token):
        mock_get_token.return_value = AccessToken(token="notebook-token", expires_on=9999999999)
        creds = FakeCredentials(authentication="notebookutils")
        provider = FabricTokenProvider(creds)

        token = provider.get_access_token("https://database.windows.net/.default")

        assert token == "notebook-token"


class TestUnsupportedAuthenticationStillRejected:
    """Unknown authentication values (not aliases) must still fail clearly."""

    def test_unknown_method_raises(self):
        creds = FakeCredentials(authentication="NotARealMethod")
        provider = FabricTokenProvider(creds)

        with pytest.raises(ValueError, match="Unsupported authentication method: NotARealMethod"):
            provider.get_access_token("https://database.windows.net/.default")
