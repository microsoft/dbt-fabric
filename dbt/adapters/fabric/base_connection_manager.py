import abc

import dbt_common.exceptions

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabric.base_credentials import BaseFabricCredentials
from dbt.adapters.fabric.credential_context import credential_runtime_state
from dbt.adapters.fabric.fabric_api_client import FabricApiClient
from dbt.adapters.fabric.fabric_token_provider import FabricTokenProvider
from dbt.adapters.fabric.purview_client import PurviewClient
from dbt.adapters.sql.connections import SQLConnectionManager

logger = AdapterLogger("fabric")


class BaseFabricConnectionManager(SQLConnectionManager, metaclass=abc.ABCMeta):
    @classmethod
    def get_fabric_token_provider(cls, credentials: BaseFabricCredentials) -> FabricTokenProvider:
        """Return the FabricTokenProvider of these credentials, creating it once.

        The provider is shared by everything using the same credentials object
        and is never shared with another credential context.

        Args:
            credentials: Fabric connection credentials used to configure the provider.
        """
        return credential_runtime_state(credentials).get_or_create(
            "fabric_token_provider", lambda: FabricTokenProvider(credentials)
        )

    @classmethod
    def get_fabric_api_client(cls, credentials: BaseFabricCredentials) -> FabricApiClient:
        """Return the FabricApiClient of these credentials, creating it once.

        Args:
            credentials: Fabric connection credentials used to configure the client.
        """
        return FabricApiClient.create(credentials, cls.get_fabric_token_provider(credentials))

    @classmethod
    def get_purview_client(cls, credentials: BaseFabricCredentials) -> PurviewClient:
        """Return the PurviewClient of these credentials, creating it once.

        Raises:
            DbtConfigError: If purview_endpoint is not configured.
        """
        endpoint = credentials.purview_endpoint
        if not endpoint:
            raise dbt_common.exceptions.DbtConfigError(
                "purview_endpoint must be set in profiles.yml to use Purview integration"
            )
        return credential_runtime_state(credentials).get_or_create(
            "purview_client",
            lambda: PurviewClient(endpoint, cls.get_fabric_token_provider(credentials)),
        )
