import abc

from dbt.adapters.fabric.base_credentials import BaseFabricCredentials
from dbt.adapters.fabric.fabric_api_client import FabricApiClient
from dbt.adapters.fabric.fabric_token_provider import FabricTokenProvider
from dbt.adapters.sql.connections import SQLConnectionManager


class BaseFabricConnectionManager(SQLConnectionManager, metaclass=abc.ABCMeta):
    _fabric_token_provider: FabricTokenProvider | None = None
    _fabric_api_client: FabricApiClient | None = None

    @classmethod
    def get_fabric_token_provider(cls, credentials: BaseFabricCredentials) -> FabricTokenProvider:
        if cls._fabric_token_provider is None:
            cls._fabric_token_provider = FabricTokenProvider(credentials)
        return cls._fabric_token_provider

    @classmethod
    def get_fabric_api_client(cls, credentials: BaseFabricCredentials) -> FabricApiClient:
        if cls._fabric_api_client is None:
            cls._fabric_api_client = FabricApiClient(
                credentials, cls.get_fabric_token_provider(credentials)
            )
        return cls._fabric_api_client
