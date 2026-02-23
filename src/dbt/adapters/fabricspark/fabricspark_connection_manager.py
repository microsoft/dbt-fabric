from contextlib import contextmanager
from typing import Any

from dbt.adapters.contracts.connection import AdapterResponse, Connection, ConnectionState
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabric.base_connection_manager import BaseFabricConnectionManager
from dbt.adapters.fabric.fabric_livy_session import LivySession
from dbt.adapters.fabricspark.fabricspark_connection import FabricSparkConnection

logger = AdapterLogger("fabricspark")


class FabricSparkConnectionManager(BaseFabricConnectionManager):
    TYPE = "fabricspark"

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield

        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)
            raise

    def cancel(self, connection: Connection):
        connection.handle.cancel()

    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        raise NotImplementedError

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        def connect() -> FabricSparkConnection:
            livy_session = LivySession(cls.get_fabric_api_client(connection.credentials))
            livy_session.wait_for_session_ready()
            return FabricSparkConnection(livy_session)

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=connection.credentials.retries,
            retry_timeout=10,
            retryable_exceptions=[TimeoutError],
        )
