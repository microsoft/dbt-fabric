import re
from typing import Any

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.base.impl import PythonJobHelper
from dbt.adapters.fabric.fabric_api_client import FabricApiClient
from dbt.adapters.fabric.fabric_credentials import FabricCredentials
from dbt.adapters.fabric.fabric_livy_session import LivySession
from dbt.adapters.fabric.fabric_token_provider import FabricTokenProvider


class FabricLivyHelper(PythonJobHelper):
    _livy_session: LivySession | None = None
    _sql_endpoint: str | None = None

    def __init__(self, parsed_model: dict, credential: FabricCredentials) -> None:
        fabric_api_client: FabricApiClient = FabricApiClient.create(
            credential, FabricTokenProvider(credential)
        )

        if not self._livy_session:
            self._livy_session = LivySession(fabric_api_client)

        if not self._sql_endpoint:
            self._sql_endpoint = fabric_api_client.get_warehouse_connection_string()

    def submit(self, compiled_code: str) -> Any:
        assert self._livy_session is not None
        assert self._sql_endpoint is not None
        compiled_code = compiled_code.replace("DBT_FABRIC_REPLACED_WITH_HOST", self._sql_endpoint)
        result = self._livy_session.run_statement(compiled_code, "python")
        if not result.success:
            raise DbtRuntimeError(
                f"Python statement execution failed. Logs URL: {self._livy_session.get_logs_url()}. Error: {result.error_message}"
            )
        return result
