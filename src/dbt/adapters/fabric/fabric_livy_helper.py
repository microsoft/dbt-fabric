import time
from typing import Any

from dbt.adapters.base import PythonJobHelper
from dbt.adapters.fabric.fabric_api_client import FabricApiClient
from dbt.adapters.fabric.fabric_credentials import FabricCredentials
from dbt.adapters.fabric.fabric_token_provider import FabricTokenProvider


class LivySessionResult:
    def __init__(self, success: bool, error_message: str | None = None) -> None:
        self.error_message = error_message
        self.success = success


class LivySession:
    _POLLING_INTERVAL = 5  # seconds
    _MAX_POLLING_ATTEMPTS_SESSION_READY = 120  # 10 minutes
    _MAX_POLLING_ATTEMPTS_STATEMENT_READY = 720  # 60 minutes

    def __init__(self, fabric_api_client: FabricApiClient) -> None:
        self._fabric_api_client = fabric_api_client

    def _get_logs_url(self) -> str:
        return f"https://app.fabric.microsoft.com/workloads/de-ds/sparkmonitor/{self._fabric_api_client.get_lakehouse_id()}/{self._fabric_api_client.get_livy_session_id()}"

    def _wait_for_session_ready(self) -> None:
        attempts = 0
        while self._fabric_api_client.get_livy_session_state() != "idle":
            if attempts >= self._MAX_POLLING_ATTEMPTS_SESSION_READY:
                raise TimeoutError("Livy session did not become idle in time.")
            attempts += 1
            time.sleep(self._POLLING_INTERVAL)

    def _wait_for_statement_ready(self, statement_id: str) -> dict[str, Any]:
        attempts = 0
        while True:
            time.sleep(self._POLLING_INTERVAL)
            statement_response = self._fabric_api_client.get_livy_statement(statement_id)
            statement_state = statement_response.get("state", "unknown")
            if statement_state in ("available", "error"):
                return statement_response
            if attempts >= self._MAX_POLLING_ATTEMPTS_STATEMENT_READY:
                raise TimeoutError("Livy statement did not become available in time.")
            attempts += 1

    def submit(self, python_code: str) -> Any:
        try:
            self._wait_for_session_ready()
            statement_id = self._fabric_api_client.submit_livy_statement(python_code)
            response = self._wait_for_statement_ready(statement_id)
            return LivySessionResult(
                success=response["state"] == "available"
                and response.get("output", {}).get("status") == "ok",
                error_message=response.get("output", {}).get("evalue"),
            )
        except Exception as e:
            print(f"Logs available at {self._get_logs_url()}")
            return LivySessionResult(success=False, error_message=str(e))


class FabricLivyHelper(PythonJobHelper):
    _livy_session: LivySession | None = None
    _sql_endpoint: str | None = None

    def __init__(self, parsed_model: dict, credential: FabricCredentials) -> None:
        fabric_api_client = FabricApiClient.create(credential, FabricTokenProvider(credential))

        if not self._livy_session:
            self._livy_session = LivySession(fabric_api_client)

        if not self._sql_endpoint:
            self._sql_endpoint = fabric_api_client.get_warehouse_connection_string()

    def submit(self, compiled_code: str) -> Any:
        assert self._livy_session is not None
        assert self._sql_endpoint is not None
        compiled_code = compiled_code.replace("DBT_FABRIC_REPLACED_WITH_HOST", self._sql_endpoint)
        return self._livy_session.submit(compiled_code)
