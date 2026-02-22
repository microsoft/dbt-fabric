import time
from typing import Any

from dbt.adapters.base.impl import PythonSubmissionResult
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabric.fabric_api_client import FabricApiClient

logger = AdapterLogger("fabricspark")


class LivySessionResult(PythonSubmissionResult):
    def __init__(
        self, statement_id: str | None, success: bool, code: str, error_message: str | None = None
    ) -> None:
        self.error_message = error_message
        self.success = success
        self.run_id = statement_id or ""
        self.compiled_code = code


class LivySession:
    _POLLING_INTERVAL = 10  # seconds

    def __init__(self, fabric_api_client: FabricApiClient) -> None:
        self._fabric_api_client = fabric_api_client

    def get_logs_url(self) -> str:
        return f"https://app.fabric.microsoft.com/workloads/de-ds/sparkmonitor/{self._fabric_api_client.get_lakehouse_id()}/{self._fabric_api_client.get_livy_session_id()}"

    def wait_for_session_ready(self) -> None:
        start_time = time.time()
        while self._fabric_api_client.get_livy_session_state() != "idle":
            if (
                time.time() - start_time
                >= self._fabric_api_client._credentials.spark_session_timeout
            ):
                raise TimeoutError("Livy session did not become idle in time.")
            time.sleep(self._POLLING_INTERVAL)

    def wait_for_statement_ready(self, statement_id: str) -> dict[str, Any]:
        start_time = time.time()
        while True:
            time.sleep(self._POLLING_INTERVAL)
            statement_response = self._fabric_api_client.get_livy_statement(statement_id)
            statement_state = statement_response.get("state", "unknown")
            if statement_state in ("available", "error"):
                return statement_response
            if time.time() - start_time >= self._fabric_api_client._credentials.query_timeout:
                raise TimeoutError("Livy statement did not become available in time.")

    def run_statement(self, statement_code: str, statement_language: str) -> LivySessionResult:
        try:
            self.wait_for_session_ready()
            func = (
                self._fabric_api_client.submit_livy_sql_statement
                if statement_language == "sql"
                else self._fabric_api_client.submit_livy_python_statement
            )
            statement_id = func(statement_code)
        except Exception as e:
            logger.error(
                f"Error while creating for Livy statement. Logs URL: {self.get_logs_url()}"
            )
            logger.exception(e)
            return LivySessionResult(
                statement_id=None, success=False, code=statement_code, error_message=str(e)
            )

        try:
            response = self.wait_for_statement_ready(statement_id)
            return LivySessionResult(
                statement_id=statement_id,
                code=statement_code,
                success=response["state"] == "available"
                and response.get("output", {}).get("status") == "ok",
                error_message=response.get("output", {}).get("evalue"),
            )
        except TimeoutError as e:
            logger.error(
                f"Timeout (> {self._fabric_api_client._credentials.query_timeout}s) while waiting for Livy statement to be ready. Logs URL: {self.get_logs_url()}"
            )
            logger.exception(e)
            return LivySessionResult(
                statement_id=statement_id, code=statement_code, success=False, error_message=str(e)
            )
        except Exception as e:
            logger.error(
                f"Error while waiting for Livy statement to be ready. Logs URL: {self.get_logs_url()}"
            )
            logger.exception(e)
            return LivySessionResult(
                statement_id=statement_id, code=statement_code, success=False, error_message=str(e)
            )
