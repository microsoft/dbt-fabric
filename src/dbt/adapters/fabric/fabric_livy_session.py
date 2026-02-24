import time
from dataclasses import dataclass
from typing import Any

from dbt.adapters.base.impl import PythonSubmissionResult
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabric.fabric_api_client import FabricApiClient

logger = AdapterLogger("fabricspark")


@dataclass
class LivySubmissionResult(PythonSubmissionResult):
    success: bool
    error_message: str | None = None


@dataclass
class LivySessionResult:
    statement_id: int = -1
    success: bool = False
    error_message: str | None = None
    status_code: str | None = None
    json_data: dict[str, Any] | None = {}

    def to_submission_result(self, code: str) -> LivySubmissionResult:
        return LivySubmissionResult(
            run_id=str(self.statement_id),
            compiled_code=code,
            success=self.success,
            error_message=self.error_message,
        )


class LivySession:
    _POLLING_INTERVAL = 3  # seconds

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

    def wait_for_statement_ready(self, statement_id: int) -> dict[str, Any]:
        start_time = time.time()
        while True:
            statement_response = self._fabric_api_client.get_livy_statement(statement_id)
            statement_state = statement_response.get("state", "unknown")
            if statement_state in ("available", "error"):
                return statement_response
            if time.time() - start_time >= self._fabric_api_client._credentials.query_timeout:
                raise TimeoutError("Livy statement did not become available in time.")
            time.sleep(self._POLLING_INTERVAL)

    def wait_and_get_statement_result(self, statement_id: int) -> LivySessionResult:
        try:
            response = self.wait_for_statement_ready(statement_id)
            return LivySessionResult(
                statement_id=statement_id,
                success=response["state"] == "available"
                and response.get("output", {}).get("status") == "ok",
                error_message=response.get("output", {}).get("evalue"),
                status_code=response.get("output", {}).get("status"),
                json_data=response.get("output", {}).get("data", {}).get("application/json", {}),
            )
        except TimeoutError as e:
            logger.error(
                f"Timeout (> {self._fabric_api_client._credentials.query_timeout}s) while waiting for Livy statement to be ready. Logs URL: {self.get_logs_url()}"
            )
            logger.exception(e)
            return LivySessionResult(
                statement_id=statement_id, success=False, error_message=str(e)
            )
        except Exception as e:
            logger.error(
                f"Error while waiting for Livy statement to be ready. Logs URL: {self.get_logs_url()}"
            )
            logger.exception(e)
            return LivySessionResult(
                statement_id=statement_id, success=False, error_message=str(e)
            )

    def run_statement(
        self, statement_code: str, statement_language: str, wait_for_result: bool = True
    ) -> LivySessionResult | int:
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
            return LivySessionResult(success=False, error_message=str(e))
        if wait_for_result:
            return self.wait_and_get_statement_result(statement_id)
        else:
            return statement_id
