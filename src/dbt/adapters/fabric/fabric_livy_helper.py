import time
from typing import Any, Dict, Optional

import requests

from dbt.adapters.base import PythonJobHelper
from dbt.adapters.fabric.fabric_credentials import FabricCredentials
from dbt.adapters.fabric.fabric_token_provider import FabricTokenProvider


class LivySessionResult:
    def __init__(self, success: bool, error_message: Optional[str] = None) -> None:
        self.error_message = error_message
        self.success = success


class LivySession:
    _TOKEN_SCOPE = FabricTokenProvider.FABRIC_CREDENTIAL_SCOPE
    _API_VERSION = "2023-12-01"
    _POLLING_INTERVAL = 5  # seconds
    _MAX_POLLING_ATTEMPTS = 60  # 5 minutes

    def __init__(
        self, workspace_id: str, lakehouse_id: str, token_provider: FabricTokenProvider
    ) -> None:
        self._workspace_id = workspace_id
        self._lakehouse_id = lakehouse_id
        self._token_provider = token_provider
        self.session_id = self._initialize_session()

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
        }

    def _initialize_session(self) -> str:
        response = requests.post(
            self._get_api_url("sessions"),
            headers=self._get_headers(),
            json={"name": "dbt-fabric"},
        )
        response.raise_for_status()
        return response.json()["id"]

    def _get_api_url(self, endpoint: str) -> str:
        return f"https://api.fabric.microsoft.com/v1/workspaces/{self._workspace_id}/lakehouses/{self._lakehouse_id}/livyapi/versions/{self._API_VERSION}/{endpoint}"

    def _get_logs_url(self) -> str:
        return f"https://app.fabric.microsoft.com/workloads/de-ds/sparkmonitor/{self._lakehouse_id}/{self.session_id}?experience=fabric-developer"

    @property
    def _session_state(self) -> str:
        response = requests.get(
            self._get_api_url(f"sessions/{self.session_id}"),
            headers=self._get_headers(),
        )
        response.raise_for_status()
        return response.json()["state"]

    @property
    def _access_token(self) -> str:
        return self._token_provider.get_api_token()

    def _wait_for_session_ready(self) -> None:
        attempts = 0
        while self._session_state != "idle":
            if attempts >= self._MAX_POLLING_ATTEMPTS:
                raise TimeoutError("Livy session did not become idle in time.")
            attempts += 1
            time.sleep(self._POLLING_INTERVAL)

    def _wait_for_statement_ready(self, statement_id: str) -> dict[str, Any]:
        attempts = 0
        while True:
            response = requests.get(
                self._get_api_url(f"statements/{statement_id}"),
                headers=self._get_headers(),
            )
            response.raise_for_status()
            statement_state = response.json()["state"]
            if statement_state in ("available", "error"):
                return response.json()
            if attempts >= self._MAX_POLLING_ATTEMPTS:
                raise TimeoutError("Livy statement did not become available in time.")
            attempts += 1
            time.sleep(self._POLLING_INTERVAL)

    def submit(self, python_code: str) -> Any:
        try:
            self._wait_for_session_ready()
            response = requests.post(
                self._get_api_url(f"sessions/{self.session_id}/statements"),
                headers=self._get_headers(),
                json={"code": python_code, "kind": "pyspark"},
            )
            response.raise_for_status()
            statement_id = response.json()["id"]
            response = self._wait_for_statement_ready(statement_id)
            return LivySessionResult(success=True)
        except Exception as e:
            print(f"Logs available at {self._get_logs_url()}")
            return LivySessionResult(success=False, error_message=str(e))


class FabricLivyHelper(PythonJobHelper):
    _livy_session: Optional[LivySession] = None

    def __init__(self, parsed_model: Dict, credential: FabricCredentials) -> None:
        if not self._livy_session:
            self._livy_session = LivySession(
                workspace_id=credential.workspace_id,
                lakehouse_id=credential.lakehouse_id,
                token_provider=FabricTokenProvider(credential),
            )

    def submit(self, compiled_code: str) -> Any:
        return self._livy_session.submit(compiled_code)
