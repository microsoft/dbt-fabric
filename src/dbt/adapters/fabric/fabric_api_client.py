import threading
import time
import urllib.parse
from typing import Any, Self

import dbt_common.exceptions
import requests

from dbt.adapters.fabric.base_credentials import BaseFabricCredentials
from dbt.adapters.fabric.fabric_token_provider import FabricTokenProvider

_livy_session_thread_lock = threading.Lock()


class FabricApiClient:
    _LIVY_API_VERSION = "2023-12-01"
    _WAREHOUSE_SNAPSHOT_TIMEOUT_SECONDS = 60 * 30  # 30 minutes
    _instance: Self | None = None

    def __init__(
        self, credentials: BaseFabricCredentials, token_provider: FabricTokenProvider
    ) -> None:
        self._credentials = credentials
        self._token_provider = token_provider
        self._warehouse_connection_string: str | None = None
        self._lakehouse_id: str | None = None
        self._warehouse_id: str | None = None
        self._workspace_id: str | None = None
        self._cached_warehouses: list[dict] | None = None
        self._cached_lakehouses: list[dict] | None = None
        self._livy_session_id: str | None = None
        self._warehouse_snapshot_operations: dict[str, str] = {}

    @classmethod
    def create(
        cls, credentials: BaseFabricCredentials, token_provider: FabricTokenProvider
    ) -> Self:
        if cls._instance is None:
            cls._instance = FabricApiClient(credentials, token_provider)
        return cls._instance

    def _get_auth_headers(self) -> dict[str, str]:
        token = self._token_provider.get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

    def _api_request(
        self, url: str, method: str = "get", body: dict | None = None
    ) -> requests.Response:
        response = requests.request(method, url, json=body, headers=self._get_auth_headers())

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            time.sleep(retry_after)
            return self._api_request(url, method, body)

        if not (200 <= response.status_code < 300):
            raise dbt_common.exceptions.DbtRuntimeError(
                f"{method} request to {url} failed with status code {response.status_code}: {response.text}"
            )
        return response

    def _api_get(self, url: str) -> requests.Response:
        return self._api_request(url, method="get")

    def _api_post(self, url: str, body: dict) -> requests.Response:
        return self._api_request(url, method="post", body=body)

    def _api_patch(self, url: str, body: dict) -> requests.Response:
        return self._api_request(url, method="patch", body=body)

    def _api_delete(self, url: str) -> requests.Response:
        return self._api_request(url, method="delete")

    def get_workspace_id(self) -> str:
        if self._workspace_id is not None:
            return self._workspace_id
        if self._credentials.workspace_id:
            return self._credentials.workspace_id
        if not self._credentials.workspace_name:
            raise dbt_common.exceptions.DbtConfigError(
                "Either workspace_id or workspace_name must be provided."
            )

        query_param = f"name eq '{self._credentials.workspace_name}'"
        query_param_encoded = urllib.parse.quote_plus(query_param)
        response = self._api_get(
            f"{self._credentials.powerbi_base_api_uri}/myorg/groups?$filter={query_param_encoded}"
        )
        workspaces = response.json().get("value", [])

        if len(workspaces) == 0:
            raise dbt_common.exceptions.DbtRuntimeError(
                f"No workspace found with name {self._credentials.workspace_name}"
            )

        self._workspace_id = workspaces[0]["id"]
        assert self._workspace_id is not None
        return self._workspace_id

    def get_warehouses(self, fetch_all: bool = True) -> list[dict]:
        if self._cached_warehouses is not None:
            return self._cached_warehouses

        workspace_id = self.get_workspace_id()

        url = f"{self._credentials.fabric_base_api_uri}/workspaces/{workspace_id}/warehouses"
        warehouses = []

        while url is not None:
            response = self._api_get(url)
            warehouses = warehouses + response.json().get("value", [])
            url = response.json().get("continuationUri", None) if fetch_all else None

        if fetch_all:
            self._cached_warehouses = warehouses
        return warehouses

    def get_lakehouses(self, fetch_all: bool = True) -> list[dict]:
        if self._cached_lakehouses is not None:
            return self._cached_lakehouses

        workspace_id = self.get_workspace_id()

        url = f"{self._credentials.fabric_base_api_uri}/workspaces/{workspace_id}/lakehouses"
        lakehouses = []

        while url is not None:
            response = self._api_get(url)
            lakehouses = lakehouses + response.json().get("value", [])
            url = response.json().get("continuationUri", None) if fetch_all else None

        if fetch_all:
            self._cached_lakehouses = lakehouses
        return lakehouses

    def get_warehouse_connection_string(self) -> str:
        if self._warehouse_connection_string is not None:
            return self._warehouse_connection_string

        # first we try to find it in any warehouse (they all have the same connection string)
        warehouses = self.get_warehouses(fetch_all=False)
        if len(warehouses) > 0:
            self._warehouse_connection_string = warehouses[0]["properties"]["connectionString"]
            assert self._warehouse_connection_string is not None
            return self._warehouse_connection_string

        # then we try to find it in any lakehouse (also have the same connection string)
        lakehouses = self.get_lakehouses(fetch_all=False)
        if len(lakehouses) > 0:
            self._warehouse_connection_string = lakehouses[0]["properties"][
                "sqlEndpointProperties"
            ]["connectionString"]
            assert self._warehouse_connection_string is not None
            return self._warehouse_connection_string

        raise dbt_common.exceptions.DbtRuntimeError(
            f"No Data Warehouses or Lakehouses found in workspace"
        )

    def get_lakehouse_id(self) -> str:
        if self._lakehouse_id is not None:
            return self._lakehouse_id
        if not self._credentials.lakehouse_name:
            raise dbt_common.exceptions.DbtConfigError("lakehouse must be provided.")

        for lakehouse in self.get_lakehouses():
            if lakehouse["displayName"] == self._credentials.lakehouse_name:
                self._lakehouse_id = lakehouse["id"]
                assert self._lakehouse_id is not None
                return self._lakehouse_id

        raise dbt_common.exceptions.DbtRuntimeError(
            f"No Lakehouse found with name {self._credentials.lakehouse_name}"
        )

    def get_warehouse_id(self) -> str:
        if self._warehouse_id is not None:
            return self._warehouse_id

        for warehouse in self.get_warehouses():
            if warehouse["displayName"] == self._credentials.database:
                self._warehouse_id = warehouse["id"]
                assert self._warehouse_id is not None
                return self._warehouse_id

        raise dbt_common.exceptions.DbtRuntimeError(
            f"No Data Warehouse found with name {self._credentials.database}"
        )

    def get_warehouse_snapshots(self) -> list[dict]:
        warehouse_id = self.get_warehouse_id()
        workspace_id = self.get_workspace_id()

        url = (
            f"{self._credentials.fabric_base_api_uri}/workspaces/{workspace_id}/warehousesnapshots"
        )
        snapshots = []

        while url is not None:
            response = self._api_get(url)
            for snapshot in response.json().get("value", []):
                parent_warehouse_id = snapshot.get("properties", {}).get("parentWarehouseId")
                if parent_warehouse_id == warehouse_id:
                    snapshots.append(snapshot)

            url = response.json().get("continuationUri", None)

        return snapshots

    def create_warehouse_snapshot(
        self, snapshot_name: str, description: str | None = None
    ) -> None:
        url = f"{self._credentials.fabric_base_api_uri}/workspaces/{self.get_workspace_id()}/warehousesnapshots"
        body = {
            "displayName": snapshot_name,
            "creationPayload": {"parentWarehouseId": self.get_warehouse_id()},
        }
        if description is not None:
            body["description"] = description

        response = self._api_post(
            url,
            body,
        )

        location_uri = response.headers.get("Location")
        if location_uri is not None and response.status_code == 202:
            self._warehouse_snapshot_operations[snapshot_name] = location_uri

    def update_warehouse_snapshot(
        self, snapshot_id: str, snapshot_name: str, description: str | None = None
    ) -> None:
        url = f"{self._credentials.fabric_base_api_uri}/workspaces/{self.get_workspace_id()}/warehousesnapshots/{snapshot_id}"
        body: dict[str, Any] = {"properties": {}}
        if description is not None:
            body["description"] = description
        response = self._api_patch(url, body)

        location_uri = response.headers.get("Location")
        if location_uri is not None and response.status_code == 202:
            self._warehouse_snapshot_operations[snapshot_name] = location_uri

    def wait_and_get_snapshot_id_from_operation(self, operation_uri: str) -> str:
        timer = time.time()
        while True:
            if time.time() - timer > self._WAREHOUSE_SNAPSHOT_TIMEOUT_SECONDS:
                raise dbt_common.exceptions.DbtRuntimeError(
                    f"Timed out waiting for Warehouse Snapshot operation to complete after {self._WAREHOUSE_SNAPSHOT_TIMEOUT_SECONDS} seconds."
                )

            response = self._api_get(operation_uri)
            operation_status = response.json().get("status", "Unknown")
            retry_sleep = int(response.headers.get("Retry-After", 5))

            if operation_status == "Succeeded":
                result_location = response.headers["Location"]
                result_response = self._api_get(result_location)
                return result_response.json()["id"]

            if operation_status not in ("NotStarted", "Running"):
                raise dbt_common.exceptions.DbtRuntimeError(
                    f"Warehouse Snapshot operation failed with status {operation_status}."
                )

            time.sleep(retry_sleep)

    def create_or_update_warehouse_snapshot(
        self, snapshot_name: str, description: str | None = None
    ) -> None:
        existing_snapshot_id = None

        snapshot_operation_uri = self._warehouse_snapshot_operations.get(snapshot_name)
        if snapshot_operation_uri is not None:
            existing_snapshot_id = self.wait_and_get_snapshot_id_from_operation(
                snapshot_operation_uri
            )
        else:
            all_snapshots = self.get_warehouse_snapshots()
            for snapshot in all_snapshots:
                if snapshot["displayName"] == snapshot_name:
                    existing_snapshot_id = snapshot["id"]
                    break

        if existing_snapshot_id is not None:
            self.update_warehouse_snapshot(existing_snapshot_id, snapshot_name, description)
        else:
            self.create_warehouse_snapshot(snapshot_name, description)

    def delete_warehouse_snapshot(self, snapshot_name: str) -> None:
        for snapshot in self.get_warehouse_snapshots():
            if snapshot["displayName"] == snapshot_name:
                self._api_delete(
                    f"{self._credentials.fabric_base_api_uri}/workspaces/{self.get_workspace_id()}/warehousesnapshots/{snapshot['id']}"
                )

    def get_livy_base_api_uri(self) -> str:
        workspace_id = self.get_workspace_id()
        lakehouse_id = self.get_lakehouse_id()
        return f"{self._credentials.fabric_base_api_uri}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/livyapi/versions/{self._LIVY_API_VERSION}"

    def get_existing_livy_session(self) -> str | None:
        url = self.get_livy_base_api_uri() + "/sessions"
        response = self._api_get(url)
        sessions = response.json().get("items", [])
        for session in sessions:
            if session["name"] == self._credentials.livy_session_name and session["livyState"] in (
                "idle",
                "starting",
                "running",
                "busy",
            ):
                return session["id"]
        return None

    def initialize_livy_session(self) -> str:
        url = self.get_livy_base_api_uri() + "/sessions"
        response = self._api_post(url, {"name": self._credentials.livy_session_name, "ttl": "30s"})
        time.sleep(10)  # give it a moment to initialize before we try to use it
        return response.json()["id"]

    def get_livy_session_id(self) -> str:
        if self._livy_session_id is None:
            with _livy_session_thread_lock:
                self._livy_session_id = (
                    self.get_existing_livy_session() or self.initialize_livy_session()
                )
        return self._livy_session_id

    def get_livy_session_base_uri(self) -> str:
        return self.get_livy_base_api_uri() + f"/sessions/{self.get_livy_session_id()}"

    def get_livy_session_state(self) -> str:
        response = self._api_get(self.get_livy_session_base_uri())
        return response.json().get("state", "unknown")

    def get_livy_statement(self, statement_id: int) -> dict[str, Any]:
        url = self.get_livy_session_base_uri() + f"/statements/{statement_id}"
        response = self._api_get(url)
        return response.json()

    def submit_livy_python_statement(self, code: str) -> int:
        url = self.get_livy_session_base_uri() + "/statements"
        response = self._api_post(url, {"code": code, "kind": "pyspark"})
        return response.json()["id"]

    def submit_livy_sql_statement(self, code: str) -> int:
        url = self.get_livy_session_base_uri() + "/statements"
        response = self._api_post(url, {"code": code, "kind": "sql"})
        return response.json()["id"]

    def cancel_livy_statement(self, statement_id: int) -> str:
        url = self.get_livy_session_base_uri() + f"/statements/{statement_id}/cancel"
        response = self._api_post(url, {})
        return response.json()["msg"]
