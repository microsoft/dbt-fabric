import re
import urllib.parse
from typing import Any, Self

import dbt_common.exceptions
import requests

from dbt.adapters.fabric.fabric_credentials import FabricCredentials
from dbt.adapters.fabric.fabric_token_provider import FabricTokenProvider


class FabricApiClient:
    _LIVY_API_VERSION = "2023-12-01"
    _instance: Self | None = None

    def __init__(
        self, credentials: FabricCredentials, token_provider: FabricTokenProvider
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

    @classmethod
    def create(cls, credentials: FabricCredentials, token_provider: FabricTokenProvider) -> Self:
        if cls._instance is None:
            cls._instance = FabricApiClient(credentials, token_provider)
        return cls._instance

    def _get_auth_headers(self) -> dict[str, str]:
        token = self._token_provider.get_api_token()
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

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
        url = (
            f"{self._credentials.powerbi_base_api_uri}/myorg/groups?$filter={query_param_encoded}"
        )
        response = requests.get(url, headers=self._get_auth_headers())
        if not response.status_code == 200:
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Failed to fetch workspace ID for {self._credentials.workspace_name}: {response.text}"
            )
        workspaces = response.json().get("value", [])
        if len(workspaces) == 0:
            raise dbt_common.exceptions.DbtRuntimeError(
                f"No workspace found with name {self._credentials.workspace_name}"
            )
        self._workspace_id = workspaces[0]["id"]
        return self._workspace_id

    def get_warehouses(self, fetch_all: bool = True) -> list[dict]:
        if self._cached_warehouses is not None:
            return self._cached_warehouses

        workspace_id = self.get_workspace_id()

        url = f"{self._credentials.fabric_base_api_uri}/workspaces/{workspace_id}/warehouses"
        warehouses = []

        while url is not None:
            response = requests.get(url, headers=self._get_auth_headers())
            if not response.status_code == 200:
                raise dbt_common.exceptions.DbtRuntimeError(
                    f"Failed to retrieve Warehouses from Fabric API: {response.text}"
                )
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
            response = requests.get(url, headers=self._get_auth_headers())
            if not response.status_code == 200:
                raise dbt_common.exceptions.DbtRuntimeError(
                    f"Failed to retrieve Lakehouses from Fabric API: {response.text}"
                )
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
            return self._warehouse_connection_string

        # then we try to find it in any lakehouse (also have the same connection string)
        lakehouses = self.get_lakehouses(fetch_all=False)
        if len(lakehouses) > 0:
            self._warehouse_connection_string = lakehouses[0]["properties"][
                "sqlEndpointProperties"
            ]["connectionString"]
            return self._warehouse_connection_string

        raise dbt_common.exceptions.DbtRuntimeError(
            f"No Data Warehouses or Lakehouses found in workspace"
        )

    def get_lakehouse_id(self) -> str:
        if self._lakehouse_id is not None:
            return self._lakehouse_id
        if self._credentials.lakehouse_id:
            return self._credentials.lakehouse_id
        if not self._credentials.lakehouse_name:
            raise dbt_common.exceptions.DbtConfigError(
                "Either lakehouse_id or lakehouse_name must be provided."
            )

        for lakehouse in self.get_lakehouses(self._credentials):
            if lakehouse["displayName"] == self._credentials.lakehouse_name:
                self._lakehouse_id = lakehouse["id"]
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
            response = requests.get(url, headers=self._get_auth_headers())
            if not response.status_code == 200:
                raise dbt_common.exceptions.DbtRuntimeError(
                    f"Failed to retrieve Data Warehouse Snapshots from Fabric API: {response.text}"
                )
            for snapshot in response.json().get("value", []):
                parent_warehouse_id = snapshot.get("properties", {}).get("parentWarehouseId")
                if parent_warehouse_id == warehouse_id:
                    snapshots.append(snapshot)

            url = response.json().get("continuationUri", None)

        return snapshots

    def get_livy_base_api_uri(self) -> str:
        workspace_id = self.get_workspace_id()
        lakehouse_id = self.get_lakehouse_id()
        return f"{self._credentials.fabric_base_api_uri}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/livyapi/versions/{self._LIVY_API_VERSION}"

    def get_existing_livy_session(self) -> str | None:
        url = self.get_livy_base_api_uri() + "/sessions"
        response = requests.get(url, headers=self._get_auth_headers())
        if not response.status_code == 200:
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Failed to retrieve Livy sessions from Fabric API: {response.text}"
            )
        sessions = response.json().get("value", [])
        for session in sessions:
            if session["name"] == "dbt-fabric" and session["livyState"] in (
                "idle",
                "starting",
                "running",
            ):
                return session["id"]
        return None

    def initialize_livy_session(self) -> str:
        url = self.get_livy_base_api_uri() + "/sessions"
        response = requests.post(
            url,
            headers=self._get_auth_headers(),
            json={"name": "dbt-fabric"},
        )
        if not response.status_code in (200, 201, 202):
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Failed to create Livy session via Fabric API: {response.text}"
            )
        return response.json()["id"]

    def get_livy_session_id(self) -> str:
        if self._livy_session_id is None:
            self._livy_session_id = (
                self.get_existing_livy_session() or self.initialize_livy_session()
            )
        return self._livy_session_id

    def get_livy_session_base_uri(self) -> str:
        return self.get_livy_base_api_uri() + f"/sessions/{self.get_livy_session_id()}"

    def get_livy_session_state(self) -> str:
        response = requests.get(self.get_livy_session_base_uri(), headers=self._get_auth_headers())
        if not response.status_code == 200:
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Failed to retrieve Livy session state from Fabric API: {response.text}"
            )
        return response.json().get("state", "unknown")

    def get_livy_statement(self, statement_id: str) -> dict[str, Any]:
        url = self.get_livy_session_base_uri() + f"/statements/{statement_id}"
        response = requests.get(url, headers=self._get_auth_headers())
        if not response.status_code == 200:
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Failed to retrieve Livy statement state from Fabric API: {response.text}"
            )
        return response.json()

    def submit_livy_statement(self, code: str) -> str:
        url = self.get_livy_session_base_uri() + "/statements"
        response = requests.post(
            url,
            headers=self._get_auth_headers(),
            json={"code": code, "kind": "pyspark"},
        )
        if not response.status_code in (200, 201, 202):
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Failed to submit Livy statement via Fabric API: {response.text}"
            )
        return response.json()["id"]
