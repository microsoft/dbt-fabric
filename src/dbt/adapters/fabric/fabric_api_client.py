import urllib.parse

import dbt_common.exceptions
import requests

import dbt
from dbt.adapters.fabric.fabric_credentials import FabricCredentials
from dbt.adapters.fabric.fabric_token_provider import FabricTokenProvider


class FabricApiClient:
    _fabric_token_provider = None
    _warehouse_connection_string = None
    _workspace_id = None
    _lakehouse_id = None

    @classmethod
    def get_fabric_token_provider(cls, credentials: FabricCredentials) -> FabricTokenProvider:
        if cls._fabric_token_provider is None:
            cls._fabric_token_provider = FabricTokenProvider(credentials)
        return cls._fabric_token_provider

    @classmethod
    def get_workspace_id(cls, credentials: FabricCredentials) -> str:
        if cls._workspace_id is not None:
            return cls._workspace_id
        if credentials.workspace_id:
            return credentials.workspace_id
        if not credentials.workspace_name:
            raise dbt_common.exceptions.DbtConfigError(
                "Either workspace_id or workspace_name must be provided."
            )
        token = cls.get_fabric_token_provider(credentials).get_api_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        query_param = f"name eq '{credentials.workspace_name}'"
        query_param_encoded = urllib.parse.quote_plus(query_param)
        url = f"https://api.powerbi.com/v1.0/myorg/groups?$filter={query_param_encoded}"
        response = requests.get(url, headers=headers)
        if not response.status_code == 200:
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Failed to fetch workspace ID for {credentials.workspace_name}: {response.text}"
            )
        workspaces = response.json().get("value", [])
        if len(workspaces) == 0:
            raise dbt_common.exceptions.DbtRuntimeError(
                f"No workspace found with name {credentials.workspace_name}"
            )
        cls._workspace_id = workspaces[0]["id"]
        return cls._workspace_id

    @classmethod
    def get_warehouse_connection_string(cls, credentials: FabricCredentials) -> str:
        if cls._warehouse_connection_string is not None:
            return cls._warehouse_connection_string

        token = cls.get_fabric_token_provider(credentials).get_api_token()
        workspace_id = cls.get_workspace_id(credentials)

        # first we try to find it in any warehouse (they all have the same connection string)
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        response = requests.get(url, headers=headers)
        if not response.status_code == 200:
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Failed to fetch warehouse connection string: {response.text}"
            )
        warehouses = response.json().get("value", [])
        if len(warehouses) > 0:
            cls._warehouse_connection_string = warehouses[0]["properties"]["connectionString"]
            return cls._warehouse_connection_string

        # then we try to find it in any lakehouse (also have the same connection string)
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
        response = requests.get(url, headers=headers)
        if not response.status_code == 200:
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Failed to fetch lakehouse connection string: {response.text}"
            )
        lakehouses = response.json().get("value", [])
        if len(lakehouses) > 0:
            cls._warehouse_connection_string = lakehouses[0]["properties"][
                "sqlEndpointProperties"
            ]["connectionString"]
            return cls._warehouse_connection_string

        raise dbt_common.exceptions.DbtRuntimeError(
            f"No warehouses or lakehouses found in workspace {workspace_id}"
        )

    @classmethod
    def get_lakehouse_id(cls, credentials: FabricCredentials) -> str:
        if cls._lakehouse_id is not None:
            return cls._lakehouse_id
        if credentials.lakehouse_id:
            return credentials.lakehouse_id
        if not credentials.lakehouse_name:
            raise dbt_common.exceptions.DbtConfigError(
                "Either lakehouse_id or lakehouse_name must be provided."
            )
        token = cls.get_fabric_token_provider(credentials).get_api_token()
        workspace_id = cls.get_workspace_id(credentials)
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
        while url is not None:
            response = requests.get(url, headers=headers)
            if not response.status_code == 200:
                raise dbt_common.exceptions.DbtRuntimeError(
                    f"Failed to fetch lakehouse ID for {credentials.lakehouse_name}: {response.text}"
                )
            lakehouses = response.json().get("value", [])
            for lakehouse in lakehouses:
                if lakehouse["displayName"] == credentials.lakehouse_name:
                    cls._lakehouse_id = lakehouse["id"]
                    return cls._lakehouse_id
            url = response.json().get("continuationUri", None)
        raise dbt_common.exceptions.DbtRuntimeError(
            f"No lakehouse found with name {credentials.lakehouse_name} in workspace {workspace_id}"
        )
