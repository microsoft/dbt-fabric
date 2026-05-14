import time
import urllib.parse
import uuid

import requests
from azure.identity import AzureCliCredential


class FabricTestItemManager:
    """Creates and manages temporary Fabric items for isolated test runs.

    Each call to create_warehouse / create_lakehouse fires a non-blocking
    POST.  Call wait_for_all() once to block until every pending item is
    provisioned.  Use delete_all() (or the matching delete_* helpers) to
    tear everything down.
    """

    FABRIC_API = "https://api.fabric.microsoft.com/v1"
    POWERBI_API = "https://api.powerbi.com/v1.0"
    PROVISION_TIMEOUT = 600
    MAX_RETRIES = 10

    def __init__(self, workspace_name: str) -> None:
        self._workspace_name = workspace_name
        self._credential = AzureCliCredential()
        self._workspace_id: str | None = None
        self._pending: list[tuple[str, str, str]] = []
        self._items: dict[str, tuple[str, str]] = {}

    @staticmethod
    def generate_suffix() -> str:
        return uuid.uuid4().hex[:8]

    def _get_token(self) -> str:
        return self._credential.get_token(
            "https://analysis.windows.net/powerbi/api/.default"
        ).token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, url: str, body: dict | None = None) -> requests.Response:
        for attempt in range(self.MAX_RETRIES):
            resp = requests.request(method, url, json=body, headers=self._headers())
            if resp.status_code != 429:
                return resp
            delay = int(resp.headers.get("Retry-After", 5))
            if attempt == self.MAX_RETRIES - 1:
                raise RuntimeError(
                    f"Fabric API rate limit (429) not resolved after "
                    f"{self.MAX_RETRIES} retries on {method.upper()} {url}"
                )
            time.sleep(delay)
        raise RuntimeError("Unreachable")

    def get_workspace_id(self) -> str:
        if self._workspace_id:
            return self._workspace_id

        query = urllib.parse.quote_plus(f"name eq '{self._workspace_name}'")
        resp = self._request("get", f"{self.POWERBI_API}/myorg/groups?$filter={query}")
        resp.raise_for_status()
        workspaces = resp.json().get("value", [])
        if not workspaces:
            raise RuntimeError(f"No workspace found: {self._workspace_name}")
        self._workspace_id = workspaces[0]["id"]
        assert self._workspace_id is not None
        return self._workspace_id

    def _create_item(self, item_type: str, url: str, name: str, body: dict) -> None:
        resp = self._request("post", url, body)

        if resp.status_code == 201:
            self._items[name] = (item_type, resp.json()["id"])
        elif resp.status_code == 202:
            location = resp.headers.get("Location")
            if not location:
                raise RuntimeError(
                    f"Fabric returned 202 Accepted for {item_type} '{name}' "
                    f"but no Location header for polling"
                )
            self._pending.append((item_type, name, location))
        else:
            resp.raise_for_status()

    def create_warehouse(self, name: str) -> None:
        ws = self.get_workspace_id()
        url = f"{self.FABRIC_API}/workspaces/{ws}/warehouses"
        self._create_item("warehouse", url, name, {"displayName": name})

    def create_lakehouse(self, name: str) -> None:
        ws = self.get_workspace_id()
        url = f"{self.FABRIC_API}/workspaces/{ws}/lakehouses"
        body: dict = {
            "displayName": name,
            "creationPayload": {"enableSchemas": True},
        }
        self._create_item("lakehouse", url, name, body)

    def wait_for_all(self) -> None:
        for item_type, name, location_url in self._pending:
            start = time.time()
            while True:
                elapsed = time.time() - start
                if elapsed > self.PROVISION_TIMEOUT:
                    raise RuntimeError(
                        f"Timed out after {self.PROVISION_TIMEOUT}s "
                        f"provisioning {item_type} '{name}'"
                    )

                resp = self._request("get", location_url)
                status = resp.json().get("status", "Unknown")

                if status == "Succeeded":
                    result_location = resp.headers.get("Location")
                    if not result_location:
                        raise RuntimeError(
                            f"Provisioning succeeded for {item_type} '{name}' "
                            f"but response has no Location header to retrieve item id"
                        )
                    result = self._request("get", result_location)
                    self._items[name] = (item_type, result.json()["id"])
                    break

                if status not in ("NotStarted", "Running"):
                    raise RuntimeError(f"Failed to provision {item_type} '{name}': {status}")

                time.sleep(int(resp.headers.get("Retry-After", 5)))

        self._pending.clear()

    def delete_item(self, name: str) -> None:
        entry = self._items.get(name)
        if not entry:
            return
        item_type, item_id = entry
        ws = self.get_workspace_id()

        if item_type == "warehouse":
            url = f"{self.FABRIC_API}/workspaces/{ws}/warehouses/{item_id}"
        elif item_type == "lakehouse":
            url = f"{self.FABRIC_API}/workspaces/{ws}/lakehouses/{item_id}"
        else:
            return

        resp = self._request("delete", url)
        resp.raise_for_status()

    def delete_all(self) -> None:
        for name in list(self._items):
            self.delete_item(name)
