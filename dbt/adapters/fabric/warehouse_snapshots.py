from typing import Any, Dict, List, Optional

import requests  # type: ignore
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("fabric")


class WarehouseSnapshotManager:
    """Manager for Microsoft Fabric warehouse snapshots."""

    def __init__(
        self,
        workspace_id: Optional[str],
        access_token: str,
        base_url: Optional[str] = "https://api.fabric.microsoft.com/v1",
    ):
        self.workspace_id = workspace_id
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

    # CRUD Methods
    def list_warehouses(self) -> List[Dict[str, Any]]:
        """List all warehouses in the workspace."""
        try:
            url = f"{self.base_url}/workspaces/{self.workspace_id}/warehouses"

            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            data = response.json()
            warehouses = data.get("value", [])

            logger.debug(f"Found {len(warehouses)} warehouses in workspace {self.workspace_id}")
            return warehouses

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list warehouses: {str(e)}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response body: {e.response.text}")
            raise e
        except Exception as e:
            logger.error(f"Unexpected error listing warehouses: {str(e)}")
            raise e

    def get_warehouse_id_by_name(
        self, warehouse_list: List[Dict[str, Any]], warehouse_name: str
    ) -> Optional[str]:
        """Get warehouse ID by warehouse name."""
        try:
            for warehouse in warehouse_list:
                if (
                    warehouse.get("displayName") == warehouse_name
                    and warehouse.get("type") == "Warehouse"
                ):
                    warehouse_id = warehouse.get("id")
                    return warehouse_id
            return None

        except Exception as e:
            logger.error(f"Error searching for warehouse '{warehouse_name}': {str(e)}")
            return None

    def list_warehouse_snapshots(self) -> List[Dict[str, Any]]:
        """List all warehouse snapshots in the workspace."""
        try:
            url = f"{self.base_url}/workspaces/{self.workspace_id}/warehousesnapshots"

            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            data = response.json()
            snapshots = data.get("value", [])

            logger.debug(
                f"Found {len(snapshots)} warehouse snapshots in workspace {self.workspace_id}"
            )
            return snapshots

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list warehouse snapshots: {str(e)}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response body: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing warehouse snapshots: {str(e)}")
            raise

    def _poll_operation_status(
        self, operation_url: str, operation_id: str, max_retries: int = 5, retry_delay: int = 30
    ) -> Dict[str, Any]:
        """Poll operation status until completion."""
        try:
            import time

            for attempt in range(max_retries):
                logger.debug(
                    f"Polling operation {operation_id}, attempt {attempt + 1}/{max_retries}"
                )

                response = requests.get(operation_url, headers=self.headers)

                if response.status_code == 201:
                    # Operation is complete - get the result
                    logger.debug(f"Operation {operation_id} completed (201)")

                    # Fetch the actual result from /result endpoint
                    result_url = f"{operation_url}/result"
                    result_response = requests.get(result_url, headers=self.headers)
                    result_response.raise_for_status()

                    result = result_response.json()
                    logger.debug(f"Operation {operation_id} result retrieved successfully")
                    return result

                elif response.status_code == 200:
                    # Operation status available - check the status field
                    response.raise_for_status()
                    operation_status = response.json()
                    status = operation_status.get("status", "").lower()

                    if status == "succeeded":
                        logger.info(f"Operation {operation_id} succeeded")
                        # Try to get result from /result endpoint
                        try:
                            result_url = f"{operation_url}/result"
                            result_response = requests.get(result_url, headers=self.headers)
                            if result_response.status_code == 200:
                                return result_response.json()
                            else:
                                # Return the operation status if result endpoint not available
                                return operation_status
                        except Exception:
                            return operation_status

                    elif status == "failed":
                        error_msg = operation_status.get("error", {}).get(
                            "message", "Unknown error"
                        )
                        error_code = operation_status.get("error", {}).get("code", "Unknown")
                        raise Exception(
                            f"Operation {operation_id} failed: {error_code} - {error_msg}"
                        )

                    elif status in ["running", "notstarted"]:
                        logger.info(f"Operation {operation_id} status: {status}")
                        time.sleep(retry_delay)

                    elif status == "undefined":
                        logger.warning(
                            f"Operation {operation_id} status is undefined, continuing to poll..."
                        )
                        time.sleep(retry_delay)

                    else:
                        logger.warning(
                            f"Unknown operation status '{status}' for operation {operation_id}"
                        )
                        time.sleep(retry_delay)

                elif response.status_code == 202:
                    # Operation still accepted/in progress
                    logger.info(f"Operation {operation_id} still in progress (202)")
                    time.sleep(retry_delay)

                else:
                    response.raise_for_status()

            raise Exception(f"Operation {operation_id} timed out after {max_retries} attempts")

        except Exception as e:
            logger.error(f"Failed to poll operation {operation_id}: {str(e)}")
            raise e

    def create_warehouse_snapshot(self, warehouse_id: str, snapshot_name: str) -> Dict[str, Any]:
        """Create a new warehouse snapshot."""
        try:
            from datetime import datetime, timezone

            url = f"{self.base_url}/workspaces/{self.workspace_id}/warehousesnapshots"

            # Get current UTC time for snapshot
            current_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            # Build request payload
            payload = {
                "displayName": snapshot_name,
                "description": f"Warehouse snapshot created at {current_utc}",
                "creationPayload": {
                    "parentWarehouseId": warehouse_id,
                    "snapshotDateTime": current_utc,
                },
            }

            response = requests.post(url, headers=self.headers, json=payload)

            if response.status_code == 201:
                # Synchronous success
                result = response.json()
                logger.info(
                    f"Successfully created snapshot '{snapshot_name}' with ID: {result.get('id')}"
                )
                return result

            elif response.status_code == 202:
                # Asynchronous operation - need to poll
                location = response.headers.get("Location")
                operation_id = response.headers.get("x-ms-operation-id")
                retry_after = int(response.headers.get("Retry-After", 30))

                logger.info(
                    f"Snapshot creation accepted. Operation ID: {operation_id}, polling every {retry_after} seconds"
                )

                if not location:
                    raise Exception("202 response received but no Location header found")

                # Poll the operation status
                operation_result = self._poll_operation_status(
                    location, operation_id, retry_delay=retry_after
                )

                # Get the actual snapshot details from the operation result
                snapshot_id = operation_result.get("resourceId") or operation_result.get(
                    "result", {}
                ).get("id")
                if snapshot_id:
                    logger.info(
                        f"Successfully created snapshot '{snapshot_name}' with ID: {snapshot_id}"
                    )
                    return {
                        "snapshot_id": snapshot_id,
                        "displayName": snapshot_name,
                        "operation_type": "create",
                        "operation": operation_result,
                        "warehouse_id": warehouse_id,
                    }
                else:
                    return operation_result
            else:
                response.raise_for_status()
                # This line should never be reached since raise_for_status() will raise an exception
                # for non-2xx status codes, but we need it to satisfy mypy
                raise Exception(f"Unexpected response status: {response.status_code}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create warehouse snapshot '{snapshot_name}': {str(e)}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response body: {e.response.text}")
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error creating warehouse snapshot '{snapshot_name}': {str(e)}"
            )
            raise

    def update_warehouse_snapshot(self, snapshot_id: str) -> Dict[str, Any]:
        """Update existing warehouse snapshot."""
        try:
            from datetime import datetime, timezone

            url = (
                f"{self.base_url}/workspaces/{self.workspace_id}/warehousesnapshots/{snapshot_id}"
            )

            # Build update payload - only include fields that are provided
            payload: Dict[str, Any] = {}

            # Default description with current timestamp
            current_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            payload["description"] = f"Warehouse snapshot updated at {current_utc}"

            properties: Dict[str, str] = {}
            properties["snapshotDateTime"] = current_utc
            payload["properties"] = properties

            response = requests.patch(url, headers=self.headers, json=payload)
            response.raise_for_status()

            result = response.json()
            logger.info(f"Successfully updated snapshot '{snapshot_id}'")
            # Return consistent format like create method
            return {
                "snapshot_id": result.get("id", snapshot_id),
                "displayName": result.get("displayName"),
                "warehouse_id": result.get("properties", {}).get("parentWarehouseId"),
                "operation_type": "update",
                "operation": result,
            }

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to update warehouse snapshot '{snapshot_id}': {str(e)}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response body: {e.response.text}")
            raise e
        except Exception as e:
            logger.error(f"Unexpected error updating warehouse snapshot '{snapshot_id}': {str(e)}")
            raise e

    def delete_warehouse_snapshot(self, snapshot_id: str) -> bool:
        """Delete warehouse snapshot."""
        return True

    def find_snapshot_by_warehouse_and_name(
        self, warehouse_snapshots: List[Dict[str, Any]], warehouse_id: str, snapshot_name: str
    ) -> Optional[str]:
        """Find snapshot by warehouse ID and snapshot name."""
        try:
            for snapshot in warehouse_snapshots:
                # Check if this snapshot belongs to the specified warehouse
                parent_warehouse_id = snapshot.get("properties", {}).get("parentWarehouseId")
                snapshot_display_name = snapshot.get("displayName")

                if (
                    parent_warehouse_id == warehouse_id
                    and snapshot_display_name == snapshot_name
                    and snapshot.get("type") == "WarehouseSnapshot"
                ):
                    logger.info(
                        f"Found existing snapshot '{snapshot_name}' with ID: {snapshot.get('id')}"
                    )
                    return snapshot.get("id")

            logger.info(
                f"No snapshot found with name '{snapshot_name}' for warehouse '{warehouse_id}'"
            )
            return None

        except Exception as e:
            logger.error(
                f"Error searching for snapshot '{snapshot_name}' in warehouse '{warehouse_id}': {str(e)}"
            )
            return None

    # Main Orchestration Flow
    def orchestrate_snapshot_management(
        self, warehouse_name: str, snapshot_name: str
    ) -> Dict[str, Any]:
        """
        Main orchestration flow for warehouse snapshot management.

        Flow:
        1. List warehouses by workspace ID
        2. Get warehouse ID by warehouse name
        3. List all warehouse snapshots in workspace
        4. Find snapshot by warehouse ID and snapshot name
        5. If exists -> update, if not -> create

        Args:
            warehouse_name: Name of the warehouse
            snapshot_name: Name of the snapshot
            snapshot_config: Configuration for snapshot creation/update

        Returns:
            Dict containing operation result and snapshot details
        """
        try:
            # Step 1: Get warehouse ID by name
            logger.info(f"Looking up warehouse: {warehouse_name}")
            warehouse_list = self.list_warehouses()
            warehouse_id = self.get_warehouse_id_by_name(warehouse_list, warehouse_name)

            if not warehouse_id:
                raise ValueError(
                    f"Warehouse '{warehouse_name}' not found in workspace {self.workspace_id}"
                )
            logger.info(f"Found warehouse ID: {warehouse_id}")

            # Step 2: Check if snapshot already exists
            warehouse_snapshots = self.list_warehouse_snapshots()

            if len(warehouse_snapshots) > 0:
                logger.info(f"Checking for existing snapshot: {snapshot_name}")
                existing_snapshot_id = self.find_snapshot_by_warehouse_and_name(
                    warehouse_snapshots, warehouse_id, snapshot_name
                )
                if existing_snapshot_id:
                    result = self.update_warehouse_snapshot(existing_snapshot_id)
                else:
                    logger.info(f"Creating new snapshot: {snapshot_name}")
                    result = self.create_warehouse_snapshot(warehouse_id, snapshot_name)
            else:
                logger.info(f"Creating new snapshot: {snapshot_name}")
                result = self.create_warehouse_snapshot(
                    warehouse_id,
                    snapshot_name,
                )

            return result

        except Exception as e:
            logger.error(f"Snapshot management failed: {str(e)}")
            raise
