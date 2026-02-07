import token

import pytest
from requests import api

from dbt.adapters.fabric import FabricCredentials
from dbt.adapters.fabric.fabric_api_client import FabricApiClient
from dbt.adapters.fabric.fabric_token_provider import FabricTokenProvider
from dbt.tests.util import run_dbt


class TestWarehouseSnapshots:
    @pytest.fixture(scope="class")
    def snapshot_name(self, unique_schema: str):
        return f"{unique_schema}_snapshot"

    @pytest.fixture(scope="class")
    def models(self):
        return {"simple.sql": "select 1 as id"}

    @pytest.fixture(scope="class")
    def credentials(self, adapter) -> FabricCredentials:
        return adapter.config.credentials

    @pytest.fixture(scope="class")
    def project_config_update(self, snapshot_name: str):
        return {
            "on-run-start": [
                f"{{{{ create_or_update_fabric_warehouse_snapshot('{snapshot_name}') }}}}"
            ],
            "on-run-end": [
                f"{{{{ create_or_update_fabric_warehouse_snapshot('{snapshot_name}') }}}}"
            ],
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, credentials: FabricCredentials, snapshot_name: str):
        token_provider = FabricTokenProvider(credentials)
        api_client = FabricApiClient.create(credentials, token_provider)
        api_client.delete_warehouse_snapshot(snapshot_name)
        yield
        api_client.delete_warehouse_snapshot(snapshot_name)

    def test_warehouse_snapshots(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3
