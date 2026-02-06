import pytest

from dbt.tests.util import run_dbt


class TestWarehouseSnapshots:
    @pytest.fixture(scope="class")
    def models(self):
        return {"simple.sql": "select 1 as id"}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "on-run-start": [
                "{{ create_or_update_fabric_warehouse_snapshot('dbt_test_snapshot') }}"
            ],
            "on-run-end": [
                "{{ create_or_update_fabric_warehouse_snapshot('dbt_test_snapshot') }}"
            ],
        }

    def test_warehouse_snapshots(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3
