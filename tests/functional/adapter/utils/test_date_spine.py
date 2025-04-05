import pytest

from dbt.tests.adapter.utils.test_date_spine import BaseDateSpine


class TestDateSpineFabric(BaseDateSpine):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+materialized": "table"  # no support for nested CTEs in Views yet
            },
        }
