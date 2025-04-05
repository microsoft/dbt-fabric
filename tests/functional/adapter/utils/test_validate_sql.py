import pytest

from dbt.tests.adapter.utils.test_validate_sql import BaseValidateSqlMethod


@pytest.mark.skip("Fabric does not support SHOWPLAN at the moment")
class TestValidateSqlMethodFabric(BaseValidateSqlMethod):
    @pytest.fixture(scope="class")
    def valid_sql(self) -> str:
        return "select 1 as id"
