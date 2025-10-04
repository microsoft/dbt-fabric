import pytest

from dbt.tests.adapter.empty._models import schema_sources_yml
from dbt.tests.adapter.empty.test_empty import BaseTestEmpty, BaseTestEmptyInlineSourceRef


class TestFabricEmpty(BaseTestEmpty):
    pass


class TestFabricEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    model_inline_sql = """
        select * from {{ source('seed_sources', 'raw_source') }} as raw_source
        """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": "select * from {{ source('seed_sources', 'raw_source') }}",
            "sources.yml": schema_sources_yml,
        }
