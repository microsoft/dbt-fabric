import pytest

from dbt.tests.adapter.empty import _models
from dbt.tests.adapter.empty.test_empty import BaseTestEmpty, BaseTestEmptyInlineSourceRef

model_sql = """
select *
from {{ ref('model_input') }} m1
union all
select *
from {{ ref('ephemeral_model_input') }} m2 
union all
select *
from {{ source('seed_sources', 'raw_source') }} m3
"""


class TestFabricEmpty(BaseTestEmpty):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_input.sql": _models.model_input_sql,
            "ephemeral_model_input.sql": _models.ephemeral_model_input_sql,
            "model.sql": model_sql,
            "sources.yml": _models.schema_sources_yml,
        }


class TestFabricEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    pass
