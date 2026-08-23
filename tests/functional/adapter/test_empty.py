import pytest

from dbt.tests.adapter.empty._models import (
    ephemeral_model_input_sql,
    model_input_sql,
    schema_sources_yml,
)
from dbt.tests.adapter.empty.test_empty import BaseTestEmpty

model_sql_with_aliases = """
select *
from {{ ref('model_input') }} as model_input_alias
union all
select *
from {{ ref('ephemeral_model_input') }} as ephemeral_model_input_alias
union all
select *
from {{ source('seed_sources', 'raw_source') }} as raw_source_alias
"""


class TestEmpty(BaseTestEmpty):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_input.sql": model_input_sql,
            "ephemeral_model_input.sql": ephemeral_model_input_sql,
            "model.sql": model_sql_with_aliases,
            "sources.yml": schema_sources_yml,
        }
