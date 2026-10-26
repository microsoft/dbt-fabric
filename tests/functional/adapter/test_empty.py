import pytest

from dbt.tests.adapter.empty import _models
from dbt.tests.adapter.empty.test_empty import (
    BaseTestEmpty,
    BaseTestEmptyInlineSourceRef,
    BaseTestEmptySeedFlag,
)
from dbt.tests.util import run_dbt


class TestEmpty(BaseTestEmpty):
    pass


class TestEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    @pytest.fixture(scope="class")
    def models(self):
        # T-SQL requires one alias for the derived table, not both dbt's and the model's.
        return {
            "sources.yml": _models.schema_sources_yml,
            "model.sql": """
                select * from
                {{ source('seed_sources', 'raw_source').incorporate(require_alias=False) }}
                as raw_source
            """,
            "ref_model.sql": """
                select * from {{ ref('raw_source').incorporate(require_alias=False) }}
                as raw_source
            """,
        }

    def test_run_with_empty(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        self.assert_row_count(project, "model", 1)
        self.assert_row_count(project, "ref_model", 1)

        run_dbt(["run", "--empty"])
        self.assert_row_count(project, "model", 0)
        self.assert_row_count(project, "ref_model", 0)


class TestEmptySeedFlag(BaseTestEmptySeedFlag):
    def test_seed_empty_clears_existing_rows(self, project):
        run_dbt(["seed"])
        self.assert_row_count(project, "raw_seed", 2)

        run_dbt(["seed", "--empty"])
        self.assert_row_count(project, "raw_seed", 0)

        run_dbt(["seed"])
        self.assert_row_count(project, "raw_seed", 2)
