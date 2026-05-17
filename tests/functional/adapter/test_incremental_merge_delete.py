"""
Functional tests for dbt-fabric incremental merge delete features.

Two scenarios:
  1. delete_not_matched_by_source — WHEN NOT MATCHED BY SOURCE THEN DELETE inside MERGE.
     Deletes rows in target whose unique_key is absent from the source relation.
     Use when the incremental model returns the complete current dataset.

  2. delete_condition — separate DELETE after MERGE, using a SQL expression.
     Deletes rows that appear in source and satisfy the condition (e.g. a soft-delete flag).
     Use when the source carries a soft-delete column.
"""

import pytest
from dbt.tests.util import run_dbt

# ---------------------------------------------------------------------------
# Shared seed data
# ---------------------------------------------------------------------------

_SEED_CSV = """id,name,is_deleted
1,Alice,0
2,Bob,0
3,Carol,0
"""

# ---------------------------------------------------------------------------
# Option A: delete_not_matched_by_source
# ---------------------------------------------------------------------------

# First run returns all 3 rows; second run omits id=3 so it gets deleted.
_MODEL_DELETE_NOT_MATCHED = """
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='id',
        delete_not_matched_by_source=true
    )
}}

select id, name
from {{ ref('seed_merge_delete') }}
{% if is_incremental() %}
-- On the incremental run, exclude id=3 so it is deleted from the target
where id != 3
{% endif %}
"""

# ---------------------------------------------------------------------------
# Option B: delete_condition (soft-delete column)
# ---------------------------------------------------------------------------

# First run: all 3 rows, none deleted.
# Second run: same 3 rows but row id=2 now has is_deleted=1, so it gets deleted.
_MODEL_DELETE_CONDITION = """
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='id',
        delete_condition='DBT_INTERNAL_SOURCE.is_deleted = 1'
    )
}}

select id, name, is_deleted
from {{ ref('seed_merge_delete') }}
"""

_SEED_UPDATED_CSV = """id,name,is_deleted
1,Alice,0
2,Bob,1
3,Carol,0
"""


# ---------------------------------------------------------------------------
# Test: delete_not_matched_by_source
# ---------------------------------------------------------------------------


class TestIncrementalMergeDeleteNotMatchedBySource:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_merge_delete.csv": _SEED_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_delete_not_matched.sql": _MODEL_DELETE_NOT_MATCHED}

    def test_delete_not_matched_by_source(self, project):
        # Run 1 — full refresh, all 3 rows land in target
        run_dbt(["seed"])
        run_dbt(["run", "--select", "incremental_delete_not_matched", "--full-refresh"])

        result = project.run_sql(
            "select count(*) as cnt from {schema}.incremental_delete_not_matched",
            fetch="one",
        )
        assert result[0] == 3, f"Expected 3 rows after initial run, got {result[0]}"

        # Run 2 — incremental run: source omits id=3, so it should be deleted from target
        run_dbt(["run", "--select", "incremental_delete_not_matched"])

        result = project.run_sql(
            "select count(*) as cnt from {schema}.incremental_delete_not_matched",
            fetch="one",
        )
        assert (
            result[0] == 2
        ), f"Expected 2 rows after incremental run (id=3 deleted), got {result[0]}"

        # Confirm id=3 is gone and id=1, id=2 remain
        rows = project.run_sql(
            "select id from {schema}.incremental_delete_not_matched order by id",
            fetch="all",
        )
        ids = [r[0] for r in rows]
        assert ids == [1, 2], f"Expected ids [1, 2], got {ids}"


# ---------------------------------------------------------------------------
# Test: delete_condition (soft-delete column)
# ---------------------------------------------------------------------------


class TestIncrementalMergeDeleteCondition:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_merge_delete.csv": _SEED_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_delete_condition.sql": _MODEL_DELETE_CONDITION}

    def test_delete_condition(self, project):
        # Run 1 — full refresh, all 3 rows, none marked deleted
        run_dbt(["seed"])
        run_dbt(["run", "--select", "incremental_delete_condition", "--full-refresh"])

        result = project.run_sql(
            "select count(*) as cnt from {schema}.incremental_delete_condition",
            fetch="one",
        )
        assert result[0] == 3, f"Expected 3 rows after initial run, got {result[0]}"

        # Update seed: mark id=2 as deleted
        project.run_sql("update {schema}.seed_merge_delete set is_deleted = 1 where id = 2")

        # Run 2 — incremental run: id=2 has is_deleted=1, should be deleted from target
        run_dbt(["run", "--select", "incremental_delete_condition"])

        result = project.run_sql(
            "select count(*) as cnt from {schema}.incremental_delete_condition",
            fetch="one",
        )
        assert (
            result[0] == 2
        ), f"Expected 2 rows after incremental run (id=2 deleted), got {result[0]}"

        # Confirm id=2 is gone and id=1, id=3 remain
        rows = project.run_sql(
            "select id from {schema}.incremental_delete_condition order by id",
            fetch="all",
        )
        ids = [r[0] for r in rows]
        assert ids == [1, 3], f"Expected ids [1, 3], got {ids}"
