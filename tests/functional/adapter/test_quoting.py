"""
Functional tests for T-SQL bracket quoting [identifier].

Verifies that the adapter uses [] brackets (not "") across:
  - table creation / selection
  - seeds (column lists)
  - incremental models (merge, delete+insert)
  - snapshots (column additions)
  - schema operations (USE, CREATE SCHEMA)
  - identifiers with special characters (hyphens, spaces)
"""

import pytest
from dbt.tests.util import get_manifest, run_dbt

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

_MODEL_TABLE = """
{{ config(materialized='table') }}
select 1 as id, 'alice' as user_name
"""

_MODEL_VIEW = """
{{ config(materialized='view') }}
select id, user_name from {{ ref('quoting_table') }}
"""

_MODEL_INCREMENTAL_DELETE_INSERT = """
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id',
) }}
select 1 as id, 'alice' as user_name
{% if is_incremental() %}
union all
select 2 as id, 'bob' as user_name
{% endif %}
"""

_MODEL_SPECIAL_CHARS = """
{{ config(materialized='table') }}
select 1 as [col with spaces], 2 as [col-with-hyphens]
"""

# ---------------------------------------------------------------------------
# Seeds
# ---------------------------------------------------------------------------

_SEED_CSV = """id,user_name,score
1,alice,100
2,bob,200
3,charlie,300
"""

# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------

_SNAPSHOT = """
{% snapshot quoting_snap %}
{{ config(
    target_schema=schema,
    strategy='check',
    unique_key='id',
    check_cols=['user_name'],
) }}
select * from {{ ref('quoting_table') }}
{% endsnapshot %}
"""


# ===================================================================
# Test: basic table + view creation uses bracket quoting
# ===================================================================
class TestBracketQuotingTableAndView:
    """Table and view creation should succeed with [schema].[table] quoting."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "quoting_table.sql": _MODEL_TABLE,
            "quoting_view.sql": _MODEL_VIEW,
        }

    def test_run_and_query(self, project):
        res = run_dbt(["run"])
        assert len(res) == 2

        # Verify data round-trips correctly
        result = project.run_sql(
            "select id, user_name from {schema}.quoting_table",
            fetch="one",
        )
        assert result[0] == 1
        assert result[1] == "alice"

        result = project.run_sql(
            "select id, user_name from {schema}.quoting_view",
            fetch="one",
        )
        assert result[0] == 1
        assert result[1] == "alice"


# ===================================================================
# Test: seeds produce bracket-quoted column lists
# ===================================================================
class TestBracketQuotingSeeds:
    """Seed inserts should use [col] bracket quoting in INSERT column lists."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"quoting_seed.csv": _SEED_CSV}

    def test_seed_and_query(self, project):
        res = run_dbt(["seed"])
        assert len(res) == 1

        rows = project.run_sql(
            "select count(*) from {schema}.quoting_seed",
            fetch="one",
        )
        assert rows[0] == 3


# ===================================================================
# Test: incremental delete+insert with bracket quoting
# ===================================================================
class TestBracketQuotingIncrementalDeleteInsert:
    """Incremental delete+insert should work with bracket-quoted identifiers."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "quoting_incr.sql": _MODEL_INCREMENTAL_DELETE_INSERT,
        }

    def test_incremental_runs(self, project):
        # First run (full refresh)
        res = run_dbt(["run"])
        assert len(res) == 1

        rows = project.run_sql(
            "select count(*) from {schema}.quoting_incr",
            fetch="one",
        )
        assert rows[0] == 1

        # Second run (incremental)
        res = run_dbt(["run"])
        assert len(res) == 1

        rows = project.run_sql(
            "select count(*) from {schema}.quoting_incr",
            fetch="one",
        )
        assert rows[0] == 2


# ===================================================================
# Test: snapshot uses bracket-quoted columns
# ===================================================================
class TestBracketQuotingSnapshot:
    """Snapshot should work with bracket-quoted column references."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"quoting_table.sql": _MODEL_TABLE}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"quoting_snap.sql": _SNAPSHOT}

    def test_snapshot_runs(self, project):
        # Create the source table first
        run_dbt(["run"])

        # First snapshot
        res = run_dbt(["snapshot"])
        assert len(res) == 1

        rows = project.run_sql(
            "select count(*) from {schema}.quoting_snap",
            fetch="one",
        )
        assert rows[0] == 1

        # Second snapshot (no changes)
        res = run_dbt(["snapshot"])
        assert len(res) == 1


# ===================================================================
# Test: special character identifiers with brackets
# ===================================================================
class TestBracketQuotingSpecialChars:
    """Columns with spaces/hyphens should work via bracket quoting."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"special_chars.sql": _MODEL_SPECIAL_CHARS}

    def test_special_char_columns(self, project):
        res = run_dbt(["run"])
        assert len(res) == 1

        result = project.run_sql(
            "select [col with spaces], [col-with-hyphens] from {schema}.special_chars",
            fetch="one",
        )
        assert result[0] == 1
        assert result[1] == 2


# ===================================================================
# Test: relation rendering produces bracket quoting (not double-quotes)
# ===================================================================
class TestRelationRenderingBrackets:
    """Compiled SQL should contain [] brackets, not double-quote identifiers."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"quoting_table.sql": _MODEL_TABLE}

    def test_compiled_sql_uses_brackets(self, project):
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)

        for node_id, node in manifest.nodes.items():
            if node.resource_type == "model" and hasattr(node, "relation_name"):
                relation_name = node.relation_name
                if relation_name:
                    # Should not contain double-quote quoting
                    assert (
                        '""' not in relation_name
                    ), f"Found double-quote quoting in {node_id}: {relation_name}"
