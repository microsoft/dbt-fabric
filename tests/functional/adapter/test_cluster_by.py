import re

import pytest
from dbt.tests.util import read_file, run_dbt, run_dbt_and_capture

# -- Model fixtures --

cluster_by_table_model_sql = """
{{ config(
    materialized='table',
    cluster_by=['id', 'order_date']
) }}

select 1 as id, cast('2024-01-01' as date) as order_date, 100.00 as amount
"""

cluster_by_single_column_model_sql = """
{{ config(
    materialized='table',
    cluster_by='id'
) }}

select 1 as id, cast('2024-01-01' as date) as order_date
"""

cluster_by_incremental_model_sql = """
{{ config(
    materialized='incremental',
    cluster_by=['id', 'order_date']
) }}

select 1 as id, cast('2024-01-01' as date) as order_date, 100.00 as amount
"""

cluster_by_too_many_columns_model_sql = """
{{ config(
    materialized='table',
    cluster_by=['col1', 'col2', 'col3', 'col4', 'col5']
) }}

select 1 as col1, 2 as col2, 3 as col3, 4 as col4, 5 as col5
"""

cluster_by_max_columns_model_sql = """
{{ config(
    materialized='table',
    cluster_by=['col1', 'col2', 'col3', 'col4']
) }}

select 1 as col1, 2 as col2, 3 as col3, 4 as col4
"""

no_cluster_by_model_sql = """
{{ config(materialized='table') }}

select 1 as id, cast('2024-01-01' as date) as order_date
"""

cluster_by_contract_model_sql = """
{{ config(
    materialized='table',
    cluster_by=['id', 'order_date'],
    contract={'enforced': true}
) }}

select 1 as id, cast('2024-01-01' as date) as order_date, 100.00 as amount
"""

cluster_by_contract_schema_yml = """
version: 2
models:
  - name: cluster_by_contract_model
    config:
      contract:
        enforced: true
      cluster_by:
        - id
        - order_date
    columns:
      - name: id
        data_type: int
      - name: order_date
        data_type: date
      - name: amount
        data_type: "decimal(10,2)"
"""


def _normalize_whitespace(input: str) -> str:
    subbed = re.sub(r"\s+", " ", input)
    return re.sub(r"\s?([\(\),])\s?", r"\1", subbed).lower().strip()


class TestClusterByTable:
    """Test CLUSTER BY clause in table materialization (CTAS path)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cluster_by_model.sql": cluster_by_table_model_sql,
        }

    def test__cluster_by_in_generated_sql(self, project):
        results = run_dbt(["run", "-s", "cluster_by_model"])
        assert len(results) == 1

        generated_sql = read_file("target", "run", "test", "models", "cluster_by_model.sql")
        normalized = _normalize_whitespace(generated_sql)

        assert "with(cluster by(id,order_date))" in normalized


class TestClusterBySingleColumn:
    """Test CLUSTER BY with a single column string shorthand."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cluster_by_single.sql": cluster_by_single_column_model_sql,
        }

    def test__cluster_by_single_column(self, project):
        results = run_dbt(["run", "-s", "cluster_by_single"])
        assert len(results) == 1

        generated_sql = read_file("target", "run", "test", "models", "cluster_by_single.sql")
        normalized = _normalize_whitespace(generated_sql)

        assert "with(cluster by(id))" in normalized


class TestClusterByIncremental:
    """Test CLUSTER BY in incremental materialization (full refresh creates new table)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cluster_by_incremental.sql": cluster_by_incremental_model_sql,
        }

    def test__cluster_by_incremental_full_refresh(self, project):
        # First run = full refresh (new table), should have CLUSTER BY
        results = run_dbt(["run", "-s", "cluster_by_incremental"])
        assert len(results) == 1

        generated_sql = read_file("target", "run", "test", "models", "cluster_by_incremental.sql")
        normalized = _normalize_whitespace(generated_sql)

        assert "with(cluster by(id,order_date))" in normalized

    def test__cluster_by_incremental_merge_no_cluster(self, project):
        # First run creates the table
        run_dbt(["run", "-s", "cluster_by_incremental"])

        # Second run is incremental merge — temp table should NOT have CLUSTER BY
        results = run_dbt(["run", "-s", "cluster_by_incremental"])
        assert len(results) == 1

        generated_sql = read_file("target", "run", "test", "models", "cluster_by_incremental.sql")
        normalized = _normalize_whitespace(generated_sql)

        # The incremental path creates a temp table (temporary=True),
        # so CLUSTER BY should not appear in the temp table creation
        # but the main merge/insert statement should not add clustering either
        assert "cluster by" not in normalized or "with(cluster by" not in normalized


class TestClusterByTooManyColumns:
    """Test that more than 4 columns raises a compiler error."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cluster_by_too_many.sql": cluster_by_too_many_columns_model_sql,
        }

    def test__cluster_by_exceeds_max_columns(self, project):
        results, log_output = run_dbt_and_capture(
            ["run", "-s", "cluster_by_too_many"], expect_pass=False
        )
        assert len(results) == 1
        assert "maximum of 4 columns" in log_output


class TestClusterByMaxColumns:
    """Test that exactly 4 columns (the maximum) works."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cluster_by_max.sql": cluster_by_max_columns_model_sql,
        }

    def test__cluster_by_four_columns(self, project):
        results = run_dbt(["run", "-s", "cluster_by_max"])
        assert len(results) == 1

        generated_sql = read_file("target", "run", "test", "models", "cluster_by_max.sql")
        normalized = _normalize_whitespace(generated_sql)

        assert "with(cluster by(col1,col2,col3,col4))" in normalized


class TestNoClusterBy:
    """Test that without cluster_by config, no CLUSTER BY clause appears."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "no_cluster_model.sql": no_cluster_by_model_sql,
        }

    def test__no_cluster_by_clause(self, project):
        results = run_dbt(["run", "-s", "no_cluster_model"])
        assert len(results) == 1

        generated_sql = read_file("target", "run", "test", "models", "no_cluster_model.sql")
        normalized = _normalize_whitespace(generated_sql)

        assert "cluster by" not in normalized


class TestClusterByWithContract:
    """Test CLUSTER BY combined with contract enforcement (CREATE TABLE + INSERT path)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cluster_by_contract_model.sql": cluster_by_contract_model_sql,
            "constraints_schema.yml": cluster_by_contract_schema_yml,
        }

    def test__cluster_by_with_contract(self, project):
        results = run_dbt(["run", "-s", "cluster_by_contract_model"])
        assert len(results) == 1

        generated_sql = read_file(
            "target", "run", "test", "models", "cluster_by_contract_model.sql"
        )
        normalized = _normalize_whitespace(generated_sql)

        # Contract path: CREATE TABLE (columns) WITH (CLUSTER BY (...))
        assert "with(cluster by(id,order_date))" in normalized
        # Should also have the column definitions from the contract
        assert "id int" in normalized
        assert "order_date date" in normalized
