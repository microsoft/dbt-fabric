import pytest

from dbt.tests.util import run_dbt, write_file

INCREMENTAL_MODEL = """
{{ config(materialized='incremental', unique_key='id') }}
select cast(1 as int) as id,
       cast('original' as varchar(8000)) as payload
"""

WIDENED_INCREMENTAL_MODEL = """
{{ config(materialized='incremental', unique_key='id') }}
select cast(2 as int) as id,
       cast('widened' as varchar(max)) as payload
"""


def _column_length(project, relation_name, column_name):
    """sys.columns reports the byte length of a varchar column, or -1 for varchar(max)."""
    return project.run_sql(
        f"""
        select c.max_length
        from sys.columns c
        where c.object_id = object_id('{project.test_schema}.{relation_name}')
          and c.name = '{column_name}'
        """,
        fetch="one",
    )[0]


def _object_id(project, relation_name):
    return project.run_sql(
        f"select object_id('{project.test_schema}.{relation_name}')",
        fetch="one",
    )[0]


class TestIncrementalColumnWidening:
    """An incremental model whose source column widens keeps the same table."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"widening_model.sql": INCREMENTAL_MODEL}

    def test_widening_alters_the_column_in_place(self, project):
        run_dbt(["run", "-s", "widening_model"])
        assert _column_length(project, "widening_model", "payload") == 8000
        original_object_id = _object_id(project, "widening_model")

        write_file(WIDENED_INCREMENTAL_MODEL, "models", "widening_model.sql")
        run_dbt(["run", "-s", "widening_model"])

        assert _column_length(project, "widening_model", "payload") == -1
        assert _object_id(project, "widening_model") == original_object_id

        rows = project.run_sql(
            f"select id, payload from {project.test_schema}.widening_model order by id",
            fetch="all",
        )
        assert [(row[0], row[1]) for row in rows] == [(1, "original"), (2, "widened")]
