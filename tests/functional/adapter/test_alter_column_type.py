import pytest

from dbt.tests.util import run_dbt

MODEL_SQL = """
{{ config(materialized='incremental', on_schema_change='sync_all_columns') }}
select
    cast('abc' as varchar({{ var('width', 18) }})) as id,
    1 as [my-col],
    2 as [other--col],
    3 as [space - col],
    4 as [bracket]]-col]
{% if is_incremental() %}
where 1 = 0
{% endif %}
"""


class TestAlterColumnTypeWithHyphens:
    @pytest.fixture(scope="class")
    def models(self):
        return {"hyphen_columns.sql": MODEL_SQL}

    def test_type_expansion_preserves_columns_and_data(self, project):
        assert len(run_dbt(["run"])) == 1
        assert len(run_dbt(["run", "--vars", "{width: 8000}"])) == 1

        rows = project.run_sql(
            "select id, [my-col], [other--col], [space - col], [bracket]]-col] "
            "from {schema}.hyphen_columns",
            fetch="all",
        )
        assert [tuple(row) for row in rows] == [("abc", 1, 2, 3, 4)]
        column_type = project.run_sql(
            "select CHARACTER_MAXIMUM_LENGTH from INFORMATION_SCHEMA.COLUMNS "
            "where TABLE_SCHEMA = '{schema}' and TABLE_NAME = 'hyphen_columns' "
            "and COLUMN_NAME = 'id'",
            fetch="one",
        )
        assert column_type[0] == 8000
