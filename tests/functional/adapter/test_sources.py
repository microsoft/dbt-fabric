import pytest

from dbt.tests.adapter.basic.files import config_materialized_table, config_materialized_view
from dbt.tests.fixtures.project import TestProjInfo
from dbt.tests.util import run_dbt

source_regular = """
version: 2
sources:
- name: regular
  schema: {}
  tables:
  - name: sample
    columns:
    - name: id
      tests:
      - not_null
"""

source_space_in_name = """
version: 2
sources:
- name: 'space in name'
  schema: {}
  tables:
  - name: sample
    columns:
    - name: id
      tests:
      - not_null
"""

select_from_source_regular = """
select id from {{ source("regular", "sample") }}
"""

select_from_source_space_in_name = """
select id from {{ source("space in name", "sample") }}
"""


class TestSourcesFabric:
    @pytest.fixture(scope="class")
    def models(self, unique_schema: str):
        return {
            "source_regular.yml": source_regular.format(unique_schema),
            "source_space_in_name.yml": source_space_in_name.format(unique_schema),
            "v_select_from_source_regular.sql": config_materialized_view
            + select_from_source_regular,
            "v_select_from_source_space_in_name.sql": config_materialized_view
            + select_from_source_space_in_name,
            "t_select_from_source_regular.sql": config_materialized_table
            + select_from_source_regular,
            "t_select_from_source_space_in_name.sql": config_materialized_table
            + select_from_source_space_in_name,
        }

    def test_dbt_run(self, project):
        project.run_sql(f"create table {project.test_schema}.sample (id int)")
        run_dbt(["compile"])

        ls = run_dbt(["list"])
        assert len(ls) == 8
        ls_sources = [src for src in ls if src.startswith("source:")]
        assert len(ls_sources) == 2

        run_dbt(["run"])
        run_dbt(["test"])
