import pytest

from dbt.tests.util import run_dbt, write_file

my_model_sql = """
select
    tested_column from {{ ref('my_upstream_model')}}
"""

my_upstream_model_sql = """
select
  {sql_value} as tested_column
"""

# test_my_model_yml = """
# unit_tests:
#   - name: test_my_model
#     model: my_model
#     given:
#       - input: ref('my_upstream_model')
#         rows:
#           - {{ tested_column: {yaml_value} }}
#     expect:
#       rows:
#         - {{ tested_column: {yaml_value} }}
# """

test_my_model_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('my_upstream_model')
        rows:
          - {{ tested_column: {yaml_value} }}
    expect:
      rows:
        - {{ tested_column: {yaml_value} }}
"""


class BaseUnitTestingTypes:
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["'1'", "1"],
            ["true", "1"],
            ["DATE '2020-01-02'", "2020-01-02"],
            ["DATETIME2 '2013-11-03 00:00:00-0'", "2013-11-03 00:00:00-0"],
        ]

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_upstream_model.sql": my_upstream_model_sql,
            "schema.yml": test_my_model_yml,
        }

    def test_unit_test_data_type(self, project, data_types):
        for sql_value, yaml_value in data_types:
            # Write parametrized type value to sql files
            write_file(
                my_upstream_model_sql.format(sql_value=sql_value),
                "models",
                "my_upstream_model.sql",
            )

            # Write parametrized type value to unit test yaml definition
            write_file(
                test_my_model_yml.format(yaml_value=yaml_value),
                "models",
                "schema.yml",
            )

            results = run_dbt(["run", "--select", "my_upstream_model"])
            assert len(results) == 1

            try:
                run_dbt(["test", "--select", "my_model"])
            except Exception:
                raise AssertionError(f"unit test failed when testing model with {sql_value}")


@pytest.mark.skip(
    reason="Nested CTE's are not supported by Fabric DW. Should able to run this in 6 months"
)
class TestPostgresUnitTestingTypes(BaseUnitTestingTypes):
    pass
