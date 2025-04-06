import pytest

from dbt.tests.util import run_dbt


class TestDbtUtils:
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "limit_zero.sql": """
{% macro fabric__limit_zero() %}
  {{ return('where 0=1') }} 
{% endmacro %}"""
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"git": "https://github.com/dbt-labs/dbt-utils", "revision": "1.3.0"},
                {
                    "git": "https://github.com/dbt-labs/dbt-utils",
                    "revision": "1.3.0",
                    "subdirectory": "integration_tests",
                },
            ]
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_dbt_utils",
            "dispatch": [
                {
                    "macro_namespace": "dbt_utils",
                    "search_order": [
                        "test_dbt_utils",
                        "dbt_fabric",
                        "dbt",
                        "dbt_utils",
                    ],
                }
            ],
            "seeds": {
                "dbt_utils_integration_tests": {
                    "sql": {
                        "data_get_single_value": {
                            "+column_types": {"date_value": "datetime2(6)", "int_value": "int"}
                        }
                    },
                    "schema_tests": {
                        "data_test_sequential_timestamps": {
                            "+column_types": {"my_timestamp": "datetime2(6)"}
                        }
                    },
                }
            },
            "models": {
                "dbt_utils_integration_tests": {"sql": {"test_groupby": {"+enabled": False}}}
            },
        }

    def test_dbt_utils(self, project, dbt_core_bug_workaround):
        run_dbt(["deps"])
        run_dbt(["seed"])
        run_dbt(["run"])
