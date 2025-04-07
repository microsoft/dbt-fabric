import pytest

from tests.functional.packages.base_package_test import BaseDbtPackageTests


class TestDbtUtils(BaseDbtPackageTests):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "limit_zero.sql": """
{% macro fabric__limit_zero() %}
  {{ return('where 0=1') }} 
{% endmacro %}"""
        }

    @pytest.fixture(scope="class")
    def package_name(self) -> str:
        return "dbt_utils"

    @pytest.fixture(scope="class")
    def package_repo(self) -> str:
        return "https://github.com/dbt-labs/dbt-utils"

    @pytest.fixture(scope="class")
    def package_revision(self) -> str:
        return "1.3.0"

    @pytest.fixture(scope="class")
    def models_config(self):
        return {"dbt_utils_integration_tests": {"sql": {"test_groupby": {"+enabled": False}}}}

    @pytest.fixture(scope="class")
    def seeds_config(self):
        return {
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
        }
