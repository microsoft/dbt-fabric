import os

import pytest

from dbt.tests.fixtures.project import TestProjInfo

pytest_plugins = ["dbt.tests.fixtures.project"]


@pytest.fixture(scope="class")
def dbt_profile_target(dbt_profile_target_update):
    target = {
        "type": "fabric",
        "driver": os.getenv("FABRIC_TEST_DRIVER", "ODBC Driver 18 for SQL Server"),
        "host": os.getenv("FABRIC_TEST_ENDPOINT"),
        "authentication": "auto",
        "database": os.getenv("FABRIC_TEST_DWH_NAME"),
        "retries": 2,
    }

    target.update(dbt_profile_target_update)
    return target


@pytest.fixture(scope="class")
def dbt_profile_target_update():
    return {}


@pytest.fixture(scope="class")
def profile_user(dbt_profile_target):
    return "dbo"


class TestProjInfoFabric(TestProjInfo):
    def get_tables_in_schema(self):
        sql = f"""
                select
                        t.name as table_name,
                        'table' as materialization
                from sys.tables t
                inner join sys.schemas s
                on s.schema_id = t.schema_id
                where lower(s.name) = '{self.test_schema.lower()}'
                union all
                select
                        v.name as table_name,
                        'view' as materialization
                from sys.views v
                inner join sys.schemas s
                on s.schema_id = v.schema_id
                where lower(s.name) = '{self.test_schema.lower()}'
                """
        result = self.run_sql(sql, fetch="all")
        return {model_name: materialization for (model_name, materialization) in result}


@pytest.fixture(scope="class")
def project(
    project_setup: TestProjInfo,
    project_files,
):
    return TestProjInfoFabric(
        project_root=project_setup.project_root,
        profiles_dir=project_setup.profiles_dir,
        adapter_type=project_setup.adapter_type,
        test_dir=project_setup.test_dir,
        shared_data_dir=project_setup.shared_data_dir,
        test_data_dir=project_setup.test_data_dir,
        test_schema=project_setup.test_schema,
        database=project_setup.database,
        test_config=project_setup.test_config,
    )
