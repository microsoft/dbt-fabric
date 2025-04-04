import os

import pytest

from dbt.tests.fixtures.project import TestProjInfo
from tests.test_proj_info import TestProjInfoFabric

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
