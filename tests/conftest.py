import os

import pytest
from _pytest.fixtures import FixtureRequest

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
