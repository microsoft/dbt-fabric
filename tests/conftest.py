import os
from pathlib import Path

import pytest
import yaml

pytest_plugins = ["dbt.tests.fixtures.project"]


@pytest.fixture(scope="class")
def dbt_profile_target(dbt_profile_target_update):
    target = {
        "type": "fabric",
        "driver": os.getenv("FABRIC_TEST_DRIVER", "ODBC Driver 18 for SQL Server"),
        "host": os.getenv("FABRIC_TEST_HOST"),
        "workspace_name": os.getenv("FABRIC_TEST_WORKSPACE_NAME"),
        "workspace_id": os.getenv("FABRIC_TEST_WORKSPACE_ID"),
        "lakehouse_id": os.getenv("FABRIC_TEST_LAKEHOUSE_ID"),
        "lakehouse_name": os.getenv("FABRIC_TEST_LAKEHOUSE_NAME"),
        "authentication": "auto",
        "database": os.getenv("FABRIC_TEST_DWH_NAME"),
        "retries": 3,
        "threads": int(os.getenv("FABRIC_TEST_THREADS", 20)),
        "login_timeout": 30,
        "query_timeout": 10,
    }

    target.update(dbt_profile_target_update)
    return target


@pytest.fixture(scope="class")
def dbt_profile_target_update():
    return {}


@pytest.fixture(scope="class")
def profile_user(dbt_profile_target):
    return "dbo"


def pytest_addoption(parser):
    parser.addoption("--with-grants", action="store_true", default=False, help="run GRANT tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "grants: mark test containing GRANT statements")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--with-grants"):
        # --with-grants given in cli: do not skip slow tests
        return
    skip_grants = pytest.mark.skip(reason="need --with-grants option to run")
    for item in items:
        if "grants" in item.keywords:
            item.add_marker(skip_grants)


@pytest.fixture(scope="class")
def logs_dir(request, prefix):
    dbt_log_dir = os.path.join(request.config.rootdir, "logs", prefix)
    print(f"\n=== Test logs_dir: {dbt_log_dir}\n")
    os.environ["DBT_LOG_PATH"] = str(dbt_log_dir)
    yield str(Path(dbt_log_dir))
    del os.environ["DBT_LOG_PATH"]


@pytest.fixture(scope="class")
def dbt_core_bug_workaround(project):
    # Workaround for https://github.com/dbt-labs/dbt-core/issues/5410
    with open(Path(project.project_root).parent / "dbt_project.yml", "w") as f:
        f.write(yaml.safe_dump({"name": "workaround"}))


@pytest.fixture(scope="class")
def project(
    project_setup,
    project_files,
):
    from dbt.tests.fixtures.project import TestProjInfo

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
