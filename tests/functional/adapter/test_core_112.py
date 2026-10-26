import shutil
from pathlib import Path

import pytest
from mssql_python.exceptions import ProgrammingError
from packaging.version import Version

from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.util import relation_from_name, run_dbt, write_file
from dbt.version import __version__ as dbt_version

INLINE_SQL_COMMIT_XFAIL = pytest.mark.xfail(
    Version("1.12.0") <= Version(dbt_version) <= Version("1.12.5"),
    reason="dbt-labs/dbt#16784: Core releases inline SQL transactions without committing",
    raises=ProgrammingError,
    strict=True,
)


class TestRunOperationSQL:
    @INLINE_SQL_COMMIT_XFAIL
    def test_sql_executes_without_jinja(self, project):
        relation = relation_from_name(project.adapter, "plain_operation_result")
        results = run_dbt(
            [
                "run-operation",
                "--sql",
                f"create table {relation} (id int); insert into {relation} values (42);",
            ]
        )

        assert results.results[0].status == RunStatus.Success
        assert project.run_sql(f"select id from {relation}", fetch="one")[0] == 42

    @INLINE_SQL_COMMIT_XFAIL
    def test_sql_executes_with_jinja(self, project):
        run_dbt(
            [
                "run-operation",
                "--sql",
                "create table {{ adapter.quote(target.schema) }}.[operation_result] (id int); "
                "insert into {{ adapter.quote(target.schema) }}.[operation_result] values (42);",
            ]
        )

        relation = relation_from_name(project.adapter, "operation_result")
        assert project.run_sql(f"select id from {relation}", fetch="one")[0] == 42

    def test_sql_failure_rolls_back_writes(self, project):
        relation = relation_from_name(project.adapter, "failed_operation_result")
        results = run_dbt(
            [
                "run-operation",
                "--sql",
                f"create table {relation} (id int); "
                f"insert into {relation} values (42); "
                f"insert into {relation} values ('not-an-integer');",
            ],
            expect_pass=False,
        )

        assert results.results[0].status == RunStatus.Error
        with project.adapter.connection_named("check_operation_rollback"):
            assert (
                project.adapter.get_relation(
                    database=project.database,
                    schema=project.test_schema,
                    identifier="failed_operation_result",
                )
                is None
            )


class TestLatestVersionPointer:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "versioned_v1.sql": "select 1 as id",
            "versioned_v2.sql": "select 2 as id",
            "schema.yml": """
version: 2
models:
  - name: versioned
    latest_version: 2
    config:
      latest_version_pointer:
        enabled: true
    versions:
      - v: 1
      - v: 2
""",
        }

    def test_pointer_view_uses_latest_version(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        relation = relation_from_name(project.adapter, "versioned")
        assert project.run_sql(f"select id from {relation}", fetch="one")[0] == 2


class TestStateModifiedUnrenderedValues:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "state_model.sql": "select 1 as id",
            "schema.yml": """
version: 2
models:
  - name: state_model
    config:
      materialized: "{{ var('materialization', 'view') }}"
""",
        }

    def test_changed_jinja_config_is_selected(self, project, tmp_path):
        run_dbt(["parse"])
        shutil.copyfile(
            Path(project.project_root) / "target" / "manifest.json",
            tmp_path / "manifest.json",
        )
        assert (
            run_dbt(
                ["ls", "--state", str(tmp_path), "--select", "state:modified", "--output", "name"]
            )
            == []
        )

        write_file(
            """
version: 2
models:
  - name: state_model
    config:
      materialized: view
""",
            project.project_root,
            "models",
            "schema.yml",
        )

        assert run_dbt(
            ["ls", "--state", str(tmp_path), "--select", "state:modified", "--output", "name"]
        ) == ["state_model"]
