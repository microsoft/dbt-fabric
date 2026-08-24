import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file

table_model_sql = """
{{ config(materialized='table') }}
select cast(1 as int) as id, cast('{{ value }}' as varchar(20)) as value
"""

incremental_model_sql = """
{{ config(materialized='incremental', unique_key='id', incremental_strategy='merge') }}
select cast(1 as int) as id, cast('{{ value }}' as varchar(20)) as value
"""

constraint_schema_yml = """
version: 2
models:
  - name: constraint_refresh
    config:
      contract:
        enforced: true
    constraints:
      - type: {constraint_type}
        name: {constraint_name}
        columns: [{constraint_columns}]
{expression}
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
      - name: value
        data_type: varchar(20)
        constraints:
          - type: not_null
"""


def _object_id(project, relation_name):
    result = project.run_sql(
        f"select object_id('{project.test_schema}.{relation_name}')",
        fetch="one",
    )
    return result[0]


def _value(project, relation_name):
    result = project.run_sql(
        f"select value from {project.test_schema}.{relation_name} where id = 1",
        fetch="one",
    )
    return result[0]


def _constraint_columns(project, relation_name, constraint_name):
    result = project.run_sql(
        f"""
        select c.name
        from sys.key_constraints kc
        join sys.index_columns ic
          on kc.parent_object_id = ic.object_id
         and kc.unique_index_id = ic.index_id
        join sys.columns c
          on ic.object_id = c.object_id
         and ic.column_id = c.column_id
        where kc.parent_object_id = object_id(
          '{project.test_schema}.{relation_name}'
        )
          and kc.name = '{constraint_name}'
        order by ic.key_ordinal
        """,
        fetch="all",
    )
    return [row[0] for row in result]


class TestTableRefreshPreservesObject:
    @pytest.fixture(scope="class")
    def models(self):
        return {"refresh_table.sql": table_model_sql.replace("{{ value }}", "first")}

    def test_table_reload_preserves_object_id(self, project):
        run_dbt(["run", "-s", "refresh_table"])
        original_object_id = _object_id(project, "refresh_table")

        write_file(
            table_model_sql.replace("{{ value }}", "second"),
            "models",
            "refresh_table.sql",
        )
        run_dbt(["run", "-s", "refresh_table"])

        assert _object_id(project, "refresh_table") == original_object_id
        assert _value(project, "refresh_table") == "second"


class TestTableRefreshReplacesChangedSchema:
    @pytest.fixture(scope="class")
    def models(self):
        return {"replace_table.sql": table_model_sql.replace("{{ value }}", "first")}

    def test_schema_change_replaces_object(self, project):
        run_dbt(["run", "-s", "replace_table"])
        original_object_id = _object_id(project, "replace_table")

        write_file(
            """
            {{ config(materialized='table') }}
            select cast(1 as bigint) as id, cast('second' as varchar(20)) as value
            """,
            "models",
            "replace_table.sql",
        )
        run_dbt(["run", "-s", "replace_table"])

        assert _object_id(project, "replace_table") != original_object_id
        assert _value(project, "replace_table") == "second"


class TestTableRefreshReplacesChangedLayout:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "replace_layout.sql": """
            {{ config(materialized='table', cluster_by=['id']) }}
            select cast(1 as int) as id, cast('first' as varchar(20)) as value
            """
        }

    def test_cluster_by_change_replaces_object(self, project):
        run_dbt(["run", "-s", "replace_layout"])
        original_object_id = _object_id(project, "replace_layout")

        write_file(
            """
            {{ config(materialized='table', cluster_by=['value']) }}
            select cast(1 as int) as id, cast('second' as varchar(20)) as value
            """,
            "models",
            "replace_layout.sql",
        )
        run_dbt(["run", "-s", "replace_layout"])

        assert _object_id(project, "replace_layout") != original_object_id
        assert _value(project, "replace_layout") == "second"


class TestTableRefreshReplacesSelfReference:
    @pytest.fixture(scope="class")
    def models(self):
        return {"self_reference.sql": table_model_sql.replace("{{ value }}", "original")}

    def test_self_reference_replaces_object_before_reading_target(self, project):
        run_dbt(["run", "-s", "self_reference"])
        original_object_id = _object_id(project, "self_reference")

        write_file(
            """
            {{ config(materialized='table') }}
            select id, value
            from {{ this }}
            """,
            "models",
            "self_reference.sql",
        )
        run_dbt(["run", "-s", "self_reference"])

        assert _object_id(project, "self_reference") != original_object_id
        assert _value(project, "self_reference") == "original"


class TestIncrementalFullRefreshPreservesObject:
    @pytest.fixture(scope="class")
    def models(self):
        return {"refresh_incremental.sql": incremental_model_sql.replace("{{ value }}", "first")}

    def test_incremental_full_refresh_preserves_object_id(self, project):
        run_dbt(["run", "-s", "refresh_incremental"])
        original_object_id = _object_id(project, "refresh_incremental")

        write_file(
            incremental_model_sql.replace("{{ value }}", "second"),
            "models",
            "refresh_incremental.sql",
        )
        run_dbt(["run", "--full-refresh", "-s", "refresh_incremental"])

        assert _object_id(project, "refresh_incremental") == original_object_id
        assert _value(project, "refresh_incremental") == "second"


class TestTableRefreshRollback:
    @pytest.fixture(scope="class")
    def models(self):
        return {"rollback_table.sql": table_model_sql.replace("{{ value }}", "original")}

    def test_failed_reload_rolls_back_truncate(self, project):
        run_dbt(["run", "-s", "rollback_table"])
        original_object_id = _object_id(project, "rollback_table")

        write_file(
            """
            {{ config(materialized='table') }}
            select cast(1 as int) as id,
                   cast(case when 1 = 1 then 1 / 0 else 1 end as varchar(20)) as value
            """,
            "models",
            "rollback_table.sql",
        )
        run_dbt_and_capture(["run", "-s", "rollback_table"], expect_pass=False)

        assert _object_id(project, "rollback_table") == original_object_id
        assert _value(project, "rollback_table") == "original"


class TestTableConstraintReconciliation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "constraint_refresh.sql": table_model_sql.replace("{{ value }}", "value"),
            "schema.yml": constraint_schema_yml.format(
                constraint_type="unique",
                constraint_name="uq_constraint_refresh",
                constraint_columns="id",
                expression="",
            ),
        }

    def test_changed_constraint_is_reconciled_without_replacing_table(self, project):
        run_dbt(["run", "-s", "constraint_refresh"])
        original_object_id = _object_id(project, "constraint_refresh")
        assert _constraint_columns(
            project,
            "constraint_refresh",
            "uq_constraint_refresh",
        ) == ["id"]

        write_file(
            constraint_schema_yml.format(
                constraint_type="unique",
                constraint_name="uq_constraint_refresh",
                constraint_columns="value",
                expression="",
            ),
            "models",
            "schema.yml",
        )
        run_dbt(["run", "-s", "constraint_refresh"])

        assert _object_id(project, "constraint_refresh") == original_object_id
        assert _constraint_columns(
            project,
            "constraint_refresh",
            "uq_constraint_refresh",
        ) == ["value"]

    def test_failed_constraint_addition_rolls_back_constraint_drop(self, project):
        write_file(
            constraint_schema_yml.format(
                constraint_type="unique",
                constraint_name="uq_constraint_refresh",
                constraint_columns="value",
                expression="",
            ),
            "models",
            "schema.yml",
        )
        run_dbt(["run", "-s", "constraint_refresh"])
        original_object_id = _object_id(project, "constraint_refresh")

        write_file(
            constraint_schema_yml.format(
                constraint_type="foreign_key",
                constraint_name="fk_constraint_refresh",
                constraint_columns="id",
                expression=(f"        expression: {project.test_schema}.missing_parent (id)"),
            ),
            "models",
            "schema.yml",
        )
        write_file(
            table_model_sql.replace("{{ value }}", "replacement"),
            "models",
            "constraint_refresh.sql",
        )
        run_dbt_and_capture(["run", "-s", "constraint_refresh"], expect_pass=False)

        assert _object_id(project, "constraint_refresh") == original_object_id
        assert _value(project, "constraint_refresh") == "value"
        assert _constraint_columns(
            project,
            "constraint_refresh",
            "uq_constraint_refresh",
        ) == ["value"]
