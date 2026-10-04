"""Functional tests for Fabric IDENTITY column support (`meta.identity`).

Fabric Warehouse tables can declare a single `bigint IDENTITY` column, but that DDL
can only be attached through a contract-enforced `CREATE TABLE`. dbt-fabric exposes
this via a column's `meta.identity: auto|insert` config:

- `auto`: Fabric assigns the value; the column is left out of the INSERT/SELECT list.
- `insert`: the model's own query supplies explicit values, written through
  `SET IDENTITY_INSERT ... ON/OFF` and reseeded afterward with `DBCC CHECKIDENT` so
  later 'auto' inserts don't collide.

These tests exercise the full refresh-plan decision (reload vs. replace) together
with the actual DDL/DML Fabric executes, since none of this is observable from unit
tests that stub out the warehouse.
"""

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file


def _object_id(project, relation_name):
    result = project.run_sql(
        f"select object_id('{project.test_schema}.{relation_name}')",
        fetch="one",
    )
    return result[0]


def _value(project, relation_name, column="value"):
    # Bracket-quote the identifiers: Fabric Warehouse treats some model names used in
    # these tests (e.g. "identity_insert") as reserved keywords when referenced bare.
    result = project.run_sql(
        f"select [{column}] from [{project.test_schema}].[{relation_name}]",
        fetch="one",
    )
    return result[0]


def _is_identity(project, relation_name, column_name):
    result = project.run_sql(
        f"""
        select is_identity
        from sys.columns
        where object_id = object_id('{project.test_schema}.{relation_name}')
          and name = '{column_name}'
        """,
        fetch="one",
    )
    return bool(result[0])


def _identity_schema_yml(model_name, mode):
    return f"""
version: 2
models:
  - name: {model_name}
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: bigint
        meta:
          # Quoted: a bare `yes`/`no` is parsed as a YAML 1.1 boolean, not a string.
          identity: "{mode}"
      - name: value
        data_type: varchar(20)
"""


def _plain_contract_schema_yml(model_name):
    return f"""
version: 2
models:
  - name: {model_name}
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: bigint
      - name: value
        data_type: varchar(20)
"""


class TestIdentityAutoModeAssignsValueAndPreservesObjectAcrossReload:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "identity_auto.sql": """
            {{ config(materialized='table') }}
            select cast(null as bigint) as id, cast('first' as varchar(20)) as value
            """,
            "schema.yml": _identity_schema_yml("identity_auto", "auto"),
        }

    def test_auto_mode_assigns_value_and_reload_preserves_object(self, project):
        run_dbt(["run", "-s", "identity_auto"])

        assert _is_identity(project, "identity_auto", "id")
        assert _value(project, "identity_auto", "id") is not None
        original_object_id = _object_id(project, "identity_auto")

        # Unchanged schema: this must take the `reload` path (TRUNCATE + INSERT),
        # not a `replace`, even though the identity column is now present.
        write_file(
            """
            {{ config(materialized='table') }}
            select cast(null as bigint) as id, cast('second' as varchar(20)) as value
            """,
            "models",
            "identity_auto.sql",
        )
        run_dbt(["run", "-s", "identity_auto"])

        assert _object_id(project, "identity_auto") == original_object_id
        assert _value(project, "identity_auto") == "second"
        assert _is_identity(project, "identity_auto", "id")


class TestIdentityInsertModePreservesExplicitValueAndReseeds:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "identity_insert.sql": """
            {{ config(materialized='table') }}
            select cast(100 as bigint) as id, cast('first' as varchar(20)) as value
            """,
            "schema.yml": _identity_schema_yml("identity_insert", "insert"),
        }

    def test_explicit_value_is_preserved(self, project):
        run_dbt(["run", "-s", "identity_insert"])

        assert _is_identity(project, "identity_insert", "id")
        assert _value(project, "identity_insert", "id") == 100

    def test_reseed_lets_subsequent_inserts_auto_increment_without_collision(self, project):
        run_dbt(["run", "-s", "identity_insert"])

        # The post-build DBCC CHECKIDENT reseed must leave the identity generator at
        # (or past) the explicit value just written, so a plain auto-assigned insert
        # afterward is guaranteed not to collide with it.
        project.run_sql(
            f"insert into [{project.test_schema}].[identity_insert] ([value]) values ('second')"
        )
        new_id = project.run_sql(
            f"select [id] from [{project.test_schema}].[identity_insert] where [value] = 'second'",
            fetch="one",
        )[0]

        assert new_id > 100

    def test_reload_preserves_object_and_updated_value_on_rerun(self, project):
        run_dbt(["run", "-s", "identity_insert"])
        original_object_id = _object_id(project, "identity_insert")

        write_file(
            """
            {{ config(materialized='table') }}
            select cast(200 as bigint) as id, cast('third' as varchar(20)) as value
            """,
            "models",
            "identity_insert.sql",
        )
        run_dbt(["run", "-s", "identity_insert"])

        assert _object_id(project, "identity_insert") == original_object_id
        assert _value(project, "identity_insert", "id") == 200
        assert _value(project, "identity_insert") == "third"


class TestIdentityAddedToExistingTableForcesReplace:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "identity_added.sql": """
            {{ config(materialized='table') }}
            select cast(1 as bigint) as id, cast('first' as varchar(20)) as value
            """,
            "schema.yml": _plain_contract_schema_yml("identity_added"),
        }

    def test_declaring_identity_replaces_the_object(self, project):
        run_dbt(["run", "-s", "identity_added"])
        assert not _is_identity(project, "identity_added", "id")
        original_object_id = _object_id(project, "identity_added")

        write_file(
            _identity_schema_yml("identity_added", "auto"),
            "models",
            "schema.yml",
        )
        write_file(
            """
            {{ config(materialized='table') }}
            select cast(null as bigint) as id, cast('second' as varchar(20)) as value
            """,
            "models",
            "identity_added.sql",
        )
        run_dbt(["run", "-s", "identity_added"])

        assert _object_id(project, "identity_added") != original_object_id
        assert _is_identity(project, "identity_added", "id")
        assert _value(project, "identity_added") == "second"


class TestIdentityRemovedFromExistingTableForcesReplace:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "identity_removed.sql": """
            {{ config(materialized='table') }}
            select cast(1 as bigint) as id, cast('first' as varchar(20)) as value
            """,
            "schema.yml": _identity_schema_yml("identity_removed", "auto"),
        }

    def test_removing_identity_replaces_the_object(self, project):
        run_dbt(["run", "-s", "identity_removed"])
        assert _is_identity(project, "identity_removed", "id")
        original_object_id = _object_id(project, "identity_removed")

        write_file(
            _plain_contract_schema_yml("identity_removed"),
            "models",
            "schema.yml",
        )
        write_file(
            """
            {{ config(materialized='table') }}
            select cast(2 as bigint) as id, cast('second' as varchar(20)) as value
            """,
            "models",
            "identity_removed.sql",
        )
        run_dbt(["run", "-s", "identity_removed"])

        assert _object_id(project, "identity_removed") != original_object_id
        assert not _is_identity(project, "identity_removed", "id")
        assert _value(project, "identity_removed") == "second"


class TestIdentityRequiresContract:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "identity_no_contract.sql": """
            {{ config(materialized='table') }}
            select cast(1 as bigint) as id, cast('first' as varchar(20)) as value
            """,
            "schema.yml": """
version: 2
models:
  - name: identity_no_contract
    columns:
      - name: id
        data_type: bigint
        meta:
          identity: auto
      - name: value
        data_type: varchar(20)
""",
        }

    def test_identity_without_contract_fails_clearly(self, project):
        _, log_output = run_dbt_and_capture(
            ["run", "-s", "identity_no_contract"], expect_pass=False
        )

        assert "contract.enforced" in log_output


class TestIdentityRejectsNonBigintDataType:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "identity_wrong_type.sql": """
            {{ config(materialized='table') }}
            select cast(1 as int) as id, cast('first' as varchar(20)) as value
            """,
            "schema.yml": """
version: 2
models:
  - name: identity_wrong_type
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        meta:
          identity: auto
      - name: value
        data_type: varchar(20)
""",
        }

    def test_non_bigint_identity_fails_clearly(self, project):
        _, log_output = run_dbt_and_capture(
            ["run", "-s", "identity_wrong_type"], expect_pass=False
        )

        assert "bigint" in log_output


class TestIdentityRejectsInvalidMode:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "identity_bad_mode.sql": """
            {{ config(materialized='table') }}
            select cast(1 as bigint) as id, cast('first' as varchar(20)) as value
            """,
            "schema.yml": _identity_schema_yml("identity_bad_mode", "yes"),
        }

    def test_invalid_mode_fails_clearly(self, project):
        _, log_output = run_dbt_and_capture(["run", "-s", "identity_bad_mode"], expect_pass=False)

        assert "identity" in log_output.lower()
        assert "yes" in log_output.lower()


class TestIdentityRejectsMultipleColumns:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "identity_multiple.sql": """
            {{ config(materialized='table') }}
            select cast(1 as bigint) as id, cast(2 as bigint) as other_id,
                   cast('first' as varchar(20)) as value
            """,
            "schema.yml": """
version: 2
models:
  - name: identity_multiple
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: bigint
        meta:
          identity: auto
      - name: other_id
        data_type: bigint
        meta:
          identity: auto
      - name: value
        data_type: varchar(20)
""",
        }

    def test_multiple_identity_columns_fails_clearly(self, project):
        _, log_output = run_dbt_and_capture(["run", "-s", "identity_multiple"], expect_pass=False)

        assert "at most one IDENTITY column" in log_output


class TestIdentityRejectsNonTableMaterialization:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "identity_incremental.sql": """
            {{ config(materialized='incremental', unique_key='id', on_schema_change='fail') }}
            select cast(1 as bigint) as id, cast('first' as varchar(20)) as value
            """,
            "schema.yml": _identity_schema_yml("identity_incremental", "auto"),
        }

    def test_identity_on_incremental_fails_clearly(self, project):
        _, log_output = run_dbt_and_capture(
            ["run", "-s", "identity_incremental"], expect_pass=False
        )

        assert "materialized='table'" in log_output


class TestIdentityInsertModeHandlesEmptyResultSet:
    """Fabric's `DBCC CHECKIDENT (table, RESEED)` only supports the bare form -- it
    computes the next value internally and rejects an explicit `new_reseed_value`
    (unlike SQL Server/Synapse, where a custom value or an explicit NULL could be
    passed). This exercises the case where an `insert`-mode model's query produces
    zero rows, so there's nothing to reseed against, both on first build and across a
    reload.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "identity_insert_empty.sql": """
            {{ config(materialized='table') }}
            select cast(100 as bigint) as id, cast('first' as varchar(20)) as value
            where 1 = 0
            """,
            "schema.yml": _identity_schema_yml("identity_insert_empty", "insert"),
        }

    def test_build_with_zero_rows_succeeds(self, project):
        run_dbt(["run", "-s", "identity_insert_empty"])

        assert _is_identity(project, "identity_insert_empty", "id")
        row_count = project.run_sql(
            f"select count(*) from {project.test_schema}.identity_insert_empty",
            fetch="one",
        )[0]
        assert row_count == 0

    def test_reload_with_zero_rows_still_succeeds_and_preserves_object(self, project):
        run_dbt(["run", "-s", "identity_insert_empty"])
        original_object_id = _object_id(project, "identity_insert_empty")

        # Unchanged schema, still zero rows: must still take the `reload` path and
        # must not fail trying to reseed against an empty table.
        run_dbt(["run", "-s", "identity_insert_empty"])

        assert _object_id(project, "identity_insert_empty") == original_object_id

    def test_identity_still_assigns_values_after_an_empty_reseed(self, project):
        run_dbt(["run", "-s", "identity_insert_empty"])

        # Confirms the empty-table reseed didn't leave the identity generator in a
        # broken state: a plain auto-assigned insert afterward must still work.
        project.run_sql(
            f"insert into {project.test_schema}.identity_insert_empty (value) values ('second')"
        )
        new_id = project.run_sql(
            f"select id from {project.test_schema}.identity_insert_empty where value = 'second'",
            fetch="one",
        )[0]

        assert new_id is not None
