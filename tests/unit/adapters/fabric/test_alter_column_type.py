"""Exercises `fabric__alter_column_type` through dbt's own macro dispatch.

A real project config, adapter and macro manifest are built from the installed dbt packages,
so `alter_column_type` resolves to this adapter's implementation the same way it does in a
run. Only the two database calls are replaced: reading the columns of a relation, and
executing SQL. Whether Fabric Warehouse accepts a given change is not covered here.
"""

from argparse import Namespace

import pytest
from dbt_common.clients.agate_helper import empty_table
from dbt_common.exceptions import CompilationError

from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.fabric.fabric_column import FabricColumn
from dbt.adapters.fabric.fabric_credentials import FabricCredentials
from dbt.adapters.fabric.fabric_relation import FabricRelation
from dbt.adapters.factory import get_adapter, load_plugin, register_adapter, reset_adapters
from dbt.config.profile import Profile
from dbt.config.project import Project
from dbt.config.renderer import DbtProjectYamlRenderer
from dbt.config.runtime import RuntimeConfig
from dbt.context.providers import generate_runtime_macro_context
from dbt.flags import set_from_args
from dbt.mp_context import get_mp_context
from dbt.parser.manifest import ManifestLoader

PROJECT_NAME = "fabric_macro_dispatch"

TARGET = FabricRelation.create(
    database="warehouse", schema="dbo", identifier="target", type="table"
)
SOURCE = FabricRelation.create(
    database="warehouse", schema="dbo", identifier="source", type="table"
)


@pytest.fixture(scope="module")
def adapter(tmp_path_factory):
    project_dir = tmp_path_factory.mktemp(PROJECT_NAME)
    (project_dir / "dbt_project.yml").write_text(
        f'name: {PROJECT_NAME}\nversion: "1.0"\nprofile: {PROJECT_NAME}\n'
    )
    args = Namespace(
        profile=None,
        target=None,
        threads=1,
        vars={},
        project_dir=str(project_dir),
        profiles_dir=str(project_dir),
        which="run",
    )
    set_from_args(args, None)
    load_plugin("fabric")

    config = RuntimeConfig.from_parts(
        Project.from_project_root(str(project_dir), DbtProjectYamlRenderer()),
        Profile.from_credentials(
            FabricCredentials(database="warehouse", schema="dbo", host="example.fabric.test"),
            1,
            PROJECT_NAME,
            "default",
        ),
        args,
    )
    register_adapter(config, get_mp_context())
    adapter = get_adapter(config)
    adapter.set_macro_context_generator(generate_runtime_macro_context)
    adapter.set_macro_resolver(
        ManifestLoader.load_macros(
            config, adapter.connections.set_query_header, base_macros_only=True
        )
    )

    yield adapter

    reset_adapters()


@pytest.fixture
def executed(adapter, monkeypatch):
    """Collects the SQL that would be sent to the warehouse."""
    statements: list[str] = []

    def execute(sql, auto_begin=False, fetch=False, limit=None):
        statements.append(" ".join(sql.split()))
        return AdapterResponse(_message="OK"), empty_table()

    monkeypatch.setattr(adapter, "execute", execute)
    return statements


@pytest.fixture
def relation_columns(adapter, monkeypatch):
    """Stands in for the metadata query behind adapter.get_columns_in_relation."""
    columns: dict[str, list[FabricColumn]] = {}

    monkeypatch.setattr(
        adapter, "get_columns_in_relation", lambda relation: columns[relation.identifier]
    )
    return columns


def column(name="payload", char_size=8000, is_nullable=True, collation_name=None, identity=False):
    return FabricColumn(
        column=name,
        dtype="varchar",
        char_size=char_size,
        is_nullable=is_nullable,
        collation_name=collation_name,
        is_identity=identity,
    )


class TestExpandColumnTypes:
    """The path every incremental run and snapshot takes through expand_target_column_types."""

    def test_widening_to_varchar_max_alters_in_place(self, adapter, relation_columns, executed):
        relation_columns["target"] = [
            column(
                char_size=8000, is_nullable=False, collation_name="Latin1_General_100_BIN2_UTF8"
            )
        ]
        relation_columns["source"] = [column(char_size=-1, is_nullable=False)]

        adapter.expand_column_types(SOURCE, TARGET)

        assert executed == [
            "ALTER TABLE [warehouse].[dbo].[target] ALTER COLUMN [payload] varchar(max) "
            "COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL"
        ]

    def test_widening_does_not_copy_the_target(self, adapter, relation_columns, executed):
        relation_columns["target"] = [column(char_size=8000)]
        relation_columns["source"] = [column(char_size=-1)]

        adapter.expand_column_types(SOURCE, TARGET)

        emitted = " ".join(executed).upper()
        assert "CREATE TABLE" not in emitted
        assert "DROP TABLE" not in emitted
        assert "INSERT INTO" not in emitted
        assert "SELECT" not in emitted

    def test_varchar_max_target_is_not_narrowed(self, adapter, relation_columns, executed):
        relation_columns["target"] = [column(char_size=-1)]
        relation_columns["source"] = [column(char_size=100)]

        adapter.expand_column_types(SOURCE, TARGET)

        assert executed == []

    def test_every_changed_column_alters_separately(self, adapter, relation_columns, executed):
        relation_columns["target"] = [column("payload", 8000), column("description", 100)]
        relation_columns["source"] = [column("payload", -1), column("description", 200)]

        adapter.expand_column_types(SOURCE, TARGET)

        assert executed == [
            "ALTER TABLE [warehouse].[dbo].[target] ALTER COLUMN [payload] varchar(max) NULL",
            "ALTER TABLE [warehouse].[dbo].[target] ALTER COLUMN [description] varchar(200) NULL",
        ]


class TestSyncAllColumnsHelper:
    """diff_column_data_types decides what on_schema_change: sync_all_columns alters."""

    def _diff(self, adapter, source_column, target_column):
        return adapter.execute_macro(
            "diff_column_data_types",
            kwargs={"source_columns": [source_column], "target_columns": [target_column]},
        )

    def test_varchar_max_source_widens_the_target(self, adapter):
        assert self._diff(adapter, column(char_size=-1), column(char_size=8000)) == [
            {"column_name": "payload", "new_type": "varchar(max)"}
        ]

    def test_varchar_max_target_is_left_alone(self, adapter):
        assert self._diff(adapter, column(char_size=100), column(char_size=-1)) == []


class TestAlterColumnTypeSql:
    def test_nullable_column_stays_nullable(self, adapter, relation_columns, executed):
        relation_columns["target"] = [column(is_nullable=True)]

        adapter.alter_column_type(TARGET, "payload", "varchar(max)")

        assert executed == [
            "ALTER TABLE [warehouse].[dbo].[target] ALTER COLUMN [payload] varchar(max) NULL"
        ]

    def test_collation_is_dropped_for_a_non_string_type(self, adapter, relation_columns, executed):
        relation_columns["target"] = [column(collation_name="Latin1_General_100_BIN2_UTF8")]

        adapter.alter_column_type(TARGET, "payload", "bigint")

        assert executed == [
            "ALTER TABLE [warehouse].[dbo].[target] ALTER COLUMN [payload] bigint NULL"
        ]

    @pytest.mark.parametrize(
        ("column_name", "quoted"),
        [("my-col", "[my-col]"), ("we]ird", "[we]]ird]"), ("with space", "[with space]")],
    )
    def test_column_names_are_quoted(
        self, adapter, relation_columns, executed, column_name, quoted
    ):
        relation_columns["target"] = [column(name=column_name)]

        adapter.alter_column_type(TARGET, column_name, "varchar(max)")

        assert f"ALTER COLUMN {quoted} varchar(max)" in executed[0]

    def test_identity_column_is_refused(self, adapter, relation_columns, executed):
        relation_columns["target"] = [column(identity=True)]

        with pytest.raises(CompilationError, match="identity column"):
            adapter.alter_column_type(TARGET, "payload", "varchar(max)")

        assert executed == []

    def test_unknown_nullability_is_refused(self, adapter, relation_columns, executed):
        relation_columns["target"] = [column(is_nullable=None)]

        with pytest.raises(CompilationError, match="nullability is unknown"):
            adapter.alter_column_type(TARGET, "payload", "varchar(max)")

        assert executed == []

    def test_unknown_column_is_refused(self, adapter, relation_columns, executed):
        relation_columns["target"] = [column(name="other_column")]

        with pytest.raises(CompilationError, match="does not exist"):
            adapter.alter_column_type(TARGET, "payload", "varchar(max)")

        assert executed == []
