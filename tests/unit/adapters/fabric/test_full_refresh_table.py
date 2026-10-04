"""Regression tests for the `fabric__full_refresh_table` orchestration macro.

Unlike `fabric__create_table_as` and `fabric__atomic_reload_sql`, this macro had no
Jinja-level unit coverage at all: it decides, per run, whether to call
`adapter.get_table_refresh_plan` (existing table), or fall back to one of two
hardcoded plan dicts (`legacy_replace` for a relation-type change, or `replace` when
the target doesn't exist yet) -- and now threads `identity_column` through all three
paths. These tests render the real macro file with the surrounding dbt context
replaced by stubs, so the assertions cover which path is taken and what gets passed
along, not whether Fabric accepts the resulting SQL.
"""

import re
from pathlib import Path

import pytest
from dbt_common.clients.jinja import get_template
from dbt_common.exceptions import CompilationError
from dbt_common.exceptions.macros import MacroReturn
from dbt_common.utils.jinja import get_dbt_macro_name

from dbt.adapters.fabric.fabric_relation import FabricRelation
from dbt.adapters.fabric.table_refresh import IdentityColumn
from dbt.artifacts.resources import Contract

MACRO_DIR = Path(__file__).parents[4] / "dbt/include/fabric/macros/materializations/models/table"
REFRESH_MACRO_PATH = MACRO_DIR / "refresh.sql"
IDENTITY_MACRO_PATH = MACRO_DIR / "identity.sql"

TARGET_RELATION = FabricRelation.create(
    database="testdb", schema="dbo", identifier="my_model", type="table"
)
EXISTING_TABLE_RELATION = FabricRelation.create(
    database="testdb", schema="dbo", identifier="my_model", type="table"
)
EXISTING_VIEW_RELATION = FabricRelation.create(
    database="testdb", schema="dbo", identifier="my_model", type="view"
)

MODEL_SQL = "select id, amount from raw_orders"


class StubConfig:
    def __init__(self, values):
        self._values = values

    def get(self, name, default=None):
        return self._values.get(name, default)


class StubAdapter:
    """Stand-in for the dbt adapter surface `fabric__full_refresh_table` touches."""

    def __init__(
        self,
        macros,
        *,
        identity_column=None,
        refresh_plan=None,
        quote=lambda name: f"[{name}]",
    ):
        self._macros = macros
        self._identity_column = identity_column
        self._refresh_plan = refresh_plan
        self._quote = quote
        self.get_table_refresh_plan_calls = []
        self.renamed = []
        self.dropped = []

    def dispatch(self, macro_name, package=None):
        return self._macros[f"fabric__{macro_name}"]

    def quote(self, name):
        return self._quote(name)

    def get_identity_column(self, columns, contract_enforced=False):
        return self._identity_column

    def get_table_refresh_plan(
        self,
        existing_relation,
        compiled_code,
        cluster_by,
        constraints,
        columns,
        contract_enforced,
    ):
        self.get_table_refresh_plan_calls.append(
            {
                "existing_relation": existing_relation,
                "compiled_code": compiled_code,
                "cluster_by": cluster_by,
                "constraints": constraints,
                "columns": columns,
                "contract_enforced": contract_enforced,
            }
        )
        return self._refresh_plan

    def rename_relation(self, source, target):
        self.renamed.append((source, target))

    def drop_relation(self, relation):
        self.dropped.append(relation)


class StubExceptions:
    @staticmethod
    def raise_compiler_error(message):
        raise CompilationError(message)


def _normalize(sql):
    return re.sub(r"\s+", " ", sql).strip()


def render_full_refresh_table(
    *,
    existing_relation,
    contract_enforced=True,
    identity_column=None,
    refresh_plan=None,
    materialized="table",
    model_columns=None,
):
    """Render `fabric__full_refresh_table` from the macro file and return the emitted
    SQL plus the adapter stub, so both the chosen path and the plan it was fed can be
    asserted on."""
    source = REFRESH_MACRO_PATH.read_text()
    identity_source = IDENTITY_MACRO_PATH.read_text()
    module = {}

    def macro(name):
        def call(*args, **kwargs):
            try:
                return module[get_dbt_macro_name(name)](*args, **kwargs)
            except MacroReturn as macro_return:
                return macro_return.value

        return call

    macro_names = re.findall(r"{%\s*macro\s+(\w+)\(", source)
    macro_names += re.findall(r"{%\s*macro\s+(\w+)\(", identity_source)
    macros = {name: macro(name) for name in macro_names}

    emitted_statements = []

    def statement(name=None, fetch_result=False, auto_begin=True, language="sql", caller=None):
        sql = caller()
        emitted_statements.append(sql)
        return sql

    adapter = StubAdapter(macros, identity_column=identity_column, refresh_plan=refresh_plan)

    context = {
        "adapter": adapter,
        "config": StubConfig(
            {"contract": Contract(enforced=contract_enforced), "cluster_by": None}
        ),
        "exceptions": StubExceptions,
        "execute": True,
        "model": {
            "name": "my_model",
            "columns": model_columns if model_columns is not None else {"id": {}, "amount": {}},
            "constraints": [],
            "config": {"materialized": materialized},
        },
        "return": _macro_return,
        "log": lambda message, info=False: None,
        "statement": statement,
        "make_intermediate_relation": lambda relation: relation.incorporate(
            path={"identifier": relation.identifier + "__dbt_tmp"}
        ),
        "make_backup_relation": lambda relation, relation_type: relation.incorporate(
            path={"identifier": relation.identifier + "__dbt_backup"}, type=relation_type
        ),
        "load_cached_relation": lambda relation: None,
        "drop_relation_if_exists": lambda relation: None,
        "create_indexes": lambda relation: None,
        "create_table_as": lambda temporary, relation, sql, language="sql": (
            f"CREATE TABLE {relation} AS {sql};"
        ),
        "get_use_database_sql": lambda database: f"USE [{database}];",
        "get_create_view_as_sql": lambda relation, sql: f"CREATE VIEW {relation} AS {sql};",
        **macros,
    }
    template = get_template(source, context, capture_macros=True)
    module.update(template.make_module(vars=context, shared=False).__dict__)
    identity_template = get_template(identity_source, context, capture_macros=True)
    module.update(identity_template.make_module(vars=context, shared=False).__dict__)

    refresh_plan_result = macro("fabric__full_refresh_table")(
        TARGET_RELATION, existing_relation, MODEL_SQL, "sql"
    )

    return {
        "refresh_plan": refresh_plan_result,
        "sql": _normalize(" ".join(emitted_statements)),
        "adapter": adapter,
    }


def _macro_return(value):
    raise MacroReturn(value)


class TestFullRefreshTableWhenTargetDoesNotExist:
    """`existing_relation is none`: the hardcoded 'replace' fallback is used."""

    def test_identity_column_is_threaded_through_the_fallback_plan(self):
        identity_column = IdentityColumn(name="id", mode="auto")

        result = render_full_refresh_table(
            existing_relation=None,
            identity_column=identity_column,
        )

        assert result["refresh_plan"]["action"] == "replace"
        assert result["refresh_plan"]["reason"] == "target table does not exist"
        assert result["refresh_plan"]["identity_column"] == identity_column

    def test_no_identity_column_is_a_no_op(self):
        result = render_full_refresh_table(existing_relation=None, identity_column=None)

        assert result["refresh_plan"]["identity_column"] is None

    def test_creates_the_intermediate_table_and_swaps_it_via_sp_rename(self):
        """A 'replace' action (hardcoded or from `get_table_refresh_plan`) always
        swaps through `fabric_atomic_replace_sql`'s single CREATE + sp_rename
        statement, not through python-level `adapter.rename_relation` calls."""
        result = render_full_refresh_table(existing_relation=None, identity_column=None)

        assert "CREATE TABLE" in result["sql"]
        assert "__dbt_tmp" in result["sql"]
        assert "sp_rename" in result["sql"]
        # No existing relation to drop, and no python-level renames for this path.
        assert "DROP" not in result["sql"]
        assert result["adapter"].renamed == []


class TestFullRefreshTableWhenRelationTypeChanged:
    """`existing_relation` is a view but the model is now a table: 'legacy_replace'."""

    def test_identity_column_is_threaded_through_the_legacy_fallback_plan(self):
        identity_column = IdentityColumn(name="id", mode="insert")

        result = render_full_refresh_table(
            existing_relation=EXISTING_VIEW_RELATION,
            identity_column=identity_column,
        )

        assert result["refresh_plan"]["action"] == "legacy_replace"
        assert result["refresh_plan"]["reason"] == "relation type changed"
        assert result["refresh_plan"]["identity_column"] == identity_column

    def test_uses_the_rename_swap_path_not_atomic_replace(self):
        """legacy_replace swaps via two renames + drop, it never calls
        `fabric_atomic_replace_sql` (which does CREATE + DROP + sp_rename in one go)."""
        result = render_full_refresh_table(
            existing_relation=EXISTING_VIEW_RELATION, identity_column=None
        )

        assert "sp_rename" not in result["sql"]
        assert len(result["adapter"].renamed) == 2
        assert result["adapter"].dropped


class TestFullRefreshTableWhenExistingTableReloadPlanned:
    """`existing_relation` is a table: delegates to `adapter.get_table_refresh_plan`."""

    def test_passes_model_state_through_to_get_table_refresh_plan(self):
        render_full_refresh_table(
            existing_relation=EXISTING_TABLE_RELATION,
            contract_enforced=True,
            refresh_plan={
                "action": "reload",
                "reason": "schema and physical layout unchanged",
                "column_names": ["id", "amount"],
                "identity_column": None,
            },
        )

    def test_reload_plan_with_auto_identity_excludes_it_from_the_insert(self):
        identity_column = IdentityColumn(name="id", mode="auto")

        result = render_full_refresh_table(
            existing_relation=EXISTING_TABLE_RELATION,
            identity_column=identity_column,
            refresh_plan={
                "action": "reload",
                "reason": "schema and physical layout unchanged",
                "column_names": ["id", "amount"],
                "identity_column": identity_column,
            },
        )

        assert "INSERT INTO" in result["sql"]
        assert "([amount])" in result["sql"] or "(amount)" in result["sql"].replace(
            "[", ""
        ).replace("]", "")
        assert "SET IDENTITY_INSERT" not in result["sql"]

    def test_reload_plan_with_insert_identity_wraps_identity_insert_and_reseed(self):
        identity_column = IdentityColumn(name="id", mode="insert")

        result = render_full_refresh_table(
            existing_relation=EXISTING_TABLE_RELATION,
            identity_column=identity_column,
            refresh_plan={
                "action": "reload",
                "reason": "schema and physical layout unchanged",
                "column_names": ["id", "amount"],
                "identity_column": identity_column,
            },
        )

        assert "SET IDENTITY_INSERT" in result["sql"]
        assert "DBCC CHECKIDENT" in result["sql"]
        assert "RESEED" in result["sql"]
        # Fabric only supports the bare RESEED form -- no explicit reseed value.
        assert "RESEED," not in result["sql"]
        assert "RESEED)" in result["sql"] or "RESEED )" in result["sql"]

    def test_replace_plan_from_get_table_refresh_plan_uses_atomic_replace(self):
        identity_column = IdentityColumn(name="id", mode="auto")

        result = render_full_refresh_table(
            existing_relation=EXISTING_TABLE_RELATION,
            identity_column=identity_column,
            refresh_plan={
                "action": "replace",
                "reason": "identity column 'id' added",
                "column_names": ["id", "amount"],
                "identity_column": identity_column,
                "constraints_to_drop": [],
                "constraints_to_add": [],
                "constraint_add_sql": [],
            },
        )

        assert "sp_rename" in result["sql"]
        assert "DROP" in result["sql"]

    def test_get_table_refresh_plan_receives_columns_and_contract_enforced(self):
        result = render_full_refresh_table(
            existing_relation=EXISTING_TABLE_RELATION,
            contract_enforced=True,
            refresh_plan={
                "action": "reload",
                "reason": "schema and physical layout unchanged",
                "column_names": ["id", "amount"],
                "identity_column": None,
            },
        )

        [call] = result["adapter"].get_table_refresh_plan_calls
        assert call["contract_enforced"] is True
        assert call["columns"] == {"id": {}, "amount": {}}
        assert call["existing_relation"] == EXISTING_TABLE_RELATION


class TestFullRefreshTableIdentityGuard:
    """`fabric_assert_identity_supported` is called directly by this macro too, not
    only by `fabric__create_table_as` -- confirm it still fires here independently."""

    def test_raises_for_non_table_materialization(self):
        identity_column = IdentityColumn(name="id", mode="auto")

        with pytest.raises(CompilationError, match="only supported for materialized='table'"):
            render_full_refresh_table(
                existing_relation=None,
                identity_column=identity_column,
                materialized="incremental",
            )
