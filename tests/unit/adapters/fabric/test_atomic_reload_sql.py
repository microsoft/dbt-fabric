"""Regression tests for the `fabric__atomic_reload_sql` macro (issue #453).

The real macro file is rendered through the Jinja environment dbt uses, with the
surrounding dbt context replaced by stubs, so the assertions cover the SQL the adapter
emits. The stubs stand in for dbt and the warehouse: nothing here connects to Fabric,
so these tests say nothing about which queries Fabric accepts. That verdict belongs to
the engine, which returns it when dbt executes the emitted SQL.
"""

import re
from pathlib import Path

import pytest
from dbt_common.clients.jinja import get_template
from dbt_common.exceptions import DbtDatabaseError
from dbt_common.exceptions.macros import MacroReturn
from dbt_common.utils.jinja import get_dbt_macro_name

from dbt.adapters.fabric.fabric_relation import FabricRelation
from dbt.adapters.fabric.table_refresh import IdentityColumn

MACRO_PATH = (
    Path(__file__).parents[4]
    / "dbt/include/fabric/macros/materializations/models/table/refresh.sql"
)
IDENTITY_MACRO_PATH = (
    Path(__file__).parents[4]
    / "dbt/include/fabric/macros/materializations/models/table/identity.sql"
)

TARGET_RELATION = FabricRelation.create(
    database="testdb", schema="dbo", identifier="my_model", type="table"
)

COLUMN_NAMES = ["id", "amount"]

LEADING_CTE_MODEL = """
with source as (
    select id, amount from raw_orders
)
select id, amount from source
""".strip()

NESTED_CTE_MODEL = """
with outer_cte as (
    with inner_cte as (
        select id, amount from raw_orders
    )
    select id, amount from inner_cte
)
select id, amount from outer_cte
""".strip()

NO_CTE_MODEL = "select id, amount from raw_orders".strip()

MODEL_QUERIES = [
    pytest.param(LEADING_CTE_MODEL, id="leading_cte"),
    pytest.param(NESTED_CTE_MODEL, id="nested_cte"),
    pytest.param(NO_CTE_MODEL, id="no_cte"),
]


class StubAdapter:
    """Stand-in for the dbt adapter: dispatch plus the one warehouse call the macro makes."""

    def __init__(self, macros, drop_relation, quote=lambda name: f"[{name}]"):
        self._macros = macros
        self._drop_relation = drop_relation
        self._quote = quote

    def dispatch(self, macro_name, package=None):
        return self._macros[f"fabric__{macro_name}"]

    def drop_relation(self, relation):
        return self._drop_relation(relation)

    def quote(self, name):
        return self._quote(name)


def render_atomic_reload_sql(
    compiled_code,
    column_names=COLUMN_NAMES,
    *,
    drop_relation=lambda relation: None,
    identity_column=None,
):
    """Render fabric__atomic_reload_sql from the macro file and return the emitted SQL."""
    source = MACRO_PATH.read_text()
    identity_source = IDENTITY_MACRO_PATH.read_text()
    module = {}

    def macro(name):
        # dbt renames macros per file and resolves calls between them through the
        # context, where `{{ return(...) }}` is caught instead of unwinding the caller.
        def call(*args, **kwargs):
            try:
                return module[get_dbt_macro_name(name)](*args, **kwargs)
            except MacroReturn as macro_return:
                return macro_return.value

        return call

    macro_names = re.findall(r"{%\s*macro\s+(\w+)\(", source)
    macro_names += re.findall(r"{%\s*macro\s+(\w+)\(", identity_source)
    macros = {name: macro(name) for name in macro_names}
    context = {
        # Mirrors the context dbt renders the macro with, `execute` included, so any
        # revision of the macro file renders here unchanged.
        "adapter": StubAdapter(macros, drop_relation),
        "execute": True,
        "return": _macro_return,
        "get_use_database_sql": lambda database: f"USE [{database}];",
        "get_create_view_as_sql": lambda relation, sql: f"CREATE VIEW {relation} AS {sql};",
        **macros,
    }
    template = get_template(source, context, capture_macros=True)
    module.update(template.make_module(vars=context, shared=False).__dict__)
    identity_template = get_template(identity_source, context, capture_macros=True)
    module.update(identity_template.make_module(vars=context, shared=False).__dict__)

    return _normalize(
        macro("fabric__atomic_reload_sql")(
            TARGET_RELATION, compiled_code, column_names, identity_column
        )
    )


def _macro_return(value):
    raise MacroReturn(value)


def _normalize(sql):
    return re.sub(r"\s+", " ", sql).strip()


class TestAtomicReloadSql:
    @pytest.mark.parametrize("compiled_code", MODEL_QUERIES)
    def test_insert_never_follows_directly_with_with(self, compiled_code):
        """The literal bug from #453: `INSERT INTO t (...) WITH cte ...` is invalid
        T-SQL. The fix must never emit WITH immediately after the INSERT column list,
        regardless of whether the model query has a leading CTE."""
        sql = render_atomic_reload_sql(compiled_code)

        assert not re.search(r"\)\s*WITH\b", sql, re.IGNORECASE)

    @pytest.mark.parametrize("compiled_code", MODEL_QUERIES)
    def test_model_sql_is_loaded_through_a_temporary_view(self, compiled_code):
        sql = render_atomic_reload_sql(compiled_code)

        assert (
            "CREATE VIEW [testdb].[dbo].[my_model__dbt_reload_vw] AS "
            f"{_normalize(compiled_code)};" in sql
        )

    @pytest.mark.parametrize("compiled_code", MODEL_QUERIES)
    def test_insert_selects_from_the_view(self, compiled_code):
        sql = render_atomic_reload_sql(compiled_code)

        assert (
            "TRUNCATE TABLE [dbo].[my_model]; "
            "INSERT INTO [dbo].[my_model] ([id], [amount]) "
            "SELECT [id], [amount] FROM [dbo].[my_model__dbt_reload_vw]; "
            "DROP VIEW IF EXISTS [dbo].[my_model__dbt_reload_vw];" in sql
        )

    def test_database_error_propagates(self):
        def reject_relation(relation):
            raise DbtDatabaseError("Invalid object name 'my_model__dbt_reload_vw'")

        with pytest.raises(DbtDatabaseError, match="Invalid object name"):
            render_atomic_reload_sql(LEADING_CTE_MODEL, drop_relation=reject_relation)

    def test_drop_relation_is_called_for_the_temp_view(self):
        dropped = []
        render_atomic_reload_sql(LEADING_CTE_MODEL, drop_relation=dropped.append)

        assert len(dropped) == 1
        assert str(dropped[0]) == "[testdb].[dbo].[my_model__dbt_reload_vw]"


class TestAtomicReloadSqlIdentity:
    """Regression tests for Fabric IDENTITY column support during reload (`meta.identity`)."""

    def test_auto_mode_excludes_identity_column_from_insert_and_select(self):
        sql = render_atomic_reload_sql(
            NO_CTE_MODEL,
            identity_column=IdentityColumn(name="id", mode="auto"),
        )

        assert (
            "INSERT INTO [dbo].[my_model] ([amount]) "
            "SELECT [amount] FROM [dbo].[my_model__dbt_reload_vw]; " in sql
        )
        assert "SET IDENTITY_INSERT" not in sql
        assert "DBCC CHECKIDENT" not in sql

    def test_insert_mode_keeps_identity_column_and_wraps_with_identity_insert(self):
        sql = render_atomic_reload_sql(
            NO_CTE_MODEL,
            identity_column=IdentityColumn(name="id", mode="insert"),
        )

        assert (
            "INSERT INTO [dbo].[my_model] ([id], [amount]) "
            "SELECT [id], [amount] FROM [dbo].[my_model__dbt_reload_vw]; " in sql
        )
        assert re.search(
            r"TRUNCATE TABLE \[dbo\]\.\[my_model\];\s*"
            r"SET IDENTITY_INSERT \[dbo\]\.\[my_model\] ON;\s*"
            r"INSERT INTO \[dbo\]\.\[my_model\]",
            sql,
        )
        assert re.search(
            r"SELECT \[id\], \[amount\] FROM \[dbo\]\.\[my_model__dbt_reload_vw\];\s*"
            r"SET IDENTITY_INSERT \[dbo\]\.\[my_model\] OFF;",
            sql,
        )
        assert "DBCC CHECKIDENT" in sql
        assert "RESEED" in sql

    def test_no_identity_column_is_a_no_op(self):
        sql = render_atomic_reload_sql(NO_CTE_MODEL, identity_column=None)

        assert "SET IDENTITY_INSERT" not in sql
        assert "DBCC CHECKIDENT" not in sql
