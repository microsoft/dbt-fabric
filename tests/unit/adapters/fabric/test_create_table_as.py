"""Regression tests for the `fabric__create_table_as` macro (issue #387).

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
from dbt_common.exceptions import CompilationError, DbtDatabaseError
from dbt_common.exceptions.macros import MacroReturn
from dbt_common.utils.jinja import get_dbt_macro_name

from dbt.adapters.fabric.fabric_relation import FabricRelation
from dbt.artifacts.resources import Contract

MACRO_PATH = (
    Path(__file__).parents[4]
    / "dbt/include/fabric/macros/materializations/models/table/create_table_as.sql"
)

TARGET_RELATION = FabricRelation.create(
    database="testdb", schema="dbo", identifier="my_model", type="table"
)

NESTED_CTE_MODEL = """
with outer_cte as (
    with inner_cte as (
        select id, amount from raw_orders
    )
    select id, amount from inner_cte
)
select id, amount from outer_cte
""".strip()

TABLE_HINT_MODEL = """
with source as (
    select id, amount from raw_orders with (nolock)
)
select id, amount from source with (nolock)
""".strip()

COMMENTED_MODEL = """
-- reconciled with raw_lines
with source as (
    select id, amount, 'invoiced with credit' as label from raw_orders
)
select id, amount from source
""".strip()

MODEL_QUERIES = [
    pytest.param(NESTED_CTE_MODEL, id="nested_cte"),
    pytest.param(TABLE_HINT_MODEL, id="table_hints"),
    pytest.param(COMMENTED_MODEL, id="comment_and_literal"),
]


class StubConfig:
    """Stand-in for the dbt `config` context object."""

    def __init__(self, values):
        self._values = values

    def get(self, name, default=None):
        return self._values.get(name, default)


class StubAdapter:
    """Stand-in for the dbt adapter: dispatch plus the one warehouse call the macro makes."""

    def __init__(self, macros, drop_relation):
        self._macros = macros
        self._drop_relation = drop_relation

    def dispatch(self, macro_name, package=None):
        return self._macros[f"fabric__{macro_name}"]

    def drop_relation(self, relation):
        return self._drop_relation(relation)


class StubExceptions:
    @staticmethod
    def raise_compiler_error(message):
        raise CompilationError(message)


def render_create_table_as(
    compiled_code,
    *,
    contract_enforced=False,
    get_assert_columns_equivalent=lambda sql: "",
    drop_relation=lambda relation: None,
):
    """Render fabric__create_table_as from the macro file and return the emitted SQL."""
    source = MACRO_PATH.read_text()
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

    macros = {name: macro(name) for name in re.findall(r"{%\s*macro\s+(\w+)\(", source)}
    context = {
        # Mirrors the context dbt renders the macro with, `execute` included, so any
        # revision of the macro file renders here unchanged.
        "adapter": StubAdapter(macros, drop_relation),
        "config": StubConfig({"contract": Contract(enforced=contract_enforced)}),
        "exceptions": StubExceptions,
        "execute": True,
        "model": {"columns": {"id": {}, "amount": {}}},
        "return": _macro_return,
        "build_columns_constraints": lambda relation: "([id] int, [amount] int)",
        "get_assert_columns_equivalent": get_assert_columns_equivalent,
        "get_create_view_as_sql": lambda relation, sql: f"CREATE VIEW {relation} AS {sql};",
        **macros,
    }
    template = get_template(source, context, capture_macros=True)
    module.update(template.make_module(vars=context, shared=False).__dict__)

    return _normalize(macro("fabric__create_table_as")(False, TARGET_RELATION, compiled_code))


def _macro_return(value):
    raise MacroReturn(value)


def _normalize(sql):
    return re.sub(r"\s+", " ", sql).strip()


class TestCreateTableAs:
    @pytest.mark.parametrize("compiled_code", MODEL_QUERIES)
    def test_ctas_keeps_the_model_query(self, compiled_code):
        sql = render_create_table_as(compiled_code)

        assert sql == f"CREATE TABLE [testdb].[dbo].[my_model] AS {_normalize(compiled_code)}"

    @pytest.mark.parametrize("compiled_code", MODEL_QUERIES)
    def test_contract_loads_through_a_temporary_view(self, compiled_code):
        sql = render_create_table_as(compiled_code, contract_enforced=True)

        assert sql.startswith("CREATE TABLE [testdb].[dbo].[my_model] ([id] int, [amount] int)")
        assert (
            "CREATE VIEW [testdb].[dbo].[my_model__dbt_tmp_vw] AS "
            f"{_normalize(compiled_code)};" in sql
        )
        assert (
            "INSERT INTO [testdb].[dbo].[my_model] ( [id], [amount] ) "
            "SELECT [id], [amount] FROM [testdb].[dbo].[my_model__dbt_tmp_vw]; "
            "DROP VIEW IF EXISTS [dbo].[my_model__dbt_tmp_vw];" in sql
        )

    def test_contract_validation_error_propagates(self):
        def reject_columns(sql):
            raise CompilationError("Contract columns do not match")

        with pytest.raises(CompilationError, match="Contract columns do not match"):
            render_create_table_as(
                NESTED_CTE_MODEL,
                contract_enforced=True,
                get_assert_columns_equivalent=reject_columns,
            )

    def test_database_error_propagates(self):
        def reject_relation(relation):
            raise DbtDatabaseError("Invalid object name 'my_model__dbt_tmp_vw'")

        with pytest.raises(DbtDatabaseError, match="Invalid object name"):
            render_create_table_as(
                NESTED_CTE_MODEL,
                contract_enforced=True,
                drop_relation=reject_relation,
            )
