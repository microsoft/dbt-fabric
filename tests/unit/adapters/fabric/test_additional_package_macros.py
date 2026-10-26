import hashlib
import json
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dbt_common.clients.jinja import get_template
from dbt_common.exceptions import CompilationError
from dbt_common.exceptions.macros import MacroReturn
from dbt_common.utils.jinja import get_dbt_macro_name

from dbt.adapters.fabric.fabric_column import FabricColumn
from dbt.adapters.fabric.fabric_relation import FabricRelation

MACROS = Path(__file__).parents[4] / "dbt/include/fabric/macros/dbt_package_support"
ARTIFACTS = "dbt_artifacts/upload.sql"
CONSTRAINTS = "dbt_constraints/constraints.sql"
AUDIT = "dbt_audit_helper/compare_relation_columns.sql"
PROFILER = "dbt_profiler/get_profile.sql"
EXTERNAL = "dbt_external_tables/external_tables.sql"
RELATION = FabricRelation.create(database="db", schema="dbo", identifier="parent")


def macro_return(value):
    raise MacroReturn(value)


def compiler_error(message):
    raise CompilationError(message)


def load(path, **overrides):
    source = (MACROS / path).read_text()
    module = {}

    def call(name):
        def invoke(*args, **kwargs):
            try:
                return module[get_dbt_macro_name(name)](*args, **kwargs)
            except MacroReturn as result:
                return result.value

        return invoke

    macros = {name: call(name) for name in re.findall(r"{%[-\s]*macro\s+(\w+)\(", source)}
    context = {
        **macros,
        "return": macro_return,
        "execute": True,
        "adapter": SimpleNamespace(
            quote=lambda value: "[" + value.replace("]", "]]") + "]",
            get_columns_in_relation=Mock(spec=[], return_value=[]),
        ),
        "exceptions": SimpleNamespace(raise_compiler_error=compiler_error, warn=Mock(spec=[])),
        "run_query": Mock(spec=[]),
        "api": SimpleNamespace(Relation=FabricRelation),
        "local_md5": lambda value: hashlib.md5(value.encode()).hexdigest(),
        "get_use_database_sql": lambda value: f"USE [{value}];",
        **overrides,
    }
    template = get_template(source, context, capture_macros=True)
    module.update(template.make_module(context).__dict__)
    return SimpleNamespace(**macros), context


@pytest.mark.parametrize(
    "kind, column_count", [("model", 16), ("seed", 16), ("snapshot", 16), ("test", 13)]
)
@pytest.mark.parametrize(
    "timing",
    [
        [],
        [
            {"name": "compile", "started_at": "2026-01-01 00:00:01"},
            {"name": "execute", "completed_at": "2026-01-01 00:00:02"},
        ],
    ],
)
def test_artifact_execution_serializers_preserve_results(kind, column_count, timing):
    macros, _ = load(
        ARTIFACTS,
        invocation_id="invocation",
        run_started_at="2026-01-01",
        flags=SimpleNamespace(FULL_REFRESH=True),
        tojson=json.dumps,
    )
    node = SimpleNamespace(
        node=SimpleNamespace(
            unique_id="test.id",
            config=SimpleNamespace(full_refresh=None, materialized="table"),
            schema="owner's",
            name="name",
            alias="alias",
        ),
        timing=timing,
        thread_id="thread",
        status="success",
        execution_time=1.5,
        adapter_response={"rows_affected": 7},
        failures="",
        message="O'Brien",
    )
    sql = getattr(macros, f"fabric__get_{kind}_executions_dml_sql")([node])
    assert len(re.findall(r"\[c\d+\]", sql)) == column_count
    assert "'7'" in sql
    assert "'O''Brien'" in sql
    assert "False" not in sql and "True" not in sql
    if timing:
        assert "'2026-01-01 00:00:01'" in sql
        assert "'2026-01-01 00:00:02'" in sql
    else:
        assert sql.count("null") >= 2


def test_artifact_upload_executes_insert_and_empty_results_are_empty():
    macros, context = load(ARTIFACTS)
    assert macros.fabric_artifacts_execution_sql([], "test") == ""
    macros.fabric__insert_into_metadata_table("relation", "(id)", "select 1")
    context["run_query"].assert_called_once_with("insert into relation (id)\nselect 1")


@pytest.mark.parametrize(
    "kind", ["invocations", "exposures", "models", "seeds", "snapshots", "sources", "tests"]
)
def test_artifact_metadata_delegates_to_tsql_serializer(kind):
    serializer = Mock(spec=[], return_value="select metadata")
    macros, _ = load(
        ARTIFACTS, dbt_artifacts=SimpleNamespace(**{f"sqlserver__get_{kind}_dml_sql": serializer})
    )
    assert getattr(macros, f"fabric__get_{kind}_dml_sql")(["node"]) == "select metadata"
    serializer.assert_called_once_with(["node"])


def constraint_package(**overrides):
    return SimpleNamespace(
        sanitize_constraint_name=lambda value: re.sub("[^A-Z0-9_$]", "_", value.upper()),
        unique_constraint_exists=Mock(spec=[], return_value=False),
        have_ownership_priv=Mock(spec=[], return_value=True),
        have_references_priv=Mock(spec=[], return_value=True),
        column_list_matches=lambda a, b: {v.upper() for v in a} == {v.upper() for v in b},
        get_results_group_by=lambda result, key: {},
        **overrides,
    )


@pytest.mark.parametrize("kind", ["PRIMARY KEY", "UNIQUE"])
def test_constraint_keys_use_fabric_non_enforced_syntax(kind):
    macros, context = load(CONSTRAINTS, dbt_constraints=constraint_package())
    macros.fabric_constraint_create_key(RELATION, ["odd]name"], True, None, {}, kind)
    sql = context["run_query"].call_args.args[0]
    assert f"{kind} nonclustered ([odd]]name]) not enforced" in sql


def test_constraint_names_are_deterministic_and_fit_identifier_limit():
    macros, _ = load(CONSTRAINTS, dbt_constraints=constraint_package())
    name = macros.fabric_constraint_name(RELATION, ["a" * 150], "PK")
    assert len(name[1:-1]) == 128
    assert name == macros.fabric_constraint_name(RELATION, ["a" * 150], "PK")
    assert name != macros.fabric_constraint_name(RELATION, ["a" * 149 + "b"], "PK")


@pytest.mark.parametrize("allowed", [0, 1, None])
def test_constraint_permission_checks_do_not_assume_access(allowed):
    macros, context = load(
        CONSTRAINTS, run_query=Mock(spec=[], return_value=SimpleNamespace(rows=[[allowed]]))
    )
    assert macros.fabric_constraint_has_permission(RELATION, "ALTER", True) is (allowed == 1)
    context["run_query"].reset_mock()
    assert macros.fabric_constraint_has_permission(RELATION, "ALTER", False) is True
    context["run_query"].assert_not_called()


def test_constraint_permission_denial_warns_without_running_ddl():
    package = constraint_package()
    package.have_ownership_priv.return_value = False
    macros, context = load(CONSTRAINTS, dbt_constraints=package)
    macros.fabric_constraint_create_key(RELATION, ["id"], True, None, {}, "PRIMARY KEY")
    context["run_query"].assert_not_called()
    assert "ALTER permission" in context["exceptions"].warn.call_args.args[0]


def test_foreign_key_matching_checks_referenced_columns():
    package = constraint_package()
    package.get_results_group_by = lambda result, key: {
        "fk": [SimpleNamespace(column_name="child_id", referenced_column="different_id")]
    }
    macros, _ = load(CONSTRAINTS, dbt_constraints=package)
    assert macros.fabric_constraint_foreign_key_exists(RELATION, ["child_id"]) is True
    assert (
        macros.fabric_constraint_foreign_key_exists(RELATION, ["child_id"], RELATION, ["id"])
        is False
    )


@pytest.mark.parametrize("nullable", [True, False])
def test_not_null_requires_rebuild_only_for_nullable_columns(nullable):
    macros, context = load(CONSTRAINTS)
    context["adapter"].get_columns_in_relation.return_value = [
        FabricColumn("id", "int", is_nullable=nullable)
    ]
    macros.fabric__create_not_null(RELATION, ["id"], True, False, {}, "")
    assert context["exceptions"].warn.call_count == int(nullable)
    context["run_query"].assert_not_called()


def test_cross_database_foreign_keys_fail_before_running_sql():
    macros, context = load(CONSTRAINTS)
    other = RELATION.replace_path(database="other")
    with pytest.raises(CompilationError, match="same database"):
        macros.fabric__create_foreign_key(
            RELATION, ["id"], other, ["id"], True, False, None, {}, ""
        )
    context["run_query"].assert_not_called()


def test_constraint_release_refuses_to_drop_foreign_keys_on_unselected_tables():
    lookup = SimpleNamespace(
        rows=[SimpleNamespace(constraint_name="fk", schema_name="dbo", table_name="child")]
    )
    macros, context = load(CONSTRAINTS, run_query=Mock(spec=[], return_value=lookup))
    with pytest.raises(CompilationError, match="unselected table dbo.child"):
        macros.fabric__release_constraints_for_rebuild([RELATION])
    assert context["run_query"].call_count == 1


def test_constraint_release_deduplicates_foreign_key_drops():
    child = RELATION.replace_path(identifier="child")
    lookup = SimpleNamespace(
        rows=[SimpleNamespace(constraint_name="fk", schema_name="dbo", table_name="child")]
    )
    query = Mock(spec=[], return_value=lookup)
    macros, _ = load(CONSTRAINTS, run_query=query)
    macros.fabric__release_constraints_for_rebuild([RELATION, child])
    assert query.call_count == 3
    assert query.call_args.args[0] == "alter table [db].[dbo].[child] drop constraint [fk]"


@pytest.mark.parametrize("execute", [True, False])
def test_empty_audit_metadata_is_valid_typed_empty_sql(execute):
    macros, context = load(AUDIT, execute=execute)
    sql = macros.fabric__get_columns_in_relation_sql(RELATION)
    assert "where 1 = 0" in sql and "values" not in sql.lower()
    if not execute:
        context["adapter"].get_columns_in_relation.assert_not_called()


def test_audit_metadata_preserves_type_parameters_and_escapes_names():
    macros, context = load(AUDIT)
    context["adapter"].get_columns_in_relation.return_value = [
        FabricColumn("owner's", "varchar", char_size=45),
        FabricColumn("amount", "decimal", numeric_precision=12, numeric_scale=3),
    ]
    sql = macros.fabric__get_columns_in_relation_sql(RELATION)
    assert "'owner''s'" in sql and "'varchar(45)'" in sql and "'decimal(12,3)'" in sql
    comparison = macros.fabric__compare_relation_columns(RELATION, RELATION)
    assert all(f"as {name}" in comparison for name in ("in_a_only", "in_b_only", "in_both"))


def test_profiler_invalid_column_selections_raise_explicitly():
    macros, _ = load(PROFILER)
    with pytest.raises(CompilationError, match="only one"):
        macros.fabric__get_profile(RELATION, include_columns=["a"], exclude_columns=["b"])
    with pytest.raises(CompilationError, match="No columns selected"):
        macros.fabric__get_profile(RELATION)


def test_external_schema_escapes_identifiers_and_dynamic_sql_literals():
    macros, _ = load(EXTERNAL)
    sql = macros.fabric__create_external_schema(
        SimpleNamespace(database="db", schema="odd]'schema")
    )
    assert "where name = 'odd]''schema'" in sql
    assert "exec('create schema [odd]]''schema]')" in sql
