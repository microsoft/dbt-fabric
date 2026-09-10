from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from jinja2 import Environment

from dbt.include.fabric import PACKAGE_PATH


@pytest.mark.parametrize(
    "column_list",
    ["[my-col]", "[my-col],\n[other-col]", "[space - col],\n[bracket]]-col]"],
)
def test_alter_column_type_preserves_passthrough_identifiers(column_list):
    run_query = Mock(return_value=SimpleNamespace(rows=[[column_list]]))
    statements = {}

    def statement(name, caller):
        statements[name] = caller()
        return ""

    template = Environment(extensions=["jinja2.ext.do"]).from_string(
        (Path(PACKAGE_PATH) / "macros" / "adapters" / "columns.sql").read_text()
    )
    module = template.make_module(
        {"run_query": run_query, "statement": statement, "log": lambda message: ""}
    )
    relation = SimpleNamespace(schema="dbo", identifier="example")
    module.fabric__alter_column_type(relation, "id", "varchar(8000)")

    run_query.assert_called_once()
    query = " ".join(run_query.call_args.args[0].split())
    # Add line breaks in the separator, without replacing characters in identifiers.
    assert "SELECT STRING_AGG(ColumnName, ',' + CHAR(10)) AS ColumnDef" in query
    assert column_list in statements["create_temp_table"]
    assert "CAST([id] AS varchar(8000)) AS [id]" in statements["create_temp_table"]
