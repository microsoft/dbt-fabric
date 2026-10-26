from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dbt_common.clients.jinja import get_template
from dbt_common.exceptions import CompilationError
from dbt_common.utils.jinja import get_dbt_macro_name

MACRO_PATH = (
    Path(__file__).parents[4] / "dbt/include/fabric/macros/materializations/functions/function.sql"
)


def test_overloaded_function_is_rejected_before_database_work():
    def raise_compiler_error(message):
        raise CompilationError(message)

    load_cached_relation = Mock(spec=[])
    context = {
        "model": {"overloads": [{"defined_in": "other_signature.sql"}]},
        "exceptions": SimpleNamespace(raise_compiler_error=raise_compiler_error),
        "load_cached_relation": load_cached_relation,
    }
    module = get_template(MACRO_PATH.read_text(), context).make_module(context)
    materialization = getattr(module, get_dbt_macro_name("materialization_function_fabric"))

    with pytest.raises(CompilationError, match="Fabric does not support overloaded UDFs"):
        materialization()

    load_cached_relation.assert_not_called()
