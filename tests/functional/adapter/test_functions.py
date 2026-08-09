import pytest
from dbt_common.events.base_types import EventMsg
from dbt_common.events.event_catcher import EventCatcher

from dbt.artifacts.schemas.results import RunStatus
from dbt.contracts.graph.nodes import FunctionNode
from dbt.events.types import JinjaLogWarning
from dbt.tests.adapter.functions import files
from dbt.tests.adapter.functions.test_udfs import (
    CanFindScalarFunctionRelation,
    ErrorForUnsupportedType,
    PythonUDFNotSupported,
    SqlUDFDefaultArgSupport,
    UDFsBasic,
)
from dbt.tests.util import run_dbt

FABRIC_UDF_SQL = "SELECT @price * 2"


class FabricSqlUDF:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": FABRIC_UDF_SQL,
            "price_for_xlarge.yml": files.MY_UDF_YML,
        }

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "price_for_xlarge"
            and "CREATE OR ALTER FUNCTION" in event.data.sql
        )


class TestFabricUDFs(FabricSqlUDF, UDFsBasic):
    pass


class FabricUnsupportedVolatility(FabricSqlUDF, UDFsBasic):
    volatility: str

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"functions": {"+volatility": self.volatility}}

    def test_udfs(self, project, sql_event_catcher):
        warning_event_catcher = EventCatcher(JinjaLogWarning)
        result = run_dbt(
            ["build", "--debug"],
            callbacks=[sql_event_catcher.catch, warning_event_catcher.catch],
        )

        assert len(result.results) == 1
        node_result = result.results[0]
        assert node_result.status == RunStatus.Success
        assert isinstance(node_result.node, FunctionNode)

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "VOLATILE" not in sql
        assert "STABLE" not in sql
        assert "IMMUTABLE" not in sql

        assert len(warning_event_catcher.caught_events) == 1
        assert (
            f"Found `{self.volatility}` volatility specified on function "
            "`price_for_xlarge`. This volatility is not supported by fabric, "
            "and will be ignored" in warning_event_catcher.caught_events[0].data.msg
        )

        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert len(result.results) == 1
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 200


class TestFabricDeterministicUDFs(FabricUnsupportedVolatility):
    volatility = "deterministic"


class TestFabricStableUDFs(FabricUnsupportedVolatility):
    volatility = "stable"


class TestFabricNonDeterministicUDFs(FabricUnsupportedVolatility):
    volatility = "non-deterministic"


class TestFabricErrorForUnsupportedType(FabricSqlUDF, ErrorForUnsupportedType):
    pass


class TestFabricPythonUDFNotSupported(PythonUDFNotSupported):
    pass


class TestFabricDefaultArgsSupportSQLUDFs(FabricSqlUDF, SqlUDFDefaultArgSupport):
    expect_default_arg_support = True

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": FABRIC_UDF_SQL,
            "price_for_xlarge.yml": files.MY_UDF_WITH_DEFAULT_ARG_YML,
        }

    def test_udfs(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success
        assert len(sql_event_catcher.caught_events) == 1
        assert "@price float = 100" in sql_event_catcher.caught_events[0].data.sql

        result = run_dbt(
            ["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(DEFAULT)"]
        )
        assert len(result.results) == 1
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 200


class TestFabricCanFindScalarFunctionRelation(FabricSqlUDF, CanFindScalarFunctionRelation):
    pass
