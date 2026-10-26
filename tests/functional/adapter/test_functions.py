import pytest
from dbt_common.events.base_types import EventMsg
from dbt_common.events.event_catcher import EventCatcher

from dbt.artifacts.schemas.results import RunStatus
from dbt.contracts.graph.nodes import FunctionNode
from dbt.events.types import JinjaLogWarning
from dbt.exceptions import ParsingError
from dbt.tests.adapter.functions import files
from dbt.tests.adapter.functions.test_udfs import (
    CanFindScalarFunctionRelation,
    ErrorForUnsupportedType,
    PythonUDFNotSupported,
    SqlUDFDefaultArgSupport,
    UDFsBasic,
)
from dbt.tests.util import run_dbt, write_file

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


class TestFabricUDFPartialParsing(FabricSqlUDF):
    def test_changed_function_body(self, project):
        run_dbt(["build"])
        write_file("SELECT @price * 3", project.project_root, "functions", "price_for_xlarge.sql")
        run_dbt(["build"])

        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 300


class TestFabricJavaScriptUDFNotSupported:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": "return price * 2;",
            "price_for_xlarge.yml": files.MY_UDF_YML,
        }

    def test_javascript_udf_is_rejected(self, project):
        with pytest.raises(
            ParsingError,
            match="Function 'price_for_xlarge' uses JavaScript, which is not supported on 'fabric'",
        ):
            run_dbt(["build"], expect_pass=False)


class TestFabricOverloadedUDFNotSupported:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": FABRIC_UDF_SQL,
            "price_for_xlarge_int.sql": FABRIC_UDF_SQL,
            "price_for_xlarge.yml": """
functions:
  - name: price_for_xlarge
    config:
      type: scalar
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
    overloads:
      - defined_in: price_for_xlarge_int
        arguments:
          - name: price
            data_type: int
        returns:
          data_type: int
""",
        }

    def test_overloaded_udf_is_rejected(self, project):
        result = run_dbt(["build"], expect_pass=False)

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Error
        assert "Fabric does not support overloaded UDFs" in result.results[0].message
        with project.adapter.connection_named("check_function"):
            relation = project.adapter.get_relation(
                database=project.database,
                schema=project.test_schema,
                identifier="price_for_xlarge",
            )
        assert relation is None
