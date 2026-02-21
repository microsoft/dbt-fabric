import pytest
from dbt_common.events.base_types import EventMsg

import dbt.tests.adapter.functions.files as files
from dbt.tests.adapter.functions.test_udafs import (
    BasicPythonUDAF,
    BasicSQLUDAF,
    PythonUDAFDefaultArgSupport,
)
from dbt.tests.adapter.functions.test_udfs import (
    DeterministicUDF,
    ErrorForUnsupportedType,
    NonDeterministicUDF,
    PythonUDFDefaultArgSupport,
    PythonUDFEntryPointRequired,
    PythonUDFNotSupported,
    PythonUDFRuntimeVersionRequired,
    PythonUDFSupported,
    PythonUDFVolatilitySupport,
    SqlUDFDefaultArgSupport,
    StableUDF,
    UDFsBasic,
)


class TestUDFsBasicFabric(UDFsBasic):
    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "price_for_xlarge"
            and "CREATE OR ALTER FUNCTION" in event.data.sql
        )

    def check_function_volatility(self, sql: str):
        pass

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": """
SELECT @price * 2
""".strip(),
            "price_for_xlarge.yml": files.MY_UDF_YML,
        }


@pytest.mark.skip(reason="Functions are not fully supported yet.")
class TestDeterministicUDFFabric(DeterministicUDF):
    pass


@pytest.mark.skip(reason="Functions are not fully supported yet.")
class TestStableUDFFabric(StableUDF):
    pass


@pytest.mark.skip(reason="Functions are not fully supported yet.")
class TestNonDeterministicUDFFabric(NonDeterministicUDF):
    pass


class TestErrorForUnsupportedTypeFabric(ErrorForUnsupportedType):
    pass


@pytest.mark.skip(reason="Functions are not fully supported yet.")
class TestPythonUDFSupportedFabric(PythonUDFSupported):
    pass


class TestPythonUDFNotSupportedFabric(PythonUDFNotSupported):
    pass


class TestPythonUDFRuntimeVersionRequiredFabric(PythonUDFRuntimeVersionRequired):
    pass


class TestPythonUDFEntryPointRequiredFabric(PythonUDFEntryPointRequired):
    pass


@pytest.mark.skip(reason="Functions are not fully supported yet.")
class TestSqlUDFDefaultArgSupportFabric(SqlUDFDefaultArgSupport):
    pass


@pytest.mark.skip(reason="Functions are not fully supported yet.")
class TestPythonUDFDefaultArgSupportFabric(PythonUDFDefaultArgSupport):
    pass


@pytest.mark.skip(reason="Functions are not fully supported yet.")
class TestPythonUDFVolatilitySupportFabric(PythonUDFVolatilitySupport):
    pass


@pytest.mark.skip(reason="Functions are not fully supported yet.")
class TestBasicPythonUDAFFabric(BasicPythonUDAF):
    pass


@pytest.mark.skip(reason="Functions are not fully supported yet.")
class TestBasicSQLUDAFFabric(BasicSQLUDAF):
    pass


@pytest.mark.skip(reason="Functions are not fully supported yet.")
class TestPythonUDAFDefaultArgSupportFabric(PythonUDAFDefaultArgSupport):
    pass
