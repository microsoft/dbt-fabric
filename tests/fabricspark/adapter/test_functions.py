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


class TestUDFsBasicFabricSpark(UDFsBasic):
    pass


class TestErrorForUnsupportedTypeFabricSpark(ErrorForUnsupportedType):
    pass


class TestPythonUDFNotSupportedFabricSpark(PythonUDFNotSupported):
    pass


class TestSqlUDFDefaultArgSupportFabricSpark(SqlUDFDefaultArgSupport):
    pass


class TestBasicSQLUDAFFabricSpark(BasicSQLUDAF):
    pass


class TestDeterministicUDFFabricSpark(DeterministicUDF):
    pass


class TestStableUDFFabricSpark(StableUDF):
    pass


class TestNonDeterministicUDFFabricSpark(NonDeterministicUDF):
    pass


class TestPythonUDFSupportedFabricSpark(PythonUDFSupported):
    pass


class TestPythonUDFRuntimeVersionRequiredFabricSpark(PythonUDFRuntimeVersionRequired):
    pass


class TestPythonUDFEntryPointRequiredFabricSpark(PythonUDFEntryPointRequired):
    pass


class TestPythonUDFDefaultArgSupportFabricSpark(PythonUDFDefaultArgSupport):
    pass


class TestPythonUDFVolatilitySupportFabricSpark(PythonUDFVolatilitySupport):
    pass


class TestBasicPythonUDAFFabricSpark(BasicPythonUDAF):
    pass


class TestPythonUDAFDefaultArgSupportFabricSpark(PythonUDAFDefaultArgSupport):
    pass
