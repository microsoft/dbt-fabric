import pytest

from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonIncrementalTests,
    BasePythonModelTests,
)
from dbt.tests.adapter.python_model.test_spark import BasePySparkTests


class TestPythonModelTestsFabric(BasePythonModelTests):
    pass


@pytest.mark.skip("Support for incremental Python models is not yet implemented")
class TestPythonIncrementalTestsFabric(BasePythonIncrementalTests):
    pass


@pytest.mark.skip("We only support PySpark DataFrames for now")
class TestPySparkTestsFabric(BasePySparkTests):
    pass
