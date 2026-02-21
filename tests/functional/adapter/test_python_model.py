import pytest

from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonIncrementalTests,
    BasePythonModelTests,
)
from dbt.tests.adapter.python_model.test_spark import BasePySparkTests


class TestPythonModelTestsFabric(BasePythonModelTests):
    pass


class TestPythonIncrementalTestsFabric(BasePythonIncrementalTests):
    pass


@pytest.mark.skip("Other DataFrames than PySpark are not supported in Fabric yet")
class TestPySparkTestsFabric(BasePySparkTests):
    pass
