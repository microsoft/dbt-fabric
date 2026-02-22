import pytest

from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonEmptyTests,
    BasePythonIncrementalTests,
    BasePythonModelTests,
    BasePythonSampleTests,
)
from dbt.tests.adapter.python_model.test_spark import BasePySparkTests


class TestPythonModelTestsFabric(BasePythonModelTests):
    pass


class TestPythonIncrementalTestsFabric(BasePythonIncrementalTests):
    pass


@pytest.mark.skip("Other DataFrames than PySpark are not supported in Fabric yet")
class TestPySparkTestsFabric(BasePySparkTests):
    pass


@pytest.mark.skip("TODO: Failing test - to investigate")
class TestPythonEmptyTestsFabric(BasePythonEmptyTests):
    pass


@pytest.mark.skip("TODO: Failing test - to investigate")
class TestPythonSampleTestsFabric(BasePythonSampleTests):
    pass
