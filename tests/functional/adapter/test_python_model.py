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


class TestPySparkTestsFabric(BasePySparkTests):
    pass
