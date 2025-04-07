import pytest

from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonIncrementalTests,
    BasePythonModelTests,
)
from dbt.tests.adapter.python_model.test_spark import BasePySparkTests


@pytest.mark.skip("Python models are not supported in Fabric")
class TestPythonModelTestsFabric(BasePythonModelTests):
    pass


@pytest.mark.skip("Python models are not supported in Fabric")
class TestPythonIncrementalTestsFabric(BasePythonIncrementalTests):
    pass


@pytest.mark.skip("Python models are not supported in Fabric")
class TestPySparkTestsFabric(BasePySparkTests):
    pass
