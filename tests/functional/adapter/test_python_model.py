import pytest

from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonEmptyTests,
    BasePythonIncrementalTests,
    BasePythonModelTests,
    BasePythonSampleTests,
)
from dbt.tests.adapter.python_model.test_spark import BasePySparkTests

input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, cast('2025-01-01 01:25:00+00:00' as datetime2(6)) as event_time
UNION ALL
select 2 as id, cast('2025-01-02 13:47:00+00:00' as datetime2(6)) as event_time
UNION ALL
select 3 as id, cast('2025-01-03 01:32:00+00:00' as datetime2(6)) as event_time
"""


class TestPythonModelTestsFabric(BasePythonModelTests):
    pass


class TestPythonIncrementalTestsFabric(BasePythonIncrementalTests):
    pass


@pytest.mark.skip("Other DataFrames than PySpark are not supported in Fabric yet")
class TestPySparkTestsFabric(BasePySparkTests):
    pass


class FabricInputModel:
    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        """
        This is the SQL that defines the input model to be sampled, including any {{ config(..) }}.
        event_time is a required configuration of this input
        """
        return input_model_sql


class TestPythonEmptyTestsFabric(FabricInputModel, BasePythonEmptyTests):
    pass


class TestPythonSampleTestsFabric(FabricInputModel, BasePythonSampleTests):
    pass
