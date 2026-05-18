import pytest
from dbt.tests.adapter.sample_mode.test_sample_mode import BaseSampleModeTest

# The upstream fixture uses TIMESTAMP '...' syntax which is not valid T-SQL.
# Override input_model_sql to use CAST(... AS datetime2) instead.
_input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, CAST('2025-01-01 01:25:00' AS datetime2(6)) as event_time
UNION ALL
select 2 as id, CAST('2025-01-02 13:47:00' AS datetime2(6)) as event_time
UNION ALL
select 3 as id, CAST('2025-01-03 01:32:00' AS datetime2(6)) as event_time
"""


class TestSampleModeFabric(BaseSampleModeTest):
    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        return _input_model_sql
