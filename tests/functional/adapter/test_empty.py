import pytest
from dbt.tests.adapter.empty.test_empty import BaseTestEmpty


@pytest.mark.skip(
    reason="render_limited() defaults to dbt-core implementation instead of using Fabric implementation"
)
class TestEmpty(BaseTestEmpty):
    pass
