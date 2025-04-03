from dbt.tests.adapter.utils.test_current_timestamp import (
    BaseCurrentTimestamp,
    BaseCurrentTimestampAware,
    BaseCurrentTimestampNaive,
)


class TestCurrentTimestampFabric(BaseCurrentTimestamp):
    pass


class TestCurrentTimestampAwareFabric(BaseCurrentTimestampAware):
    pass


class TestCurrentTimestampNaiveFabric(BaseCurrentTimestampNaive):
    pass
