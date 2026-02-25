from dbt.tests.adapter.ephemeral.test_ephemeral import (
    BaseEphemeralErrorHandling,
    BaseEphemeralMulti,
    BaseEphemeralNested,
)


class TestEphemeralFabricSpark(BaseEphemeralMulti):
    pass


class TestEphemeralNestedFabricSpark(BaseEphemeralNested):
    pass


class TestEphemeralErrorHandlingFabricSpark(BaseEphemeralErrorHandling):
    pass
