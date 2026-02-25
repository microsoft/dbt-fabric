from dbt.tests.adapter.store_test_failures_tests.basic import (
    StoreTestFailuresAsExceptions,
    StoreTestFailuresAsGeneric,
    StoreTestFailuresAsInteractions,
    StoreTestFailuresAsProjectLevelEphemeral,
    StoreTestFailuresAsProjectLevelOff,
    StoreTestFailuresAsProjectLevelView,
)
from dbt.tests.adapter.store_test_failures_tests.test_store_test_failures import (
    BaseStoreTestFailures,
    BaseStoreTestFailuresLimit,
)


class TestFabricSparkStoreTestFailures(BaseStoreTestFailures):
    pass


class TestFabricSparkStoreTestFailuresAsGeneric(StoreTestFailuresAsGeneric):
    pass


class TestFabricSparkStoreTestFailuresAsExceptions(StoreTestFailuresAsExceptions):
    pass


class TestFabricSparkStoreTestFailuresAsInteractions(StoreTestFailuresAsInteractions):
    pass


class TestFabricSparkStoreTestFailuresAsProjectLevelEphemeral(
    StoreTestFailuresAsProjectLevelEphemeral,
):
    pass


class TestFabricSparkStoreTestFailuresAsProjectLevelOff(StoreTestFailuresAsProjectLevelOff):
    pass


class TestFabricSparkStoreTestFailuresAsProjectLevelView(StoreTestFailuresAsProjectLevelView):
    pass


class TestFabricSparkStoreTestFailuresLimit(BaseStoreTestFailuresLimit):
    pass
