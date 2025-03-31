from dbt.tests.adapter.hooks.test_model_hooks import (
    BaseDuplicateHooksInConfigs,
    BaseHookRefs,
    BaseHooksRefsOnSeeds,
    BasePrePostModelHooks,
    BasePrePostModelHooksInConfig,
    BasePrePostModelHooksInConfigKwargs,
    BasePrePostModelHooksInConfigWithCount,
    BasePrePostModelHooksOnSeeds,
    BasePrePostModelHooksOnSeedsPlusPrefixed,
    BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace,
    BasePrePostModelHooksOnSnapshots,
    BasePrePostSnapshotHooksInConfigKwargs,
    BaseTestPrePost,
    PrePostModelHooksInConfigSetup,
)
from dbt.tests.adapter.hooks.test_run_hooks import (
    BaseAfterRunHooks,
    BasePrePostRunHooks,
)


class TestDuplicateHooksInConfigsFabric(BaseDuplicateHooksInConfigs):
    pass


class TestHookRefsFabric(BaseHookRefs):
    pass


class TestHooksRefsOnSeedsFabric(BaseHooksRefsOnSeeds):
    pass


class TestPrePostModelHooksFabric(BasePrePostModelHooks):
    pass


class TestPrePostModelHooksInConfigFabric(BasePrePostModelHooksInConfig):
    pass


class TestPrePostModelHooksInConfigKwargsFabric(BasePrePostModelHooksInConfigKwargs):
    pass


class TestPrePostModelHooksInConfigWithCountFabric(BasePrePostModelHooksInConfigWithCount):
    pass


class TestPrePostModelHooksOnSeedsFabric(BasePrePostModelHooksOnSeeds):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixedFabric(BasePrePostModelHooksOnSeedsPlusPrefixed):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixedWhitespaceFabric(
    BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace
):
    pass


class TestPrePostModelHooksOnSnapshotsFabric(BasePrePostModelHooksOnSnapshots):
    pass


class TestPrePostModelHooksUnderscoresFabric(BaseTestPrePost):
    pass


class TestPrePostSnapshotHooksInConfigKwargsFabric(BasePrePostSnapshotHooksInConfigKwargs):
    pass


class TestPrePostModelHooksInConfigSetupFabric(PrePostModelHooksInConfigSetup):
    pass


class TestAfterRunHooksFabric(BaseAfterRunHooks):
    pass


class TestPrePostRunHooksFabric(BasePrePostRunHooks):
    pass
