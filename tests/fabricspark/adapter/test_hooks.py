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
)
from dbt.tests.adapter.hooks.test_run_hooks import BaseAfterRunHooks, BasePrePostRunHooks


class TestDuplicateHooksInConfigsFabricSpark(BaseDuplicateHooksInConfigs):
    pass


class TestHookRefsFabricSpark(BaseHookRefs):
    pass


class TestHooksRefsOnSeedsFabricSpark(BaseHooksRefsOnSeeds):
    pass


class TestPrePostModelHooksInConfigFabricSpark(BasePrePostModelHooksInConfig):
    pass


class TestPrePostModelHooksInConfigKwargsFabricSpark(BasePrePostModelHooksInConfigKwargs):
    pass


class TestPrePostModelHooksOnSeedsFabricSpark(BasePrePostModelHooksOnSeeds):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixedFabricSpark(
    BasePrePostModelHooksOnSeedsPlusPrefixed
):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixedWhitespaceFabricSpark(
    BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace,
):
    pass


class TestPrePostModelHooksOnSnapshotsFabricSpark(BasePrePostModelHooksOnSnapshots):
    pass


class TestPrePostSnapshotHooksInConfigKwargsFabricSpark(BasePrePostSnapshotHooksInConfigKwargs):
    pass


class TestAfterRunHooksFabricSpark(BaseAfterRunHooks):
    pass


class TestPrePostModelHooksFabricSpark(BasePrePostModelHooks):
    pass


class TestPrePostModelHooksInConfigWithCountFabricSpark(BasePrePostModelHooksInConfigWithCount):
    pass


class TestPrePostRunHooksFabricSpark(BasePrePostRunHooks):
    pass
