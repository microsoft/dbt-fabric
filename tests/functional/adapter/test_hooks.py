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


class FabricHooksChecks:
    def check_hooks(self, state, project, host, count=1):
        ctxs = self.get_ctx_vars(state, count=count, project=project)
        for ctx in ctxs:
            assert ctx["test_state"] == state
            assert ctx["target_name"] == "default"
            assert ctx["target_schema"] == project.test_schema
            assert ctx["target_threads"] == 1
            assert ctx["target_type"] == "fabric"

            assert ctx["run_started_at"] is not None and len(ctx["run_started_at"]) > 0, (
                "run_started_at was not set"
            )
            assert ctx["invocation_id"] is not None and len(ctx["invocation_id"]) > 0, (
                "invocation_id was not set"
            )
            assert ctx["thread_id"].startswith("Thread-")


class TestDuplicateHooksInConfigsFabric(BaseDuplicateHooksInConfigs):
    pass


class TestHookRefsFabric(FabricHooksChecks, BaseHookRefs):
    pass


class TestHooksRefsOnSeedsFabric(BaseHooksRefsOnSeeds):
    pass


class TestPrePostModelHooksFabric(FabricHooksChecks, BasePrePostModelHooks):
    pass


class TestPrePostModelHooksInConfigFabric(FabricHooksChecks, BasePrePostModelHooksInConfig):
    pass


class TestPrePostModelHooksInConfigKwargsFabric(
    FabricHooksChecks, BasePrePostModelHooksInConfigKwargs
):
    pass


class TestPrePostModelHooksInConfigWithCountFabric(
    FabricHooksChecks, BasePrePostModelHooksInConfigWithCount
):
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


class TestPrePostModelHooksUnderscoresFabric(FabricHooksChecks, BaseTestPrePost):
    pass


class TestPrePostSnapshotHooksInConfigKwargsFabric(BasePrePostSnapshotHooksInConfigKwargs):
    pass


class TestAfterRunHooksFabric(BaseAfterRunHooks):
    pass


class TestPrePostRunHooksFabric(BasePrePostRunHooks):
    pass
