import pytest

from dbt.tests.adapter.hooks.test_model_hooks import (
    BaseDuplicateHooksInConfigs,
    BaseHookRefs,
    BaseHooksRefsOnSeeds,
    BasePrePostModelHooksInConfig,
    BasePrePostModelHooksInConfigKwargs,
    BasePrePostModelHooksOnSeeds,
    BasePrePostModelHooksOnSeedsPlusPrefixed,
    BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace,
    BasePrePostModelHooksOnSnapshots,
    BasePrePostSnapshotHooksInConfigKwargs,
)
from dbt.tests.adapter.hooks.test_run_hooks import (
    BaseAfterRunHooks,
)
from dbt.tests.fixtures.project import TestProjInfo


class RunModelFile:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project: TestProjInfo):
        project.run_sql("drop table if exists {schema}.on_model_hook;")
        project.run_sql("""
create table {schema}.on_model_hook (
    test_state       varchar(100), -- start|end
    target_dbname    varchar(100),
    target_host      varchar(100),
    target_name      varchar(100),
    target_schema    varchar(100),
    target_type      varchar(100),
    target_user      varchar(100),
    target_pass      varchar(100),
    target_threads   int,
    run_started_at   varchar(100),
    invocation_id    varchar(100),
    thread_id        varchar(100)
);
""")


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


class TestHookRefsFabric(RunModelFile, FabricHooksChecks, BaseHookRefs):
    pass


class TestHooksRefsOnSeedsFabric(BaseHooksRefsOnSeeds):
    pass


class TestPrePostModelHooksInConfigFabric(
    RunModelFile, FabricHooksChecks, BasePrePostModelHooksInConfig
):
    pass


class TestPrePostModelHooksInConfigKwargsFabric(
    RunModelFile, FabricHooksChecks, BasePrePostModelHooksInConfigKwargs
):
    pass


class TestPrePostModelHooksOnSeedsFabric(BasePrePostModelHooksOnSeeds):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "post-hook": [
                    "alter table {{ this }} add new_col int",
                    "update {{ this }} set new_col = 1",
                    # call any macro to track dependency: https://github.com/dbt-labs/dbt-core/issues/6806
                    "select cast(null as {{ dbt.type_int() }}) as id",
                ],
                "quote_columns": False,
            },
        }


class TestPrePostModelHooksOnSeedsPlusPrefixedFabric(BasePrePostModelHooksOnSeedsPlusPrefixed):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "+post-hook": [
                    "alter table {{ this }} add new_col int",
                    "update {{ this }} set new_col = 1",
                ],
                "quote_columns": False,
            },
        }


class TestPrePostModelHooksOnSeedsPlusPrefixedWhitespaceFabric(
    BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace
):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "+post-hook": [
                    "alter table {{ this }} add new_col int",
                    "update {{ this }} set new_col = 1",
                ],
                "quote_columns": False,
            },
        }


class TestPrePostModelHooksOnSnapshotsFabric(BasePrePostModelHooksOnSnapshots):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "snapshot-paths": ["test-snapshots"],
            "models": {},
            "snapshots": {
                "post-hook": [
                    "alter table {{ this }} add new_col int",
                    "update {{ this }} set new_col = 1",
                ]
            },
            "seeds": {
                "quote_columns": False,
            },
        }


class TestPrePostSnapshotHooksInConfigKwargsFabric(BasePrePostSnapshotHooksInConfigKwargs):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "snapshot-paths": ["test-kwargs-snapshots"],
            "models": {},
            "snapshots": {
                "post-hook": [
                    "alter table {{ this }} add new_col int",
                    "update {{ this }} set new_col = 1",
                ]
            },
            "seeds": {
                "quote_columns": False,
            },
        }


class TestAfterRunHooksFabric(BaseAfterRunHooks):
    pass
