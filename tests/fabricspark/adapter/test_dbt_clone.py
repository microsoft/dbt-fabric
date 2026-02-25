from dbt.tests.adapter.dbt_clone.test_dbt_clone import (
    BaseCloneNotPossible,
    BaseClonePossible,
    BaseCloneSameSourceAndTarget,
    BaseCloneSameTargetAndState,
)


class TestFabricSparkCloneNotPossible(BaseCloneNotPossible):
    pass


class TestFabricSparkCloneSameTargetAndState(BaseCloneSameTargetAndState):
    pass


class TestFabricSparkClonePossible(BaseClonePossible):
    pass


class TestFabricSparkCloneSameSourceAndTarget(BaseCloneSameSourceAndTarget):
    pass
