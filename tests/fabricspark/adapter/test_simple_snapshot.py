from dbt.tests.adapter.simple_snapshot.test_snapshot import (
    BaseSimpleSnapshot,
    BaseSimpleSnapshotBase,
    BaseSnapshotCheck,
)


class TestSimpleSnapshotFabricSpark(BaseSimpleSnapshot, BaseSimpleSnapshotBase):
    pass


class TestSnapshotCheckFabricSpark(BaseSnapshotCheck, BaseSimpleSnapshotBase):
    pass
