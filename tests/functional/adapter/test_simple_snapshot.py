from dbt.tests.adapter.simple_snapshot.test_snapshot import (
    BaseSimpleSnapshot,
    BaseSimpleSnapshotBase,
    BaseSnapshotCheck,
)


class TestSimpleSnapshotBaseFabric(BaseSimpleSnapshotBase):
    pass


class TestSimpleSnapshotFabric(BaseSimpleSnapshot):
    pass


class TestSnapshotCheckFabric(BaseSnapshotCheck):
    pass
