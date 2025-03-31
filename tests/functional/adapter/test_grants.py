from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


class TestModelGrantsFabric(BaseModelGrants):
    pass


class TestSeedGrantsFabric(BaseSeedGrants):
    pass


class TestSnapshotGrantsFabric(BaseSnapshotGrants):
    pass


class TestIncrementalGrantsFabric(BaseIncrementalGrants):
    pass


class TestInvalidGrantsFabric(BaseInvalidGrants):
    pass
