from dbt.tests.adapter.incremental.test_incremental_merge_exclude_columns import (
    BaseMergeExcludeColumns,
)
from dbt.tests.adapter.incremental.test_incremental_microbatch import BaseMicrobatch
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
)
from dbt.tests.adapter.incremental.test_incremental_predicates import BaseIncrementalPredicates
from dbt.tests.adapter.incremental.test_incremental_unique_id import BaseIncrementalUniqueKey


class TestBaseIncrementalUniqueKeyFabricSpark(BaseIncrementalUniqueKey):
    pass


class TestIncrementalOnSchemaChangeFabricSpark(BaseIncrementalOnSchemaChange):
    pass


class TestIncrementalPredicatesDeleteInsertFabricSpark(BaseIncrementalPredicates):
    pass


class TestPredicatesDeleteInsertFabricSpark(BaseIncrementalPredicates):
    pass


class TestMergeExcludeColumnsFabricSpark(BaseMergeExcludeColumns):
    pass


class TestFabricSparkMicrobatch(BaseMicrobatch):
    pass
