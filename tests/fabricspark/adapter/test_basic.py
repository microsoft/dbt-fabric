from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate, BaseDocsGenReferences
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_get_catalog_for_single_relation import (
    BaseGetCatalogForSingleRelation,
)
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalBadStrategy,
    BaseIncrementalNotSchemaChange,
)
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection


class TestSimpleMaterializationsSpark(BaseSimpleMaterializations):
    pass


class TestSingularTestsSpark(BaseSingularTests):
    pass


class TestSingularTestsEphemeralSpark(BaseSingularTestsEphemeral):
    pass


class TestEmptySpark(BaseEmpty):
    pass


class TestEphemeralSpark(BaseEphemeral):
    pass


class TestIncrementalSpark(BaseIncremental):
    pass


class TestIncrementalNotSchemaChangeFabric(BaseIncrementalNotSchemaChange):
    pass


class TestGenericTestsSpark(BaseGenericTests):
    pass


class TestSnapshotCheckColsSpark(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampSpark(BaseSnapshotTimestamp):
    pass


class TestBaseCachingSpark(BaseAdapterMethod):
    pass


class TestValidateConnectionSpark(BaseValidateConnection):
    pass


class TestDocsGenerateSpark(BaseDocsGenerate):
    pass


class TestDocsGenReferencesSpark(BaseDocsGenReferences):
    pass


class TestTableMaterializationSpark(BaseTableMaterialization):
    pass


class TestGetCatalogForSingleRelationSpark(BaseGetCatalogForSingleRelation):
    pass


class TestIncrementalBadStrategySpark(BaseIncrementalBadStrategy):
    pass
