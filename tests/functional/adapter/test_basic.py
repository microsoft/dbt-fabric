import pytest

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
    BaseIncrementalNotSchemaChange,
)
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection


class TestSimpleMaterializations(BaseSimpleMaterializations):
    pass


class TestSingularTestsFabric(BaseSingularTests):
    pass


class TestSingularTestsEphemeralFabric(BaseSingularTestsEphemeral):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "singular_tests_ephemeral",
            "models": {
                "+materialized": "table"  # no support for nested CTEs in Views yet
            },
        }


class TestEmptyFabric(BaseEmpty):
    pass


class TestEphemeralFabric(BaseEphemeral):
    pass


class TestIncrementalFabric(BaseIncremental):
    pass


class TestIncrementalNotSchemaChangeFabric(BaseIncrementalNotSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_not_schema_change.sql": """
{{ config(materialized="incremental",
unique_key="user_id_current_time",on_schema_change="sync_all_columns") }}
select
CAST(1 + '-' + current_timestamp AS DATETIME2(6)) as user_id_current_time,
{% if is_incremental() %}
'thisis18characters' as platform
{% else %}
'okthisis20characters' as platform
{% endif %}
"""
        }


class TestGenericTestsFabric(BaseGenericTests):
    pass


class TestSnapshotCheckColsFabric(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampFabric(BaseSnapshotTimestamp):
    pass


class TestBaseCachingFabric(BaseAdapterMethod):
    pass


class TestValidateConnectionFabric(BaseValidateConnection):
    pass


class TestDocsGenerateFabric(BaseDocsGenerate):
    pass


class TestDocsGenReferencesFabric(BaseDocsGenReferences):
    pass


class TestGetCatalogForSingleRelationFabric(BaseGetCatalogForSingleRelation):
    pass


class TestTableMaterializationFabric(BaseTableMaterialization):
    pass
